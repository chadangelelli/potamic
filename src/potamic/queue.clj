(ns potamic.queue
  "Implements a stream-based message queue over Redis."
  (:refer-clojure :exclude [read])
  (:require [malli.core :as malli]
            [taoensso.carmine :as car :refer [wcar]]
            [potamic.util :as util]))

(def ^:private
  queues_
  "Contains queue specs."
  (atom nil))

(defn get-queue
  "Returns queue spec for `queue-name`."
  [queue-name]
  (get @queues_ queue-name))

(defn get-queues
  "Get all, or a subset, of queues created via `potamic.queue/create-queue`.
  Return value varies depending on `x` input type.

  | Type      | Result                                         |
  | --------- | ---------------------------------------------- |
  | `nil`     | all queues                                     |
  | `regex`   | map filtered by searching kv space for pattern |
  | `vector`  | calls `get-in` for `x`                         |
  | `keyword` | same as calling `(get-queue x)`                |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :queue/one)
  ;= [true nil]

  (q/create-queue :queue/two)
  ;= [true nil]

  (q/create-queue :another/three)
  ;= [true nil]

  ;;TODO: add search examples


  ```

  See also:

  - `potamic.queue/get-queue`
  - `potamic.queue/create-queue`
  - `potamic.queue/delete-queue`"
  ([] @queues_)
  ([x]
   (cond
     (instance? java.util.regex.Pattern x)
     (into {} (filter (fn [[k v]]
                        (or (re-find x (name k))
                            (re-find x (str v))))
                      @queues_))

     (vector? x)
     (get-in @queues_ x)

     :else
     (get @queues_ x))))

;;TODO: add validation
(def Valid-Create-Queue-Opts
  (malli/schema
    any?
    ))

(defn- -set-default-group-name
  [queue-name]
  (keyword (str (subs (str queue-name) 1) "-group")))

(defn- -initialize-stream
  [conn queue-name group-name init-id]
  (try
    [true (when (= "OK"
                   (wcar conn
                         (car/xgroup-create
                           (util/->str queue-name)
                           (util/->str group-name)
                           init-id
                           :mkstream)))
            nil)]
    (catch Throwable t
      [nil (Throwable->map t)])))

;;TODO: add validation, errors
;;TODO: enforce that queue-name is a keyword (allow namespaces)
;;TODO: enforce that group-name is a keyword (allow namespaces)
(defn create-queue
  "Creates a queue. If no options are provided, `:group` defaults to
  `QUEUE-NAME-group`. Returns vector of `[ok? ?err]`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/create-queue :my/queue conn {:group :my-named/queue-group})
  ;= [true nil]
  ```

  See also:
  "
  ([queue-name conn] (create-queue queue-name conn {}))
  ([queue-name conn {:keys [group init-id] :or {init-id 0}}]
   (let [group-name (or group (-set-default-group-name queue-name))
         [stream-initialized? ?err] (-initialize-stream conn
                                                        queue-name
                                                        group-name
                                                        init-id)]
     (if stream-initialized?
       (do (swap! queues_
                  assoc
                  queue-name
                  {:queue-name queue-name
                   :queue-conn conn
                   :group-name group-name
                   :redis-queue-name (util/->str queue-name)
                   :redis-group-name (util/->str group-name)})
           [true nil])
       [nil ?err]))))

;;TODO: add input validation for ID/MSG pairs and/or wildcar IDs for multi
(defn put
  "Put message(s) onto a queue. Returns vector of `[?msg-ids ?err]`.

  _NOTE_: Because `put` can add more than one message, on success `?msg-ids`
  will always be a vector of ID strings, or `nil` on error.

  _NOTE_: It is highly recommended to let Redis set the ID automatically!

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= TODO: add output

  (q/create-queue :my/queue conn)
  ;= TODO: add output

  (q/put :my/queue {:a 1 :b 2 :c 3})
  (q/put :my/queue :* {:a 1 :b 2 :c 3})
  (q/put :my/queue \"*\" {:a 1 :b 2 :c 3})
  (q/put :my/queue '* {:a 1 :b 2 :c 3})
  ;= [[\"1683660166747-0\"] nil]

  ;; setting ID for a single message
  (q/put :my/queue 1683743739-0 {:a 1})

  ;; setting the ID for a single message using wildcard.
  (q/put :my/queue 1683743739-* {:a 1})

  ;; setting IDs for multi mode. the trailing `*` is required.
  (q/put :my/queue 1683743739-* {:a 1} {:b 2} {:c 3})
  ```

  See also:

  - `potamic.queue/read`
  - `potamic.queue/read-range`
  - `potamic.queue/read-next!`
  - `potamic.queue/read-pending-summary`
  - `potamic.queue/read-pending`
  - `potamic.queue/create-queue`"
  ([queue-name & xs]
   (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)
         x (first xs)
         id-set? (or (string? x) (symbol? x))
         id (if id-set? x "*")
         msgs (if id-set? (rest xs) xs)]
     (try
       [(wcar conn
              :as-pipeline
              (mapv #(apply car/xadd qname id (reduce into [] %)) msgs))
        nil]
       (catch Throwable t
         [nil (Throwable->map t)])))))

;;TODO: optimize key-fn algorithm
(defn- -make-read-result
  "Return lazy sesquence of messages for `read*` functions.

  **Examples:**

  ```clojure
  ```

  See also:
  "
  [r]
  (map (fn [[id msg-kvs]]
         {:id id
          :msg (->> (apply hash-map msg-kvs)
                    (map (fn [[k v]] [(util/<-str k) v]))
                    (into {}))})
       r))

(defn read
  "Reads messages from a queue. Returns vector of `[?msgs ?err]`. This
  function wraps Redis' `XREAD`. It does not involve groups, nor does it
  track pending entries. Also, it limits read to a single queue (stream).

  `?msgs` is of the form:

  ```clojure
  [{:id ID :msg MSG} {:id ID :msg MSG} ..]
  ```

  All options have defaults:

  | Option   | Default Value                |
  | -------- | ---------------------------- |
  | `:start` | `0` _(all messages)_         |
  | `:count` | `nil` _(no limit)_           |
  | `:block` | `nil` _(return immediately)_ |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))

  ;; read all using defaults
  (q/read :my/queue)
  ;= TODO: add output

  ;; start at a specific time
  (q/read :my/queue :start 1683661383518-0 :block [2 :seconds])
  ;= TODO: add output

  ;; start at a specific time, returning at most 10
  (q/read :my/queue :start 1683661383518-0 :count 10)
  ;= TODO: add output
  ```

  See also:

  - `potamic.queue/read-next!`
  - `potamic.queue/put`"
  [queue-name & {:keys [start block] cnt :count :or {start 0}}]
  (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd
                  [(when cnt [:count cnt])
                   (when block [:block block])
                   [:streams qname start]])
            res (-> (wcar conn (apply car/xread cmd))
                    first
                    second
                    -make-read-result)]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

(defn read-range
  "Reads a range of messages from a queue. Returns vector of `[?msgs ?err]`.
  This function wraps Redis' `XRANGE`. It does not involve groups, nor does it
  track pending entries.

  `?msgs` is of the form:

  ```clojure
  [{:id ID :msg MSG} {:id ID :msg MSG} ..]
  ```

  All options have defaults:

  | Option   | Default Value      |
  | -------- | ------------------ |
  | `:start` | `-` _(oldest)_     |
  | `:end`   | `+` _(newest)_     |
  | `:count` | `nil` _(no limit)_ |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))

  ;; read all using defaults (same as `(q/read :my/queue)`).
  (q/read-range :my/queue)
  ;= TODO: add output

  ;; start at a specific time
  (q/read-range :my/queue :start 1683661383518-0)
  ;= TODO: add output

  ;; start at a specific time, returning at most 10
  (q/read-range :my/queue :start 1683661383518-0 :end :+ :count 10)
  ;= TODO: add output
  ```

  See also:

  - `potamic.queue/read-next!`
  - `potamic.queue/put`"
  [queue-name & {:keys [start end] cnt :count :or {start "-" end "+"}}]
  (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd [[qname start end]
                                     (when cnt [:count cnt])])
            res (-> (wcar conn (apply car/xrange cmd))
                    -make-read-result)]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

;;TODO: validate input
;;TODO: validate `:from` as a valid `queue-name`
;;TODO: confirm `:as` to be arbitrary
(defn read-next!
  "Reads next message(s) from a queue as consumer for queue group,
  side-effecting Redis' Pending Entries List. Returns vector of `[?msgs ?err]`.

  `?msgs` is of the form:

  ```clojure
  [{:id ID :msg MSG} {:id ID :msg MSG} ..]
  ```

  _NOTE_: `Readers` are responsible for declaring messages \"processed\"
  by calling `potamic.queue/set-processed!`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683661383518-0\" \"1683661383518-1\" \"1683661383518-2\"] nil]

  (read-next! 1 :from :my/queue :as :my/consumer1)

  (read-next! :all :from :my/queue :as :my/consumer1 :block 2000)
  (read-next! :all :from :my/queue :as :my/consumer1 :block [2 :seconds])
  ;=
  ```

  See also:

  - `potamic.queue/read`
  - `potamic.queue/read-pending`
  - `potamic.queue/read-pending-summary`
  - `potamic.queue/put`"
  [consume & {:keys [from as block]}]
  (let [{qname :redis-queue-name
         group :redis-group-name
         conn :queue-conn} (get-queue from)]
    (try
      (let [cmd (util/prep-cmd
                  [[:group group as]
                   (when block [:block block])
                   (when (not= consume :all) [:count consume])
                   [:streams qname ">"]])
            res (-> (wcar conn (apply car/xreadgroup cmd))
                    first
                    second
                    -make-read-result)]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

(defn- -make-pending-summary
  "Returns `summary` map for `potamic.queue/read-pending-summary`.
  See there for details."
  [raw-response]
  (let [[total start end consumers] raw-response]
    {:total total
     :start start
     :end end
     :consumers (into {} (for [[k v] consumers]
                           [(util/<-str k)
                            (util/->int v)]))}))

(defn read-pending-summary
  "Lists all pending messages, for all consumers, for `queue`.
  Returns vector of `[?summary ?err]`.

  `?summary` is of the form:

  ```clojure
  {:total N
   :start ID
   :end ID
   :consumers {CONSUMER-NAME N-PENDING}}
  ```

  _NOTE_: `CONSUMER-NAME` is coerced by the rules of `util/<-str`.
  The string `\"my/consumer1\"` becomes the keyword `:my/consumer`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683745855445-0\" \"1683745855445-1\" \"1683745855445-2\"]
  ;=  nil]

  (q/read-next! 2 :from :my/queue :as :consumer/one)
  ;= [({:id \"1683745855445-0\" :msg {:a \"1\"}}
  ;=   {:id \"1683745855445-1\" :msg {:b \"2\"}})
  ;=  nil]

  (q/read-next! 1 :from :my/queue :as :consumer/two)
  ;= [({:id \"1683745855445-2\" :msg {:c \"3\"}})
  ;=  nil]

  (q/read-pending-summary :my/queue)
  ;= [{:total 3
  ;=   :start \"1683745855445-0\"
  ;=   :end \"1683745855445-2\"
  ;=   :consumers #:consumer{:one 2 :two 1}}
  ;=  nil]
  ```

  See also:

  - `potamic.queue/read-pending`
  - `potamic.queue/set-processed!`"
  [queue-name]
  (let [{qname :redis-queue-name
         group :redis-group-name
         conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd [[qname group]])
            res (-> (wcar conn (apply car/xpending cmd))
                    -make-pending-summary)]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

(defn read-pending
  "Lists details of pending messages for a `queue`.
  Returns vector of `[?details ?err]`.

  `?details` is of the form:

  ```clojure
  ```

  See also:

  - `potamic.queue/read-pending-summary`
  - `potamic.queue/set-processed!`"
  [queue-name & {:keys [start end] cnt :count :or {start "-" end "+"}}]
  )

;;TODO: add validation
(defn set-processed!
  "Removes message(s) from Redis' Pending Entries List. This command wraps
  Redis' `XACK` command. Returns vector of `[?n-acked ?err]`.

  _NOTE_: (as per official Redis docs)

  > \"Certain message IDs may no longer be part of the PEL
  > (for example because they have already been acknowledged),
  > and XACK will not count them as successfully acknowledged.\"

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683745855445-0\" \"1683745855445-1\" \"1683745855445-2\"]
  ;=  nil]

  (q/read-next! 1 :from :my/queue :as :consumer/one)
  ;= [({:id \"1683745855445-0\" :msg {:a \"1\"}})
  ;=  nil]

  (q/set-processed! :my/queue \"1683745855445-0\")

  (q/set-processed! :my/queue '1683745855445-1  '1683745855445-2)

  ```

  See also:

  - `potamic.queue/read-next!`
  - `potamic.queue/read-pending-summary`
  - `potamic.queue/read-pending`"
  [queue-name & msg-ids]
  (let [{qname :redis-queue-name
         group :redis-group-name
         conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd [(into [qname group] msg-ids)])
            res (wcar conn (apply car/xack cmd))]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

;;TODO: Implement (reverse of create-queue)
(defn delete-queue
  []
  )

