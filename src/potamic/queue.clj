(ns potamic.queue
  "Implements a stream-based message queue over Redis."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:refer-clojure :exclude [read])
  (:require [taoensso.carmine :as car :refer [wcar]]
            [potamic.errors :as e]
            [potamic.util :as util]
            [potamic.validation :as v]
            [potamic.queue.validation :as qv]
            [potamic.queue.queues :as queues]))

(defn get-queue
  "Returns queue spec for `queue-name`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue)
  ;= [true nil]

  (q/get-queue :my/queue)
  ;= {:queue-name :my/queue
  ;=  :queue-conn
  ;=  {:spec {:uri \"redis://localhost:6379/0\"}
  ;=          :pool #taoensso.carmine.connections.ConnectionPool{..}}
  ;=  :group-name :my/queue-group
  ;=  :redis-queue-name \"my/queue\"
  ;=  :redis-group-name \"my/queue-group\"}
  ```

  See also:

  - `potamic.queue/get-queues`
  - `potamic.queue/create-queue`"
  [queue-name]
  (get @queues/queues_ queue-name))

(defn get-queues
  "Get all, or a subset, of queues created via `potamic.queue/create-queue`.
  Return value varies depending on `x` input type.

  | Type      | Result                                         |
  | --------- | ---------------------------------------------- |
  | `nil`     | all queues                                     |
  | `regex`   | map filtered by searching kv space for pattern |
  | `keyword` | same as calling `(get-queue x)`                |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/one conn)
  ;= [true nil]

  (q/create-queue :my/two conn)
  ;= [true nil]

  (q/create-queue :my/three conn)
  ;= [true nil]

  (q/get-queues)
  ;= #:my{:one {:queue-name :my/one
  ;=            :queue-conn
  ;=            {:spec {:uri \"redis://localhost:6379/0\"}
  ;=             :pool #taoensso.carmine.connections.ConnectionPool{..}}
  ;=            :group-name :my/one-group
  ;=            :redis-queue-name \"my/one\"
  ;=            :redis-group-name \"my/one-group\"}
  ;=      :two {:queue-name :my/two
  ;=            :queue-conn
  ;=            {:spec {:uri \"redis://localhost:6379/0\"}
  ;=             :pool #taoensso.carmine.connections.ConnectionPool{..}}
  ;=            :group-name :my/two-group
  ;=            :redis-queue-name \"my/two\"
  ;=            :redis-group-name \"my/two-group\"}
  ;=      :three {:queue-name :my/three
  ;=              :queue-conn
  ;=              {:spec {:uri \"redis://localhost:6379/0\"}
  ;=               :pool #taoensso.carmine.connections.ConnectionPool{.. }
  ;=              :group-name :my/three-group
  ;=              :redis-queue-name \"my/three\"
  ;=              :redis-group-name \"my/three-group\"}}

  (q/get-queues #\"three\")
  ;= #:my{:three {:queue-name :my/three
  ;=      :queue-conn
  ;=      {:spec {:uri \"redis://localhost:6379/0\"}
  ;=       :pool #taoensso.carmine.connections.ConnectionPool{}}
  ;=      :group-name :my/three-group
  ;=      :redis-queue-name \"my/three\"
  ;=      :redis-group-name \"my/three-group\"}}
  ```

  See also:

  - `potamic.queue/get-queue`
  - `potamic.queue/create-queue`
  - `potamic.queue/delete-queue`"
  ([] @queues/queues_)
  ([x]
   (cond
     (instance? java.util.regex.Pattern x)
     (into {} (filter (fn [[k v]]
                        (or (re-find x (name k))
                            (re-find x (str v))))
                      @queues/queues_))

     :else
     (get @queues/queues_ x))))

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
  "Creates a queue. Returns vector of `[ok? ?err]`.

  | Option     | Description       | Default            |
  | ---------- | ----------------- | ------------------ |
  | `:group`   | Sets reader group | `QUEUE-NAME-group` |
  | `:init-id` | Initial ID        | `0`                |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
  '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec {:uri \"redis://localhost:6379/0\"}
  ;=  :pool #taoensso.carmine.connections.ConnectionPool{..}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/create-queue :secondary/queue conn :group :secondary/group)
  ;= [true nil]

  (q/create-queue :third/queue conn :group :third/group :init-id 0)
  ;= [true nil]
  ```

  See also:
  "
  ;[queue-name conn & {:keys [group init-id] :or {init-id 0}}]
  [queue-name conn & opts]
  (let [opts* (apply hash-map opts)
        group-name (or (:group opts*) (-set-default-group-name queue-name))
        init-id (or (:init-id opts*) 0)
        args {:conn conn
              :queue-name queue-name
              :init-id init-id
              :group group-name}]
    (if-let [args-err (v/invalidate qv/Valid-Create-Queue-Args args)]
      [nil (e/error {:potamic/err-type :potamic/args-err
                     :potamic/err-fatal? false
                     :potamic/err-msg (str "Invalid args provided to "
                                           "potamic.queue/create-queue")
                     :potamic/err-data {:args (util/remove-conn args)
                                        :err args-err}})]
      (let [[stream-initialized? ?err] (-initialize-stream conn
                                                           queue-name
                                                           group-name
                                                           init-id)
            new-queue {:queue-name queue-name
                      :queue-conn conn
                      :group-name group-name
                      :redis-queue-name (util/->str queue-name)
                      :redis-group-name (util/->str group-name)}
            ]
        (if stream-initialized?
          (do (swap! queues/queues_ assoc queue-name new-queue)
              [true nil])
          [nil ?err])))))

;;TODO: add input validation for ID/MSG pairs and/or wildcar IDs for multi
(defn put
  "Put message(s) onto a queue. Returns vector of `[?msg-ids ?err]`.

  _NOTE_: Because `put` can add more than one message, on success `?msg-ids`
  will always be a vector of ID strings, or `nil` on error.

  _NOTE_: It is highly recommended to let Redis set the ID automatically.
  However, if setting the ID, anything that will resolve via `name` is
  acceptable. _As a reminder: numbers cannot be quoted._

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec {:uri \"redis://localhost:6379/0\"}
  ;=  :pool #taoensso.carmine.connections.ConnectionPool{..}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  ;; setting ID for a single message
  (q/put :my/queue \"1683743739-0\" {:a 1})
  ;= [[\"1683743739-0\"] nil]

  ;; setting the ID for a single message using wildcard.
  (q/put :my/queue \"1683743739-*\" {:a 1})
  (q/put :my/queue :1683743739-* {:a 1})
  ;= [[\"1683743739-1\"] nil]

  ;; setting IDs for multi mode. the trailing `*` is required.
  (q/put :my/queue \"1683743739-*\" {:a 1} {:b 2} {:c 3})
  ;= [[\"1683743739-2\" \"1683743739-3\" \"1683743739-4\"] nil]

  ;; let Redis set the ID (RECOMMENDED)
  ;; (all of the following are identical, in effect)
  (q/put :my/queue {:a 1 :b 2 :c 3})
  (q/put :my/queue :* {:a 1 :b 2 :c 3})
  (q/put :my/queue \"*\" {:a 1 :b 2 :c 3})
  (q/put :my/queue '* {:a 1 :b 2 :c 3})
  ;= [[\"1683660166747-0\"] nil]
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
         id-set? (or (string? x)
                     (keyword? x)
                     (symbol? x))
         id (if id-set? (name x) "*")
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
  [{:id ID :msg MSG} ..]
  ```

  | Option   | Default Value                |
  | -------- | ---------------------------- |
  | `:start` | `0` _(all messages)_         |
  | `:count` | `nil` _(no limit)_           |
  | `:block` | `nil` _(return immediately)_ |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q]
           '[clojure.core.async :as async])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec {:uri \"redis://localhost:6379/0\"}
  ;=  :pool #taoensso.carmine.connections.ConnectionPool{..}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683912716308-0\" \"1683912716308-1\" \"1683912716308-2\"]
  ;=  nil]

  (q/read :my/queue)
  ;= [({:id \"1683913507471-0\", :msg {:a \"1\"}}
  ;=   {:id \"1683913507471-1\", :msg {:b \"2\"}}
  ;=   {:id \"1683913507471-2\", :msg {:c \"3\"}})
  ;=  nil]

  (q/read :my/queue :start 0)
  ;= [({:id \"1683913507471-0\", :msg {:a \"1\"}}
  ;=   {:id \"1683913507471-1\", :msg {:b \"2\"}}
  ;=   {:id \"1683913507471-2\", :msg {:c \"3\"}})
  ;=  nil]

  (async/go (async/<! (async/timeout 2000)) (q/put :my/queue {:d 4}))
  ;= #object[clojure.core.async.impl.channels.ManyToManyChannel ..]

  ;; block until above Go call executes
  (q/read :my/queue :count 10 :start 0 :block [5 :seconds])
  ;= [({:id \"1683915375766-0\", :msg {:a \"1\"}}
  ;=   {:id \"1683915375766-1\", :msg {:b \"2\"}}
  ;=   {:id \"1683915375766-2\", :msg {:c \"3\"}}
  ;=   {:id \"1683915435992-0\", :msg {:d \"4\"}})
  ;=  nil]
  ```

  See also:

  - `potamic.queue/read-next!`
  - `potamic.queue/put`"
  [queue-name & {:keys [start block] cnt :count :or {start 0}}]
  (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd
                  [(when cnt [:count cnt])
                   (when block [:block (util/time->milliseconds block)])
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
  [{:id ID :msg MSG} ..]
  ```

  | Option   | Default Value      |
  | -------- | ------------------ |
  | `:start` | `-` _(oldest)_     |
  | `:end`   | `+` _(newest)_     |
  | `:count` | `nil` _(no limit)_ |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
  '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683946534423-0\" \"1683946534423-1\" \"1683946534423-2\"]
  ;=  nil]

  (q/read-range :my/queue :start '- :end '+)
  ;= [({:id \"1683946534423-0\", :msg {:a \"1\"}}
  ;=   {:id \"1683946534423-1\", :msg {:b \"2\"}}
  ;=   {:id \"1683946534423-2\", :msg {:c \"3\"}})
  ;=  nil]

  (q/read-range :my/queue :start '- :end '+ :count 10)
  ;= [({:id \"1683946534423-0\", :msg {:a \"1\"}}
  ;=   {:id \"1683946534423-1\", :msg {:b \"2\"}}
  ;=   {:id \"1683946534423-2\", :msg {:c \"3\"}})
  ;=  nil]
  ```

  See also:

  - `potamic.queue/read`
  - `potamic.queue/read-next!`
  - `potamic.queue/put`"
  [queue-name & opts]
  (let [opts* (apply hash-map opts)
        start (or (:start opts*) "-")
        end (or (:end opts*) "+")
        cnt (:count opts*)
        args (assoc opts*
                    :queue-name queue-name
                    :start start
                    :end end
                    :count cnt)]
    (if-let [args-err (v/invalidate qv/Valid-Read-Range-Args args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg (str "Invalid args provided to "
                                       "potamic.queue/read-range")
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
        (try
          (let [cmd (util/prep-cmd [[qname start end]
                                    (when cnt [:count cnt])])
                res (-> (wcar conn (apply car/xrange cmd))
                        -make-read-result)]
            [res nil])
          (catch Throwable t
            [nil (Throwable->map t)]))))))

(defn read-next!
  "Reads next message(s) from a queue as consumer for queue group,
  side-effecting Redis' Pending Entries List. Returns vector of `[?msgs ?err]`.

  `?msgs` is of the form:

  ```clojure
  [{:id ID :msg MSG} ..]
  ```

  _NOTE_: `Readers` are responsible for declaring messages \"processed\"
  by calling `potamic.queue/set-processed!`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
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

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
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

(defn- -make-pending-result
  [r]
  (mapv (fn [[id c ms n]]
          {:id id
           :consumer c
           :milliseconds-since-delivered ms
           :delivered-n-times n}
          )
        r))

(defn read-pending
  "Lists details of pending messages for a `queue`/`group` pair. Optionally,
  a `consumer` may be provided for sub-filtering.
  Returns vector of `[?details ?err]`.

  `?details` is of the form:

  ```clojure
  ({:id ID
    :consumer NAME
    :milliseconds-since-delivered MILLISECONDS
    :delivered-n-times N}
   ..)
  ```

  | Option   | Description   | Default                 |
  | -------- | ------------- | ----------------------- |
  | `:from`  | Queue name    | `none, required`        |
  | `:for`   | Consumer name | `nil, get entire group` |
  | `:start` | Start ID      | `\"-\"` (beginning)     |
  | `:end`   | End ID        | `\"+\"` (end)           |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683944086236-0\" \"1683944086236-1\" \"1683944086236-2\"]
  ;=  nil]

  (q/read-next! 1 :from :my/queue :as :consumer/one)
  ;= [({:id \"1683944086236-0\", :msg {:a \"1\"}})
  ;=  nil]

  (q/read-pending 10 :from :my/queue :for :consumer/one)
  ;= [({:id \"1683944086236-0\"
  ;=    :consumer \"consumer/one\"
  ;=    :milliseconds-since-delivered 9547
  ;=    :delivered-n-times 1})
  ;=  nil]

  (q/read-pending 10 :from :my/queue :for :consumer/one :start '- :end '+)
  ;= [({:id \"1683944086236-0\"
  ;=    :consumer \"consumer/one\"
  ;=    :milliseconds-since-delivered 16768
  ;=    :delivered-n-times 1})
  ;= nil]

  (q/read-pending 1
                  :from :my/queue
                  :for :consumer/one
                  :start \"1683944086236-0\"
                  :end \"1683944086236-2\")
  ;= [({:id \"1683944086236-0\"
  ;=    :consumer \"consumer/one\"
  ;=    :milliseconds-since-delivered 144556
  ;=    :delivered-n-times 1})
  ;=  nil]
  ```

  See also:

  - `potamic.queue/read-pending-summary`
  - `potamic.queue/set-processed!`"
  [count* & opts]
  (let [opts* (apply hash-map opts)
        from (:from opts*)
        start (or (:start opts*) "-")
        end (or (:end opts*) "+")
        consumer (:for opts*)
        args (assoc opts*
                    :from from
                    :for consumer
                    :start start
                    :end end
                    :count count*)]
    (if-let [args-err (v/invalidate qv/Valid-Read-Pending-Args args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg (str "Invalid args provided to "
                                       "potamic.queue/read-pending")
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name
             group :redis-group-name
             conn :queue-conn} (get-queue from)]
        (try
          (let [cmd (util/prep-cmd [[qname group start end count*]
                                    (when consumer consumer)])
                res (-> (wcar conn (apply car/xpending cmd))
                        -make-pending-result)]
            [(lazy-seq res) nil])
          (catch Throwable t
            [nil
             (e/error {:potamic/err-type :potamic/args-err
                       :potamic/err-msg (.getMessage t)
                       :potamic/err-data {:args args
                                          :err (Throwable->map t)}})]))))))

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

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
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

(defn destroy-queue!
  "Destroys a queue (stream) and the consumer group associated with it.
  Returns vector of `[ok? ?err]`.

  _WARNING_: Do not call this command unless you know it's safe to do so!

  | Option     | Description                      | Default |
  | ---------- | -------------------------------- | ------- |
  | `:unsafe`  | Skip Pending Entries List checks | `false` |

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec {:uri \"redis://localhost:6379/0\"}
  ;=  :pool #taoensso.carmine.connections.ConnectionPool{..}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683933694431-0\" \"1683933694431-1\" \"1683933694431-2\"] nil]

  (q/read-next! 2 :from :my/queue :as :consumer/one)
  ;= [({:id \"1683933694431-0\", :msg {:a \"1\"}} {:id \"1683933694431-1\", :msg {:b \"2\"}}) nil]

  ;; only destroy if no pending messages (in this case will fail)
  (q/destroy-queue! :my/queue conn)
  ;= [nil
  ;=  #:potamic{:err-type :potamic/db-err
  ;=            :err-msg \"Cannot destroy my/queue, it has pending messages\"
  ;=            :err-data
  ;=            {:args {:queue-name :my/queue :unsafe false}
  ;=             :groups [{:consumers 1
  ;=                       :entries-read 2
  ;=                       :last-delivered-id \"1683933694431-1\"
  ;=                       :name \"my/queue-group\"
  ;=                       :pending 2
  ;=                       :lag 1}]
  ;=             :consumers [{:idle 3371 :name \"consumer/one\" :pending 2}]}
  ;=            :err-file \"[..]/potamic/src/potamic/queue.clj\"
  ;=            :err-line 644
  ;=            :err-column 12}]

  ;; force-destroy, ignoring if there are pending messages
  (q/destroy-queue! :my/queue conn :unsafe true)
  ;= [true nil]
  ```

  See also:

  - `potamic.queue/create-queue`"
  [queue-name conn & opts]
  (let [opts* (apply hash-map opts)
        unsafe? (boolean (:unsafe opts*))
        args {:conn conn
              :queue-name queue-name
              :unsafe unsafe?}]
    (if-let [args-err (v/invalidate qv/Valid-Destroy-Queue-Args args)]
      [nil (e/error {:potamic/err-type :potamic/args-err
                     :potamic/err-fatal? false
                     :potamic/err-msg (str "Invalid args provided to "
                                           "potamic.queue/destroy-queue")
                     :potamic/err-data {:args (util/remove-conn args)
                                        :err args-err}})]
      (let [{qname :redis-queue-name
             group :redis-group-name
             conn :queue-conn} (get-queue queue-name)
            [rgroups rconsumers] (wcar conn
                                       :as-pipeline
                                       (car/xinfo-groups qname)
                                       (car/xinfo-consumers qname group))
            groups (mapv #(into {} (for [[k v] (apply hash-map %)]
                                     [(keyword k) v]))
                         rgroups)
            consumers (mapv #(into {} (for [[k v] (apply hash-map %)]
                                        [(keyword k) v]))
                            rconsumers)
            has-pending? (pos-int? (apply max (map :pending groups)))]
        (if (and (not unsafe?) has-pending?)
          [nil
           (e/error {:potamic/err-type :potamic/db-err
                     :potamic/err-fatal? false
                     :potamic/err-msg (str "Cannot destroy " qname
                                           ", it has pending messages")
                     :potamic/err-data {:args (util/remove-conn args)
                                        :groups groups
                                        :consumers consumers}})]
          (try
            (wcar conn
                  :as-pipeline
                  (-> (mapv #(car/xgroup-delconsumer qname group %) consumers)
                      (into (map #(car/xgroup-destroy qname %) groups))
                      (into (car/del qname))))
            [true nil]
            (catch Throwable t
              [nil
               (e/error {:potamic/err-type :potamic/db-err
                         :potamic/err-fatal? false
                         :potamic/err-msg (str "Cannot destroy " qname
                                               ", it has pending messages")
                         :potamic/err-data {:args (util/remove-conn args)
                                            :err (Throwable->map t)}})])))))))

