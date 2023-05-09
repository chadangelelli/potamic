(ns potamic.queue
  "Implements a stream-based message queue over Redis."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as string]
            [malli.core :as malli]
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
  (-> queue-name str (subs 1) (str "-group") keyword))

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
  ([queue-name conn {:keys [group id]}]
   (let [group-name (or group (-set-default-group-name queue-name))
         init-id (or id 0)
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

(defn put
  "Put a message onto a queue. Returns vector of `[?msg-ids ?err]`.

  _NOTE_: Because `put` can add more than one message, on success `?msg-ids`
  will always be a vector of ID strings (or `nil` on fail).

  **Examples:**

  ```clojure
  ;; the following lines are identical
  (put :my/queue {:a 1 :b 2 :c 3})
  (put :my/queue :* {:a 1 :b 2 :c 3})
  (put :my/queue \"*\" {:a 1 :b 2 :c 3})
  (put :my/queue '* {:a 1 :b 2 :c 3})
  ;= [[\"1683660166747-0\"] nil]

  ;;TODO: add example for multi-put

  ;;TODO: add example of manually setting ID
  ```

  See also:
  - `potamic.queue/read`
  - `potamic.queue/create-queue`
  - `potamic.queue/create-reader`"
  ([queue-name id-or-msg1 & msgs]
   (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)
         x id-or-msg1
         id-set? (or (string? x) (symbol? x))
         id (if id-set? (util/->str x) "*")
         msgs* (into [] (if id-set? msgs (conj msgs x)))]
     (try
       [(wcar conn
              :as-pipeline
              (mapv #(apply car/xadd qname id (reduce into [] %)) msgs*))
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

;;TODO: validate input
;;TODO: validate `:from` as a valid `queue-name`
;;TODO: confirm `:as` to be arbitrary
(defn read-next
  "Reads next message(s) from a queue as consumer for queue group.
  Returns vector of `[?msgs ?err]`.

  `?msgs` is of the form:

  ```clojure
  [{:id ID :msg MSG} {:id ID :msg MSG} ..]
  ```

  _NOTE_: `Readers` are responsible for declaring messages \"processed\".
  That is, to call `(potamic.queue/set-message-state :processed)`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/put :my/queue {:a 1} {:b 2} {:c 3})
  ;= [[\"1683661383518-0\" \"1683661383518-1\" \"1683661383518-2\"] nil]

  (read-next 1 :from :my/queue :as :my/consumer1)

  (read-next :all :from :my/queue :as :my/consumer1 :block 2000)
  (read-next :all :from :my/queue :as :my/consumer1 :block [2 :seconds])
  ;=
  ```

  See also:

  - `potamic.queue/read`
  - `potamic.queue/create-reader`
  - `potamic.queue/put`"
  [consume & {:keys [from as block]}]
  (let [{qname :redis-queue-name
         group :redis-group-name
         conn :queue-conn} (get-queue from)]
    (try
      (let [args [[:group group (util/->str as)]
                  (when block [:block block])
                  (when (not= consume :all) [:count consume])
                  [:streams qname ">"]]
            cmd (reduce (fn [o x] (if x (into o x) o)) [] args)
            res (-> (wcar conn (apply car/xreadgroup cmd))
                    first
                    second
                    -make-read-result)]
        [res nil])
      (catch Throwable t
        [nil (Throwable->map t)]))))

(defn create-reader
  "Creates a `Reader` that reads n-number of messages at an interval.
  Returns vector of `[?rdr ?err]` where `?rdr` is a running `Reader`
  instance (see `potamic.queue/get-reader`).

  _NOTE_: `Readers` are responsible for declaring messages \"processed\".
  That is, to call `(potamic.queue/set-message-state :processed)`.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (def reader (q/create-reader conn :my/queue {:consume 1 :every 2000}))
  ;= TODO: add output

  (def reader (q/create-reader conn
                               :my/queue
                               {:consume 1 :every [2 :seconds]}))
  ;= TODO: add output
  ```

  See also:

  - `potamic.queue/read`
  - `potamic.queue/get-reader`
  - `potamic.queue/put`
  - `potamic.queue/create-queue`"
  [{:keys [queue-name consume every]}]
  )

(defn get-reader
  "Returns running `Reader` instance.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn {:uri \"redis://localhost:6379/0\"}))
  ;= {:uri \"redis://localhost:6379/0\", :pool {}}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (def rdr (q/create-reader {:queue :my/queue
                             :consume 1
                             :every 2000
                             :as :my/consumer1}))

  (def rdr (q/create-reader {:queue :my/queue
                             :consume 1
                             :every [2 :seconds]
                             :as :my/consumer1}))

  (get-reader rdr)
  ;= TODO: add output
  ```

  See also:

  - `potamic.queue/create-reader`
  - `potamic.queue/create-queue`
  - `potamic.queue/put`"
  []
  )

;;TODO: Implement (reverse of create-queue)
(defn delete-queue
  []
  )

