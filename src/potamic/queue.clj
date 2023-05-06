(ns potamic.queue
  "Implements a stream-based message queue over Redis."
  (:refer-clojure :exclude [read range])
  (:require [malli.core :as malli]
            [taoensso.carmine :as car :refer [wcar]]))

(def ^:private
  queues_
  "Contains queue specs."
  (atom {}))

(defn get-queue
  "Returns queue spec for `queue-name`."
  [queue-name]
  (get @queues_ queue-name))

(defn get-queues
  "Returns all queues if no `x` provided.
  Returns value at `x` if `x` is a keyword (same as `(get-queue :x)`).
  Returns map filter by `x` if `x` is a regex."
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

(defn make-redis-name
  [k]
  (let [n (namespace k)
        k (name k)]
    (if n
      (str n "/" k)
      k)))

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
                           (make-redis-name queue-name)
                           (make-redis-name group-name)
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

  (q/create-queue :my/queue conn)

  (q/create-queue :my/queue conn {:group :my-named/queue-group})
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
                   :redis-queue-name (make-redis-name queue-name)
                   :redis-group-name (make-redis-name group-name)})
           [(get-queue queue-name)
            nil])
       [nil ?err]))))

;;TODO: Implement (reverse of create-queue)
(defn delete-queue
  []
  )

(defn put
  "Put a message onto named queue. Returns vector of `[ok? ?err]`.

  **Examples:**

  ```clojure
  ;; the following lines are identical
  (put :my/queue {:a 1 :b 2 :c 3})
  (put :my/queue :* {:a 1 :b 2 :c 3})
  (put :my/queue \"*\" {:a 1 :b 2 :c 3})
  (put :my/queue '* {:a 1 :b 2 :c 3})

  ;;TODO: update this fake example
  (put :my/queue 123456789-0 {:a 1 :b 2 :c 3})
  ```

  See also:
  "
  ([queue-name kvs] (put queue-name "*" kvs))
  ([queue-name id kvs]
   (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)
         id* (or id "*")
         kvs* (reduce into [] kvs)]
     (try
       [true (when (= "OK"
                      (wcar conn (apply car/xadd qname id* kvs*)))
               nil)]
       (catch Throwable t
         [nil (Throwable->map t)])))))

;;TODO: validate input
;;TODO: validate `:from` as a valid `queue-name`
;;TODO: confirm `:as` to be arbitrary
(defn consume
  "Consumes message(s) from `queue-name`. Returns vector of `[?msgs ?err]`.

  _NOTE_: Consumers are responsible for declaring a message \"done\". That is,
  to call `(potamic.queue/set-message-state :done)`.

  **Examples:**

  ```clojure
  (consume 1 :from :my/queue :as :my/consumer1)
  ```

  See also:
  "
  [n-msgs & {:keys [from as]}]
  (let [{qname :redis-queue-name conn :queue-conn} (get-queue from)]
    [qname n-msgs conn as]
    ))


(defn create-consumer
  [{:keys [queue-name frequency]}]
  )
