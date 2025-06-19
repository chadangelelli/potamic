(ns potamic.test-util
  (:require [potamic.db :as db :refer [wcar*]]
            [potamic.queue :as q]
            [potamic.queue.queues :as queues]
            [taoensso.carmine :as car :refer [wcar]]))

(def redis-db-uri "redis://default:secret@localhost:6379/0")
(def redis-conn (db/make-conn :uri redis-db-uri))
(def redis-test-queue :redis/test-queue)
(def redis-test-queue-group :redis/test-queue-group)

(def kvrocks-db-cnf {:host "127.0.0.1" :port 6666 :password "secret" :db 0})
(def kvrocks-conn (db/make-conn :backend :kvrocks
                                :host "127.0.0.1"
                                :port 6666
                                :password "secret"
                                :db 0))
(def kvrocks-test-queue :kvrocks/test-queue)
(def kvrocks-test-queue-group :kvrocks/test-queue-group)

(defn flushall-redis
  []
  (wcar* redis-conn (car/flushall)))

(defn flushall-kvrocks
  "Note: Kvrocks requires admin password (requirepass) to call FLUSHALL, as
  it uses the AUTH cmd to switch between namespaces and we need to be in the
  main namespace."
  []
  (let [admin-password (:password kvrocks-db-cnf)]
    (wcar kvrocks-conn
          (car/auth admin-password)
          (car/flushall))))

(defn flushall-kv-stores
  []
  (flushall-redis)
  (flushall-kvrocks))

(defn destroy-all-queues!
  []
  (doseq [qname (keys (q/get-queues))]
    (q/destroy-queue! qname redis-conn :unsafe true)
    (q/destroy-queue! qname kvrocks-conn :unsafe true))
  (let [queues (q/get-queues)]
    (assert (= 0 (count queues))
            (str "[Test Util Error] destroy-all-queues!: Count should be zero."
                 "\n\tFound: (" (keys queues) ")"))
    (assert (not (db/key-exists? redis-test-queue redis-conn))
            "[Test Util Error] destroy-all-queues!: redis-test-queue still exists in Redis DB")
    (assert (not (db/key-exists? kvrocks-test-queue kvrocks-conn))
            "[Test Util Error] destroy-all-queues!: kvrocks-test-queue still exists in Kvrocks DB")))

(defn fx-prime-flushall-kv-stores
  [f]
  (flushall-kv-stores)
  (f))

(defn fx-cleanup-flushall-kv-stores
  [f]
  (f)
  (flushall-kv-stores))

(defn fx-reset-queues
  [f]
  (f)
  (destroy-all-queues!)
  (flushall-kv-stores)
  (reset! queues/queues_ nil))

(defn fx-prime-queues
  [f]
  (destroy-all-queues!)
  (flushall-kv-stores)
  (let [[_ ?err] (q/create-queue! redis-test-queue
                                  redis-conn
                                  :group redis-test-queue-group)]
    (assert (nil? ?err)
            (str "[Test Util Error] fx-prime-queues: Can't create redis-test-queue"
                 "\n\t?err: " ?err)))
  (let [[_ ?err] (q/create-queue! kvrocks-test-queue
                                  kvrocks-conn
                                  :group kvrocks-test-queue-group)]
    (assert (nil? ?err)
            (str "[Test Util Error] fx-prime-queues: Can't create kvrocks-test-queue"
                 "\n\t?err: " ?err)))
  (let [queues (q/get-queues)
        n (count queues)]
    (assert (= 2 n)
            (str "[Test Util Error] fx-prime-queues: wrong count: " n " (should be 2)"
                 "\n\t(queues = " (keys queues) ")"))
    (assert (contains? queues redis-test-queue)
            "[Test Util Error] fx-prime-queues: Missing redis-test-queue")
    (assert (contains? queues kvrocks-test-queue)
            "[Test Util Error] fx-prime-queues: Missing kvrocks-test-queue"))
  (f))

(defn get-default-test-queue
  [backend]
  (case backend
    :redis redis-test-queue
    :kvrocks kvrocks-test-queue))

(defn get-default-test-queue-group
  [backend]
  (case backend
    :redis redis-test-queue-group
    :kvrocks kvrocks-test-queue-group))
