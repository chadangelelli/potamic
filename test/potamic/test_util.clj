(ns potamic.test-util
  (:require [potamic.db :as db]
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
  (wcar redis-conn (car/flushall)))

(defn flushall-kvrocks
  []
  (wcar kvrocks-conn
        (car/auth (:password kvrocks-db-cnf))
        (car/flushall)))

(defn flushall-kv-stores
  []
  (flushall-redis)
  (flushall-kvrocks))

(defn fx-prime-flushall-kv-stores
  [f]
  (flushall-kv-stores)
  (f))

(defn fx-cleanup-flushall-kv-stores
  [f]
  (f)
  (flushall-kv-stores))

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
