(ns potamic.db-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [potamic.db :as db]
            [potamic.test-util :as tu :refer [redis-conn kvrocks-conn]]
            [taoensso.carmine :as car]))

(t/use-fixtures :each
                tu/fx-prime-flushall-kv-stores
                tu/fx-reset-queues
                tu/fx-prime-queues
                tu/fx-cleanup-flushall-kv-stores)

(def valid-redis-uris
  ["redis://localhost:6379/0"
   "redis://USER:PASS@localhost:1234"
   "redis://USER:PASS@localhost:12345"
   "redis://USER:PASS@localhost:123456"
   "redis://USER:PASS@localhost:123456/1"
   "redis://:PASS@localhost:6379"
   "redis://:PASS@localhost:6379/0"
   "redis://111.222.333.444:6379/0"
   "redis://USER:PASS@111.222.333.444:1234"
   "redis://USER:PASS@111.222.333.444:12345"
   "redis://USER:PASS@111.222.333.444:123456"
   "redis://USER:PASS@111.222.333.444:123456/1"
   "redis://:PASS@111.222.333.444:6379"
   "redis://:PASS@111.222.333.444:6379/0"])

(def valid-kvrocks-specs
  [{:host "localhost" :port 6666 :db 0 :password "secret"}
   {:host "127.0.0.1" :port 6666 :db 5 :password "secret"}])

(deftest make-conn
  (testing "potamic.db/make-conn"
    (doseq [uri valid-redis-uris
            :let [?conn (db/make-conn :uri uri)]]
      (is (= {:backend :redis :spec {:uri uri} :pool {}}
             (assoc ?conn :pool {}))))
    (doseq [{:keys [host port password db] :as spec} valid-kvrocks-specs
            :let [?conn (db/make-conn :backend :kvrocks
                                      :host host
                                      :port port
                                      :password password
                                      :db db)]]
      (is (= {:backend :kvrocks :spec spec :pool {}}
             (assoc ?conn :pool {}))))))

(deftest wcar*
  (testing "potamic.db/wcar*"
    (letfn [(-ping [conn] (db/wcar* conn (car/ping)))
            (-ping-pipeline [conn] (db/wcar* conn :as-pipeline (car/ping)))
            (-set-x [conn]
              [(db/wcar* conn (car/set :x 1))
               (db/wcar* conn (car/get :x))])
            (-hmset [conn]
              [(db/wcar* conn (car/hmset :y :a 1 :b 2 :c 3))
               (db/wcar* conn (car/hmget :y :a :b :c))])
            (-set-z-pipeline [conn]
              [(db/wcar* conn (car/set :z 1))
               (db/wcar* conn (car/get :z))])]
      (is (= "PONG"
             (-ping redis-conn)
             (-ping kvrocks-conn)))
      (is (= ["PONG"]
             (-ping-pipeline redis-conn)
             (-ping-pipeline kvrocks-conn)))
      (is (= ["OK" "1"]
             (-set-x redis-conn)
             (-set-x kvrocks-conn)))
      (is (= ["OK" ["1" "2" "3"]]
             (-hmset redis-conn)
             (-hmset kvrocks-conn)))
      (is (= ["OK" "1"]
             (-set-z-pipeline redis-conn)
             (-set-z-pipeline kvrocks-conn))))))
