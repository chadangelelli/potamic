(ns potamic.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.db :as db]))

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

(deftest make-conn-test
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
             (assoc ?conn :pool {})))
      ))) ; end make-conn-test
