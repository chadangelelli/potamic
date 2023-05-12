(ns potamic.db-test
  (:require [clojure.test :refer [deftest is testing]]

            [potamic.db :as db]))

(deftest make-conn-test
  (testing "potamic.db/make-conn"
    (let [valid-uris ["redis://localhost:6379/0"
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
                      "redis://:PASS@111.222.333.444:6379/0"]]
      (doseq [uri valid-uris
              :let [[?conn ?err] (db/make-conn {:uri uri})]]
        (is (nil? ?err))
        (is (= {:uri uri :pool {}} ?conn))))
    )) ; end make-conn-test
