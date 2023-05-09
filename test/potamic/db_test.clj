(ns potamic.db-test
  (:require [clojure.test :refer [deftest is testing]]

            [potamic.db :as db]))

;;TODO: add more tests
(deftest make-conn-test
  (testing "potamic.db/make-conn"
    (let [uri "redis://localhost:6379/0"]
      (is (= {:uri uri :pool {}} (db/make-conn {:uri uri})))
      ))) ; end make-conn-test
