(ns potamic.validation-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.validation :as v]
            [potamic.db-test :as db-test]))

(deftest f-test
  (testing "potamic.validation/f"
    (let [[k err-map f] (v/f int? "Invalid integer")]
      (is (= k :fn))
      (is (= err-map {:error/message "Invalid integer"}))
      (is (fn? f)))
    )) ; end f-test

(deftest invalidate-test
  (testing "potamic.validation/invalidate"
    (is (nil? (v/invalidate int? 1)))
    (is (nil? (v/invalidate [:enum 1 2 3] 2)))
    (is (nil? (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                            {:a 52 :b "ok"})))
    (is (= (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                         [:my :vector])
           ["invalid type"]))
    (is (= (v/invalidate [:map {:closed true} [:a int?] [:b string?]]
                         {:c 1 :d "ok"})
           {:a ["missing required key"]
            :b ["missing required key"]
            :c ["disallowed key"]
            :d ["disallowed key"]}))
    )) ; end invalidate-test

(deftest valid-redis-uri?-test
  (testing "potamic.validation/valid-redis-uri?"
    (doseq [uri db-test/valid-redis-uris]
      (is (v/valid-redis-uri? uri)))
    )) ; end valid-redis-uri?-test
