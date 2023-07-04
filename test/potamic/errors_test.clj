(ns potamic.errors-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.errors :as e]
            [potamic.errors.types :as et]))

(deftest error-types-test
  (testing "potamic.errors/error-types"
    (is (= et/error-types
           #{:potamic/args-err
             :potamic/db-err
             :potamic/internal-err}))
    )) ; end error-types-test

(deftest error-test
  (testing "potamic.errors/error"
    (let [{:keys [potamic/err-file
                  potamic/err-line
                  potamic/err-column
                  potamic/err-msg
                  potamic/err-type
                  potamic/err-data]
           } (e/error {:potamic/err-type :potamic/args-err
                       :potamic/err-fatal? false
                       :potamic/err-msg "ERROR MSG"
                       :potamic/err-data {:a 1 :b 2}})]
      (is (int? err-line))
      (is (int? err-column))
      (is (not= err-line 0))
      (is (not= err-column 0))
      (is (= err-file "potamic/errors_test.clj"))
      (is (= err-data {:a 1 :b 2}))
      (is (= err-msg "ERROR MSG"))
      (is (= err-type :potamic/args-err))
      ))) ; end error-test
