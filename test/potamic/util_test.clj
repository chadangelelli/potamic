(ns potamic.util-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.util :as util]))

(deftest ->str-test
  (testing "potamic.util/->str"
    (let [should-pass {:my/queue "my/queue"
                       'my/queue "my/queue"
                       "my/queue" "my/queue"
                       :x "x"
                       'x "x"
                       "x" "x"
                       :a.b/c.d "a.b/c.d"
                       'a.b/c.d "a.b/c.d"
                       "a.b/c.d" "a.b/c.d"}]
      (doseq [[x check] should-pass]
        (is (= (util/->str x) check)))
      ))) ; end ->str-test

(deftest ->int-test
  (testing "potamic.util/->int"
    (is (= 1 (util/->int "1")))
    (is (= 66 (util/->int "66")))
    )) ; end ->int-test

(deftest <-str
  (testing "potamic.util/<-str"
    (is (= :x/y (util/<-str "x/y")))
    (is (= "111" (util/<-str "111")))
    (is (= 111 (util/<-str 111)))
    )) ; end <-str

(deftest prep-cmd
  (testing "potamic.util/prep-cmd"
    (is (= ["a" "b" "c"] (util/prep-cmd [[:a] ['b] ["c"]])))
    (is (= ["a" "b" "c"] (util/prep-cmd [["a"] ['b] ["c"]])))
    (is (= ["a" "b" "c" "d" "e" "f"]
           (util/prep-cmd [[[['a]] 'b [[:c]] 'd] "e" "f"])))
    )) ; end prep-cmd

(deftest time->milliseconds-test
  (testing "potamic.util/time->milliseconds"
    (is (= (util/time->milliseconds [2 :milli]) 2))
    (is (= (util/time->milliseconds [2 :millis]) 2))
    (is (= (util/time->milliseconds [2 :second]) 2000))
    (is (= (util/time->milliseconds [2 :seconds]) 2000))
    (is (= (util/time->milliseconds [2 :minute]) 120000))
    (is (= (util/time->milliseconds [2 :minutes]) 120000))
    (is (= (util/time->milliseconds [2 :hour]) 7200000))
    (is (= (util/time->milliseconds [2 :hours]) 7200000))
    )) ; end time->milliseconds-test
