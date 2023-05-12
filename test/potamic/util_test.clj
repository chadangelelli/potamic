(ns potamic.util-test
  (:require [clojure.test :refer [deftest is testing]]
            [potamic.util :as util]))

  ;;TODO: add error tests
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


