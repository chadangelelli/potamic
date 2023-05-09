(ns potamic.queue-test
  "Test `potamic.queue`."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [taoensso.carmine :as car :refer [wcar]]
            [potamic.db :as db]
            [potamic.queue :as q]))

(def conn (db/make-conn {:uri "redis://localhost:6379/0"}))

(defn prime-db
  [f]
  (wcar conn (car/flushall))
  (q/create-queue :my/test-queue conn)
  (f)
  (wcar conn (car/flushall)))

(use-fixtures :each prime-db)

;;;; __________________________________________________ QUEUE SETUP
;;TODO: add test
(deftest Valid-Create-Queue-Opts-test
  (testing "potamic.queue/Valid-Create-Queue-Opts"
    (is (= 1 1))))

;;TODO: add error tests
(deftest create-queue-test
  (testing "potamic.queue/create-queue"
    (let [[ok? ?err] (q/create-queue :secondary/queue conn)]
      (is (= ok? true))
      (is (nil? ?err)))
    )) ; end create-queue-test

;;TODO: add error tests
(deftest get-queues-test
  (testing "potamic.queue/get-queues"
    (let [[ok? ?err] (q/create-queue :secondary/queue conn)
          all-queues (q/get-queues)]
      (is (= ok? true))
      (is (nil? ?err))
      (is (= all-queues
             {:my/test-queue
              {:group-name :my/test-queue-group,
               :queue-conn {:pool {}, :uri "redis://localhost:6379/0"},
               :queue-name :my/test-queue,
               :redis-group-name "my/test-queue-group",
               :redis-queue-name "my/test-queue"},
              :secondary/queue
              {:group-name :secondary/queue-group,
               :queue-conn {:pool {}, :uri "redis://localhost:6379/0"},
               :queue-name :secondary/queue,
               :redis-group-name "secondary/queue-group",
               :redis-queue-name "secondary/queue"}}))
      ))) ; end get-queues-test

;;TODO: add error tests
(deftest get-queue-test
  (testing "potamic.queue/get-queue"
    (is (= 1 1))
    (let [my-queue (q/get-queue :my/test-queue)]
      (is (= my-queue
             {:group-name :my/test-queue-group
              :queue-conn {:pool {} :uri "redis://localhost:6379/0"}
              :queue-name :my/test-queue
              :redis-group-name "my/test-queue-group"
              :redis-queue-name "my/test-queue"}))
      ))) ; end get-queue-test

;;;; __________________________________________________ DATA
;;TODO: add error tests
(deftest ->str-test
  (testing "potamic.queue/->str"
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
        (is (= (q/->str x) check)))
      ))) ; end ->str-test

;;TODO: add error tests
(deftest put-test
  (testing "potamic.queue/put"
    (testing "-- singular put (auto-id)"
      (let [[?ids ?err] (q/put :my/test-queue {:a 1})]
        (is (nil? ?err))
        (is (= (count ?ids) 1))
        (is (re-find #"\d+-\d+" (first ?ids)))))
    (testing "-- multi put"
      (let [[?ids ?err] (q/put :my/test-queue {:a 1} {:b 2} {:c 3})]
        (is (nil? ?err))
        (is (= (count ?ids) 3))
        (is (every? identity (mapv #(re-find #"\d+-\d+" %) ?ids)))))
    )) ; end put-test


;;;; __________________________________________________ READER

(deftest read-test
  (testing "potamic.queue/read"
    (is (= 1 1))

    )) ; end read-test

;;TODO: add test
(deftest create-reader-test
  (testing "potamic.queue/create-reader"
    (is (= 1 1))
    )) ; end create-reader-test

;;;; __________________________________________________ QUEUE TEARDOWN
;;TODO: add test
(deftest delete-queue-test
  (testing "potamic.queue/delete-queue"
    (is (= 1 1))
    )) ; end delete-queue-test

