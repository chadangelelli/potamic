(ns potamic.queue-test
  "Test `potamic.queue`."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [taoensso.carmine :as car :refer [wcar]]
            [potamic.db :as db]
            [potamic.queue :as q]
            [potamic.util :as util]))

(def conn (first (db/make-conn {:uri "redis://localhost:6379/0"})))

(def id-pat #"\d+-\d+")

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
      (is (= (-> all-queues
                 (assoc-in [:my/test-queue :queue-conn :pool] {})
                 (assoc-in [:secondary/queue :queue-conn :pool] {}))
             {:my/test-queue
              {:group-name :my/test-queue-group,
               :queue-conn {:pool {}, :spec {:uri "redis://localhost:6379/0"}},
               :queue-name :my/test-queue,
               :redis-group-name "my/test-queue-group",
               :redis-queue-name "my/test-queue"},
              :secondary/queue
              {:group-name :secondary/queue-group,
               :queue-conn {:pool {}, :spec {:uri "redis://localhost:6379/0"}},
               :queue-name :secondary/queue,
               :redis-group-name "secondary/queue-group",
               :redis-queue-name "secondary/queue"}}))
      ))) ; end get-queues-test

;;TODO: add error tests
(deftest get-queue-test
  (testing "potamic.queue/get-queue"
    (is (= 1 1))
    (let [my-queue (q/get-queue :my/test-queue)]
      (is (= (assoc-in my-queue [:queue-conn :pool] {})
             {:group-name :my/test-queue-group
              :queue-conn {:pool {}, :spec {:uri "redis://localhost:6379/0"}},
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
        (is (= (util/->str x) check)))
      ))) ; end ->str-test

;;TODO: add error tests
(deftest put-test
  (testing "potamic.queue/put"
    (testing "-- singular put (auto-id)"
      (let [[?ids ?err] (q/put :my/test-queue {:a 1})]
        (is (nil? ?err))
        (is (= (count ?ids) 1))
        (is (re-find id-pat (first ?ids)))))
    (testing "-- multi put"
      (let [[?ids ?err] (q/put :my/test-queue {:a 1} {:b 2} {:c 3})]
        (is (nil? ?err))
        (is (= (count ?ids) 3))
        (is (every? identity (mapv #(re-find id-pat %) ?ids)))))
    )) ; end put-test

;;;; __________________________________________________ READER

(deftest read-next!-test
  (testing "potamic.queue/read-next!"
    (let [[?ids ?err] (q/put :my/test-queue {:a 1} {:b 2} {:c 3})]
      (is (nil? ?err))
      (is (= (count ?ids) 3))
      (is (every? identity (mapv #(re-find id-pat %) ?ids))))
    (testing "-- read-next! 1"
      (let [[?msgs ?e] (q/read-next! 1 :from :my/test-queue :as :my/consumer1)]
        (is (nil? ?e))
        (is (= 1 (count ?msgs)))
        (is (re-find id-pat (:id (first ?msgs))))
        (is (= (:msg (first ?msgs)) {:a "1"}))))
    (testing "-- read-next! :all"
      (let [[?msgs ?e] (q/read-next! 2 :from :my/test-queue :as :my/consumer1)]
        (is (nil? ?e))
        (is (= 2 (count ?msgs)))
        (is (re-find id-pat (:id (first ?msgs))))
        (is (re-find id-pat (:id (second ?msgs))))
        (is (= (:msg (first ?msgs)) {:b "2"}))
        (is (= (:msg (second ?msgs)) {:c "3"}))))
    )) ; end read-next!-test

;;;; __________________________________________________ QUEUE TEARDOWN
;;TODO: add test
(deftest delete-queue-test
  (testing "potamic.queue/delete-queue"
    (is (= 1 1))
    )) ; end delete-queue-test

