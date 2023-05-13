(ns potamic.queue-test
  "Test `potamic.queue`."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as async]
            [taoensso.carmine :as car :refer [wcar]]
            [potamic.db :as db]
            [potamic.queue :as q]
            [potamic.util :as util]))

(def conn (first (db/make-conn :uri "redis://localhost:6379/0")))

(def id-pat #"\d+-\d+")

(defn prime-db
  [f]
  (wcar conn (car/flushall))
  (q/create-queue :my/test-queue conn)
  (f)
  (wcar conn (car/flushall)))

(use-fixtures :each prime-db)

(deftest create-queue-test
  (testing "potamic.queue/create-queue"
    (let [[ok? ?err] (q/create-queue :secondary/queue conn)]
      (is (= ok? true))
      (is (nil? ?err)))
    )) ; end create-queue-test

(deftest get-queues-test
  (testing "potamic.queue/get-queues"
    ;; first queue created in fixture
    (let [[ok? ?err] (q/create-queue :secondary/queue conn :group :second/group)
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
              {:group-name :second/group
               :queue-conn {:pool {}, :spec {:uri "redis://localhost:6379/0"}},
               :queue-name :secondary/queue,
               :redis-group-name "second/group",
               :redis-queue-name "secondary/queue"}}))
      ))) ; end get-queues-test

(deftest get-queue-test
  (testing "potamic.queue/get-queue"
    (let [my-queue (q/get-queue :my/test-queue)]
      (is (= (assoc-in my-queue [:queue-conn :pool] {})
             {:group-name :my/test-queue-group
              :queue-conn {:pool {}, :spec {:uri "redis://localhost:6379/0"}},
              :queue-name :my/test-queue
              :redis-group-name "my/test-queue-group"
              :redis-queue-name "my/test-queue"}))
      ))) ; end get-queue-test

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

(deftest read-test
  (testing "potamic.queue/read"
    (let [[_ _] (q/put :my/test-queue {:a 1} {:b 2} {:c 3})
          [read1-msgs ?read1-err] (q/read :my/test-queue)
          [read2-msgs ?read2-err] (q/read :my/test-queue :start 0)
          _ (async/go (async/<! (async/timeout 100)) (q/put :my/test-queue {:d 4}))
          [read3-msgs ?read3-err] (q/read :my/test-queue
                                          :count 1
                                          :start (:id (last read1-msgs))
                                          :block [120 :millis])]
      (is (nil? ?read1-err))
      (is (nil? ?read2-err))
      (is (nil? ?read3-err))
      (is (every? #(re-find id-pat %) (map :id read1-msgs)))
      (is (every? #(re-find id-pat %) (map :id read2-msgs)))
      (is (= read1-msgs read2-msgs))
      (is (re-find id-pat (:id (first read3-msgs))))
      (is (= 1 (count read3-msgs)))
      (is (= (:msg (first read3-msgs)) {:d "4"})))
    )) ; end read-test

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

(deftest read-pending-test
  (testing "potamic.queue/read-pending"
    (is (= 1 1))
    )) ; end read-pending-test

(deftest read-pending-summary-test
  (testing "potamic.queue/read-pending-summary"
    (let [qnm :my/test-queue
          [msg-ids ?put-err]   (q/put qnm {:a 1} {:b 2} {:c 3})
          [c1-msgs ?read1-err] (q/read-next! 2 :from qnm :as :my/consumer1)
          [p1-summary ?p1-err] (q/read-pending-summary qnm)
          [c2-msgs ?read2-err] (q/read-next! 1 :from qnm :as :my/consumer2)
          [p2-summary ?p2-err] (q/read-pending-summary qnm)]
      (is (nil? ?put-err))
      (is (nil? ?read1-err))
      (is (nil? ?read2-err))
      (is (nil? ?p1-err))
      (is (nil? ?p2-err))
      (is (every? #(re-find id-pat %) msg-ids))
      (is (every? #(re-find id-pat %) (map :id c1-msgs)))
      (is (= (:msg (first c1-msgs)) {:a "1"}))
      (is (= (:msg (second c1-msgs)) {:b "2"}))
      (is (every? #(re-find id-pat %) (map :id c2-msgs)))
      (is (= (:msg (first c2-msgs)) {:c "3"}))
      (is (= (:total p1-summary) 2))
      (is (re-find id-pat (:start p1-summary)))
      (is (re-find id-pat (:end p1-summary)))
      (is (= (:consumers p1-summary) {:my/consumer1 2}))
      (is (= (:total p2-summary) 3))
      (is (re-find id-pat (:start p2-summary)))
      (is (re-find id-pat (:end p2-summary)))
      (is (= (:consumers p2-summary) {:my/consumer1 2, :my/consumer2 1}))
      ))) ; end read-pending-summary-test

(deftest read-range-test
  (testing "potamic.queue/read-range"
    (is (= 1 1))
    )) ; end read-range-test

(deftest set-processed!-test
  (testing "potamic.queue/set-processed!"
    (is (= 1 1))
    )) ; end set-processed!-test

(deftest delete-queue-test
  (testing "potamic.queue/delete-queue"
    (let [[_ ?put-err] (q/put :my/test-queue {:a 1} {:b 2} {:c 3})
          [_ ?read-err] (q/read-next! 2 :from :my/test-queue :as :c/one)
          [no? ?e] (q/destroy-queue! :my/test-queue conn)
          [ok? ?destroy-err] (q/destroy-queue! :my/test-queue
                                               conn
                                               :unsafe true)]
      (is (nil? ?put-err))
      (is (nil? ?read-err))
      (is (nil? ?destroy-err))
      (is (nil? no?))
      (is (= true ok?))
      (is (map? ?e))
      (is (= (:potamic/err-type ?e) :potamic/db-err))
      (is (= (:potamic/err-msg ?e)
             "Cannot destroy my/test-queue, it has pending messages"))
      ))) ; end delete-queue-test

