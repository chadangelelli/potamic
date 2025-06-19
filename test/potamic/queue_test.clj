(ns potamic.queue-test
  "Test `potamic.queue`."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :as walk]
            [clojure.core.async :as async]
            [taoensso.carmine :as car]
            [potamic.db :as db]
            [potamic.fmt :as fmt :refer [echo BOLD NC]]
            [potamic.queue :as q]
            [potamic.queue.queues :as queues]
            [potamic.test-util :as tu :refer [redis-conn
                                              redis-test-queue
                                              redis-test-queue-group
                                              kvrocks-conn
                                              kvrocks-test-queue
                                              kvrocks-test-queue-group]]))

(def id-pat #"\d+-\d+")

(use-fixtures :each tu/fx-prime-queues)

(deftest create-queue!-test
  (testing "potamic.queue/create-queue!"
    (letfn [(-create-queue [conn]
              (let [qname (keyword (name (:backend conn)) "secondary-queue")]
                (q/destroy-queue! qname conn :unsafe true)
                (let [[status ?err] (q/create-queue! qname conn)]
                  (is (= :created-with-new-stream status))
                  (is (nil? ?err)))))]
      (testing "| Redis"
        (-create-queue redis-conn))
      (testing "| Kvrocks"
        (-create-queue kvrocks-conn)))))

(def ^:private -correct-all-queues
  {:kvrocks/secondary-queue {:group-name :kvrocks/secondary-queue-group
                             :queue-conn {:backend :kvrocks
                                          :spec {:db 0
                                                 :host "127.0.0.1"
                                                 :password "secret"
                                                 :port 6666}}
                             :queue-name :kvrocks/secondary-queue
                             :redis-group-name "kvrocks/secondary-queue-group"
                             :redis-queue-name "kvrocks/secondary-queue"}
   :kvrocks/test-queue
   {:group-name :kvrocks/test-queue-group
                        :queue-conn {:backend :kvrocks
                                     :spec {:db 0
                                            :host "127.0.0.1"
                                            :password "secret"
                                            :port 6666}}
                        :queue-name :kvrocks/test-queue
                        :redis-group-name "kvrocks/test-queue-group"
                        :redis-queue-name "kvrocks/test-queue"}
   :redis/secondary-queue
   {:group-name :redis/secondary-queue-group
    :queue-conn {:backend :redis
                 :spec {:uri "redis://default:secret@localhost:6379/0"}}
    :queue-name :redis/secondary-queue
    :redis-group-name "redis/secondary-queue-group"
    :redis-queue-name "redis/secondary-queue"}
   :redis/test-queue
   {:group-name :redis/test-queue-group
    :queue-conn {:backend :redis
                 :spec {:uri "redis://default:secret@localhost:6379/0"}}
    :queue-name :redis/test-queue
    :redis-group-name "redis/test-queue-group"
    :redis-queue-name "redis/test-queue"}})

(deftest get-queues-test
  (testing "potamic.queue/get-queues"
    (letfn [(-create-queue [{:keys [backend] :as conn}]
              (let [[qname group] (case backend
                                    :redis [:redis/secondary-queue
                                            :redis/secondary-queue-group]
                                    :kvrocks [:kvrocks/secondary-queue
                                            :kvrocks/secondary-queue-group])]
                (q/create-queue! qname conn :group group)))]
        ;; first queue created in fixture
        (let [[redis-status ?redis-err] (-create-queue redis-conn)]
          (is (= :created-with-new-stream redis-status))
          (is (nil? ?redis-err)))
        (let [[kvrocks-status ?kvrocks-err] (-create-queue kvrocks-conn)]
          (is (= :created-with-new-stream kvrocks-status))
          (is (nil? ?kvrocks-err)))
        (is (= -correct-all-queues
               (walk/postwalk (fn [x] (if (map? x) (dissoc x :pool) x))
                              (q/get-queues))))
        ))) ; end get-queues-test

(def ^:private -correct-redis-queue
  {:group-name :redis/test-queue-group,
   :queue-conn {:backend :redis
                :pool {}
                :spec {:uri "redis://default:secret@localhost:6379/0"}}
   :queue-name :redis/test-queue,
   :redis-group-name "redis/test-queue-group",
   :redis-queue-name "redis/test-queue"})

(def ^:private -correct-kvrocks-queue
  {:group-name :kvrocks/test-queue-group,
   :queue-conn {:backend :kvrocks,
                :pool {},
                :spec {:db 0, :host "127.0.0.1", :password "secret", :port 6666}},
   :queue-name :kvrocks/test-queue,
   :redis-group-name "kvrocks/test-queue-group",
   :redis-queue-name "kvrocks/test-queue"})

(deftest get-queue-test
  (testing "potamic.queue/get-queue"
    (letfn [(-get-queue [{:keys [backend]}]
              (let [qname (tu/get-default-test-queue backend)
                    my-queue (q/get-queue qname)]
                (case backend
                  :redis (is (= -correct-redis-queue
                                (assoc-in my-queue [:queue-conn :pool] {})))
                  :kvrocks (is (= -correct-kvrocks-queue
                                (assoc-in my-queue [:queue-conn :pool] {}))))))]
      (testing "| Redis"
        (-get-queue redis-conn))
      (testing "| Kvrocks"
        (-get-queue kvrocks-conn)))))

(deftest put-test
  (testing "potamic.queue/put"
    (letfn [(-put [{:keys [backend]}]
              (let [test-queue (tu/get-default-test-queue backend)]
                (testing "-- manually set id, singular msg"
                  (let [[?ids1 ?err1] (q/put test-queue "1683743739-0" {:a 1})
                        [_ err4] (q/put :invalid/queue {:bad :call})]
                    (is (nil? ?err1))
                    (is (sequential? ?ids1))
                    (is (= (count ?ids1) 1))
                    (is (every? #(re-find id-pat %) ?ids1))
                    (is (= "No matching clause: " (:potamic/err-msg err4)))))
                (testing "-- multiple msg's"
                  (let [[?ids ?err] (q/put test-queue {:a 1} {:b 2} {:c 3})]
                    (is (nil? ?err))
                    (is (= (count ?ids) 3))
                    (is (every? #(re-find id-pat %) ?ids))))
                (testing "-- singular put (auto-id)"
                  (let [[?ids ?err] (q/put test-queue {:a 1})]
                    (is (nil? ?err))
                    (is (= (count ?ids) 1))
                    (is (re-find id-pat (first ?ids)))))))]
      (testing "| Redis"
        (-put redis-conn))
      (testing "| Kvrocks"
        (-put kvrocks-conn)))))

(deftest read-test
  (testing "potamic.queue/read"
    (letfn [(-read [{:keys [backend]}]
              (let [test-queue (tu/get-default-test-queue backend)
                    [_ _] (q/put test-queue {:a 1} {:b 2} {:c 3})
                    [read1-msgs ?read1-err] (q/read test-queue)
                    [read2-msgs ?read2-err] (q/read test-queue :start 0)
                    _ (async/go (async/<! (async/timeout 100))
                                (q/put test-queue {:d 4}))
                    [read3-msgs ?read3-err] (q/read
                                              test-queue
                                              :count 1
                                              :start (:id (last read1-msgs))
                                              :block [120 :millis])
                    ]
                (is (nil? ?read1-err))
                (is (nil? ?read2-err))
                (is (nil? ?read3-err))
                (is (every? #(re-find id-pat %) (map :id read1-msgs)))
                (is (every? #(re-find id-pat %) (map :id read2-msgs)))
                (is (= read1-msgs read2-msgs))
               ;(is (re-find id-pat (:id (first read3-msgs))))
               ;(is (= 1 (count read3-msgs)))
               ;(is (= (:msg (first read3-msgs)) {:d 4}))
                ))]
      (testing "| Redis"
        (-read redis-conn))
      (testing "| Kvrocks"
        (-read kvrocks-conn)))))

#_(deftest read-next!-test
  (testing "potamic.queue/read-next!"
    (letfn [(-read-next! [{:keys [backend]}]
              (let [test-queue (tu/get-default-test-queue backend)
                    [?ids ?err] (q/put test-queue {:a 1} {:b 2} {:c 3})]
                (is (nil? ?err))
                (is (= (count ?ids) 3))
                (is (every? identity (mapv #(re-find id-pat %) ?ids)))
                (testing "-- read-next! 1"
                  (let [[?msgs ?e] (q/read-next! 1
                                                 :from test-queue
                                                 :as :my/consumer1)]
                    (is (nil? ?e))
                    (is (= 1 (count ?msgs)))
                    (is (re-find id-pat (:id (first ?msgs))))
                    (is (= (:msg (first ?msgs)) {:a 1}))))
                (testing "-- read-next! :all"
                  (let [[?msgs ?e] (q/read-next! 2
                                                 :from test-queue
                                                 :as :my/consumer1)]
                    (is (nil? ?e))
                    (is (= 2 (count ?msgs)))
                    (is (re-find id-pat (:id (first ?msgs))))
                    (is (re-find id-pat (:id (second ?msgs))))
                    (is (= (:msg (first ?msgs)) {:b 2}))
                    (is (= (:msg (second ?msgs)) {:c 3}))))))]
      (testing "| Redis"
        (-read-next! redis-conn))
      (testing "| Kvrocks"
        (-read-next! kvrocks-conn)))))

#_(deftest read-pending-test
  (testing "potamic.queue/read-pending"
    (letfn [(-read-pending [{:keys [backend]}]
              (let [test-queue (tu/get-default-test-queue backend)
                    [_ _] (q/put test-queue {:a 1} {:b 2} {:c 3})
                    [_ _] (q/read-next! 1 :from test-queue :as :consumer/one)
                    [read1 ?read1-err] (q/read-pending 10
                                                       :from test-queue
                                                       :for :consumer/one)
                    [read2 ?read2-err] (q/read-pending 10
                                                       :from test-queue
                                                       :for :consumer/one
                                                       :start '-
                                                       :end '+)
                    [read3 ?read3-err] (q/read-pending
                                         1
                                         :from test-queue
                                         :for :consumer/one
                                        ;:start (:id (first read1))
                                        ;:end  (:id (last read2))
                                         )]
                (is (= :read1 read1))
                (is (= :read2 read2))
                (is (= :read3 read3))
                (is (nil? ?read1-err))
                (is (nil? ?read2-err))
                (is (nil? ?read3-err))
                (is (sequential? read1))
                (is (sequential? read2))
                (is (sequential? read3))
                (is (= 1 (count read1)))
                (is (= 1 (count read2)))
                (is (= 1 (count read3)))
                (is (= :WTF? read3))
               ;(is (= (:id (first read1)) (:id (first read2))))
               ;(is (= (:id (first read2)) (:id (first read3))))
               ;(is (= (:id (first read1)) (:id (first read3))))
                ))]
      (testing "| Redis"
        )
      (testing "| Kvrocks"
        )
      (testing "| Redis"
        (-read-pending redis-conn))
      (testing "| Kvrocks"
        (-read-pending kvrocks-conn)))))

#_(deftest read-pending-summary-test
  (testing "potamic.queue/read-pending-summary"
    (let [qnm test-queue
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
      (is (= (:msg (first c1-msgs)) {:a 1}))
      (is (= (:msg (second c1-msgs)) {:b 2}))
      (is (every? #(re-find id-pat %) (map :id c2-msgs)))
      (is (= (:msg (first c2-msgs)) {:c 3}))
      (is (= (:total p1-summary) 2))
      (is (re-find id-pat (:start p1-summary)))
      (is (re-find id-pat (:end p1-summary)))
      (is (= (:consumers p1-summary) {:my/consumer1 2}))
      (is (= (:total p2-summary) 3))
      (is (re-find id-pat (:start p2-summary)))
      (is (re-find id-pat (:end p2-summary)))
      (is (= (:consumers p2-summary) {:my/consumer1 2, :my/consumer2 1}))
      ))) ; end read-pending-summary-test

#_(deftest read-range-test
  (testing "potamic.queue/read-range"
    (let [[_ _] (q/put test-queue {:a 1} {:b 2} {:c 3})
          [r1 ?e1] (q/read-range test-queue :start '- :end '+)
          [r2 ?e2] (q/read-range test-queue :start '- :end '+ :count 10)]
      (is (nil? ?e1))
      (is (nil? ?e2))
      (is (every? #(re-find id-pat %) (map :id r1)))
      (is (every? #(re-find id-pat %) (map :id r2)))
      (is (= (:msg (first r1)) {:a 1}))
      (is (= (:msg (second r1)) {:b 2}))
      (is (= (:msg (nth r1 2)) {:c 3}))
      (is (= (:msg (first r2)) {:a 1}))
      (is (= (:msg (second r2)) {:b 2}))
      (is (= (:msg (nth r2 2)) {:c 3}))
      (is (= (:msg (first r1)) (:msg (first r2)))))
    )) ; end read-range-test

#_(deftest set-processed!-test
  (testing "potamic.queue/set-processed!"
    (let [_ (q/put test-queue {:a 1} {:b 2} {:c 3})
          [msgs ?read-err] (q/read-next! 3
                                         :from test-queue
                                         :as test-queue-group)
          ids (map :id msgs)
          [n-acked ?ack-err] (apply q/set-processed! test-queue ids)]
      (is (nil? ?read-err))
      (is (nil? ?ack-err))
      (is (= 3 n-acked))
      ))) ; end set-processed!-test

#_(deftest delete-queue-test
  (testing "potamic.queue/delete-queue"
    (let [[_ ?put-err] (q/put test-queue {:a 1} {:b 2} {:c 3})
          [_ ?read-err] (q/read-next! 2 :from test-queue :as :c/one)
          [nil-response safe-block-err] (q/destroy-queue! test-queue conn)
          [destroyed-status ?destroy-err] (q/destroy-queue! test-queue
                                                            conn
                                                            :unsafe true)
          [nonexistent-status ?nonexistent-err] (q/destroy-queue! test-queue
                                                                  conn)]
      (is (nil? ?put-err))
      (is (nil? ?read-err))
      (is (nil? nil-response))
      (is (= :potamic/db-err
             (:potamic/err-type safe-block-err)))
      (is (re-find #"Cannot\s+destroy.+?,\s+it has pending messages"
                   (:potamic/err-msg safe-block-err)))
      (is (nil? ?destroy-err))
      (is (= :spec-destroyed_stream-destroyed destroyed-status))
      (is (nil? ?nonexistent-err))
      (is (= :spec-nonexistent_stream-nonexistent nonexistent-status))
      ))) ; end delete-queue-test

#_(deftest create-destroy-cycle-test
  (testing "creating > destroying > creating cycle"
    (q/destroy-queue! test-queue conn :unsafe true)
    (wcar conn (car/flushall))
    (let [[create1-res ?create1-err] (q/create-queue! test-queue conn)
          [destroy1-res ?destroy1-err] (q/destroy-queue! test-queue conn)
          [create2-res ?create2-err] (q/create-queue! test-queue conn)
          [destroy2-res ?destroy2-err] (q/destroy-queue! test-queue conn)
          [create3-res ?create3-err] (q/create-queue! test-queue conn)
          [create4-res ?create4-err] (q/create-queue! test-queue conn)]
      (is (= :created-with-new-stream create1-res))
      (is (nil? ?create1-err))
      (is (= :spec-destroyed_stream-destroyed destroy1-res))
      (is (nil? ?destroy1-err))
      (is (= :created-with-new-stream create2-res))
      (is (nil? ?create2-err))
      (is (= :spec-destroyed_stream-destroyed destroy2-res))
      (is (nil? ?destroy2-err))
      (is (= :created-with-new-stream create3-res))
      (is (nil? ?create3-err))
      (is (= :updated-with-existing-stream create4-res))
      (is (nil? ?create4-err)))))

