(ns potamic.sentinel-test
  "Tests `st.queue`."
  {:added "5.0"
   :author "Chad Angelelli"}
  (:require
    [clojure.core.async :as async :refer [<! >! <!! >!!]]
    [clojure.test :refer (deftest is testing use-fixtures)]
    [potamic.db]
    [potamic.fmt :as fmt :refer [echo BOLD NC RED GREEN]]
    [potamic.queue :as q]
    [potamic.queue.queues :as queues]
    [potamic.sentinel :as s]
    [potamic.util :as u]
    [taoensso.carmine :as car :refer [wcar]])
  (:import [taoensso.carmine.connections ConnectionPool]))

(def queue-uri "redis://default:secret@localhost:6379/0")

(def conn (potamic.db/make-conn :uri queue-uri))

(defn check-queue-conn
  "Throws error if queue URI cannot be pinged."
  []
  (when (not= "PONG" (wcar conn (car/ping)))
    (throw (Exception. (str (fmt/make-prefix :error)
                            " Could not ping queue conn: "
                            fmt/BOLD queue-uri fmt/NC)))))
(check-queue-conn)

(defn prime-db
  [f]
  (wcar conn (car/flushall))
  (reset! queues/queues_ nil)
  (f))

(use-fixtures :each prime-db)

(def id-pat #"\d+-\d+")

(defn valid-ids?
  [?ids]
  (every? identity (map #(re-find id-pat (str %)) ?ids)))

(defn basic-sentinel
  [handler frequency]
  (s/create-sentinel
    {:queue-uri queue-uri
     :queue-name 'my/queue
     :queue-group 'my/group
     :frequency frequency
     :handler handler}))

(defmacro attr* [this x] `(s/get-attr ~this ~x))

(defn rand-int-between [mn mx] (+ (rand-int (- (+ 1 mx) mn)) mn))

(deftest create-sentinel-test
  (testing "st.queue/create-sentinel"
    (let [s' (basic-sentinel #(println (attr* % :n-runs)) 2000)
          s (update s' :queue-conn dissoc :pool)]
      (is (instance? ConnectionPool (get-in s' [:queue-conn :pool])))
      (is (= {:spec {:uri "redis://default:secret@localhost:6379/0"}}
             (:queue-conn s)))
      (is (= 'my/queue (:queue-name s)))
      (is (= 'my/group (:queue-group s)))
      (is (= 0 (:init-id s)))
      (is (= 2000 (:frequency s)))
      (is (= 0 (:start-offset s)))
      (is (= {:started? false
              :stopped? false
              :n-runs 0} (s/get-state s)))
      )))  ; end create-sentinel-test

(deftest sentinel-runtime-test
  (testing "st.queue/sentinel-runtime"
    (let [s (basic-sentinel
              (fn [this]
                (let [n-runs (attr* this :n-runs)
                      x2 (* n-runs 2)]
                  (s/set-attr this :last-n-runs n-runs)
                  (s/set-attr this :n-runs-times-2 x2)))
              1)]
      (try
        (testing "st.queue/start-sentinel!"
          (s/start-sentinel! s)
          (<!! (async/timeout 100))
          (dotimes [_ 3]
            (<!! (async/timeout 3))
            (testing "-- started?"
              (is (= (attr* s :started?) true)))
            (testing "-- (not) stopped?"
              (is (= (attr* s :stopped?) false)))
            (let [this-n (attr* s :n-runs)
                  last-n (attr* s :last-n-runs)
                  math-check (attr* s :n-runs-times-2)]
              (is (< last-n this-n))
              (is (= math-check (* last-n 2))))))
        (finally
          (testing "st.queue/stop-sentinel!"
            (s/stop-sentinel! s)
            (<!! (async/timeout 100))
            (testing "-- (not) started?"
              (is (= (attr* s :started?) false)))
            (testing "-- stopped?"
              (is (= (attr* s :stopped?) true))))))
      ))) ; end sentinel-runtime-test

(deftest sentinel-producer-consumer-test1
  (testing "queue read/write from within Sentinel"
    (let [s (basic-sentinel
              (fn [this]
                (let [qname (s/get-queue-name this)
                      n-runs (s/get-attr this :n-runs)]
                  (if (= n-runs 2)
                    (s/stop-sentinel! this)
                    (q/put qname {n-runs "Message put!"}))))
              10)]
      (s/start-sentinel! s)
      (<!! (async/timeout 500))
      (let [qname (s/get-queue-name s)
            consumer (s/get-queue-group s)
            [msgs ?err] (q/read-next! 1 :from qname :as consumer :block 500)]
        (is (nil? ?err))
        (is (= (count msgs) 1))
        (is (= (-> msgs first :msg) {"1" "Message put!"}))
        )))) ; end sentinel-producer-consumer-test1
