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
    [potamic.test-util :as tu :refer [redis-conn
                                      kvrocks-conn]]
    [potamic.util :as u]
    [taoensso.carmine :as car :refer [wcar]])
  (:import [taoensso.carmine.connections ConnectionPool]))

(defn check-queue-conn
  "Throws error if queue URI cannot be pinged."
  [conn]
  (when (not= "PONG" (wcar conn (car/ping)))
    (throw (Exception. (str (fmt/make-prefix :error)
                            " Could not ping queue conn: "
                            fmt/BOLD (assoc conn :pool {}) fmt/NC)))))
(check-queue-conn redis-conn)
(check-queue-conn kvrocks-conn)

(use-fixtures :each
              tu/fx-prime-flushall-kv-stores
              tu/fx-cleanup-flushall-kv-stores)

(def id-pat #"\d+-\d+")

(defn valid-ids?
  [?ids]
  (every? identity (map #(re-find id-pat (str %)) ?ids)))

(defn make-names
  [backend]
  (let [kns (name backend)]
    [(keyword kns "queue")
     (keyword kns "group")]))

(defn basic-sentinel
  [{:keys [backend] :as conn} handler frequency]
  (let [[qname group] (make-names backend)
        ?kvrocks-fields (when (= backend :kvrocks)
                          (select-keys (:spec conn)
                                       [:host :port :password :db]))]
    (s/create-sentinel
      (merge
        {:backend backend
         :queue-uri (get-in conn [:spec :uri])
         :queue-name qname
         :queue-group group
         :frequency frequency
         :handler handler}
        ?kvrocks-fields))))

(defmacro attr* [this x] `(s/get-attr ~this ~x))

(defn rand-int-between [mn mx]
  (+ (rand-int (- (+ 1 mx) mn)) mn))

(deftest create-sentinel-test
  (testing "st.queue/create-sentinel"
    (letfn [(-create-sentinel [conn]
              (let [[qname group] (make-names (:backend conn))
                    s' (basic-sentinel conn #(println (attr* % :n-runs)) 2000)
                    s (update s' :queue-conn dissoc :pool)]
                (is (instance? ConnectionPool (get-in s' [:queue-conn :pool])))
                (is (= {:backend :redis
                        :spec {:uri "redis://default:secret@localhost:6379/0"}}
                       (:queue-conn s)))
                (is (= qname (:queue-name s)))
                (is (= group (:queue-group s)))
                (is (= 0 (:init-id s)))
                (is (= 2000 (:frequency s)))
                (is (= 0 (:start-offset s)))
                (is (= {:started? false
                        :stopped? false
                        :n-runs 0} (s/get-state s)))))]
      (is (= 1 1))
      (-create-sentinel redis-conn)
      (-create-sentinel kvrocks-conn))))

#_(deftest sentinel-runtime-test
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

#_(deftest sentinel-producer-consumer-test1
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
