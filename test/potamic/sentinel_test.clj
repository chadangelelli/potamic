(ns potamic.sentinel-test
  "Tests `st.queue`."
  {:added "5.0"
   :author "Chad Angelelli"}
  (:require
    [clojure.core.async :as async :refer [<! >! <!! >!!]]
    [clojure.test :refer (deftest is testing use-fixtures)]
    [potamic.db]
    [potamic.fmt :as fmt]
    [potamic.queue :as q]
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

(defn flush-db
  [f]
  (wcar conn (car/flushall))
  (f))

(use-fixtures :once flush-db)

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

(defmacro attr* [this x] `(s/get-sentinel-state-attr ~this ~x))

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
              :n-runs 0} (s/get-sentinel-state s)))
      )))  ; end create-sentinel-test

(deftest sentinel-runtime-test
  (testing "st.queue/sentinel-runtime"
    (let [s (basic-sentinel
              (fn [this]
                (let [n-runs (attr* this :n-runs)
                      x2 (* n-runs 2)]
                  (s/set-sentinel-state-attr this :last-n-runs n-runs)
                  (s/set-sentinel-state-attr this :n-runs-times-2 x2)))
              1)]
      (try
        (testing "st.queue/start-sentinel!"
          (s/start-sentinel! s)
          (Thread/sleep 100)
          (dotimes [_ 3]
            (Thread/sleep 3)
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
            (Thread/sleep 100)
            (testing "-- (not) started?"
              (is (= (attr* s :started?) false)))
            (testing "-- stopped?"
              (is (= (attr* s :stopped?) true))))))
      ))) ; end sentinel-runtime-test

;;TODO: add producer/consumer tests


