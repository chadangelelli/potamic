(ns potamic.db
  "Redis DB functionality."
  (:require [potamic.db.validation :as dbv]
            [potamic.errors :as e]
            [potamic.util :as pu]
            [potamic.validation :as v]
            [taoensso.carmine :as car :refer [wcar]])
  (:gen-class))

(def kvrocks-namespaces_ (atom #{}))

(defn make-conn
  "Creates a connection for Redis or Kvrocks. Returns `conn` or throws
  Potamic Error. On success, `conn` will be usable by `potamic.queue` and
  the underlying `taoensso.carmine` library.

  _NOTE_: If using `:kvrocks` backend, you must provide credentials as map.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db])

  ;; Redis backend
  (db/make-conn :uri \"redis://localhost:6379/0\")
  ;= {:backend :redis
  ;=  :spec {:uri \"redis://localhost:6379/0\"}
  ;=  :pool #taoensso.carmine.connections.ConnectionPool[..]}

  ;; Kvrocks backend
  (db/make-conn :backend :kvrocks
                :host \"127.0.0.1\"
                :port 6666
                :password \"secret\"
                :db 0)
  {:backend :kvrocks
   :spec {:host \"127.0.0.1\"
          :port 6666
          :password \"secret\"
          :db 0)}
   :pool #taoensso.carmine.connections.ConnectionPool[..]}
  ```"
  [& opts]
  (let [{:keys [backend uri pool] :as args} (apply hash-map opts)]
    (if-let [args-err (v/invalidate dbv/Valid-Make-Conn-Args args)]
      (let [err (e/error {:potamic/err-type :potamic/args-err
                          :potamic/err-fatal? true
                          :potamic/err-msg (str "Invalid args provided to "
                                                "potamic.db/make-conn")
                          :potamic/err-data {:args args :err args-err}})]
        (e/throw-potamic-error err))
      (let [pool (or pool (car/connection-pool {}))]
        (if (= backend :kvrocks)
          (let [spec (select-keys args #{:host :port :password :db})
                db (:db spec)
                conn {:backend :kvrocks
                      :spec spec
                      :pool pool}]
            (when-not (contains? @kvrocks-namespaces_ db)
              (try
                (wcar conn
                      (car/auth (:password spec))
                      (car/redis-call ["NAMESPACE" "ADD" db db]))
                (catch Exception e
                  (let [err (e/error
                              {:potamic/err-type :potamic/db-err
                               :potamic/err-fatal? true
                               :potamic/err-msg (str "Can't use provided "
                                                     "Kvrocks credentials "
                                                     "in potamic.db/make-conn")
                               :potamic/err-data {:args args :err e}})]
                    (e/throw-potamic-error err))))
              (swap! kvrocks-namespaces_ conj db))
            conn)
          {:backend :redis
           :spec {:uri uri}
           :pool pool})))))

(defn key-exists?
  "Returns boolean after checking if key exists in DB.

  Examples:

  ```clojure
  (require '[potamic.db :as db]
           '[potamic.queue :as q])

  (def conn (db/make-conn :uri \"redis://localhost:6379/0\"))
  ;= {:spec
  ;=  {:uri \"redis://localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool[..]}

  (q/create-queue :my/queue conn)
  ;= [true nil]

  (db/key-exists? :my/queue conn)
  ;= true
  ```"
  [k conn]
  (try
    (let [r (wcar conn (car/exists (pu/->str k)))
          r (if (string? r) (Integer/parseInt r) r)]
      (> r 0))
    (catch Exception e
      (let [err (e/error {:potamic/err-type :potamic/internal-err
                          :potamic/err-fatal? false
                          :potamic/err-msg (.getMessage e)
                          :potamic/err-data {:args {:k k :conn (dissoc conn :pool)}
                                             :err (Throwable->map e)}})]
        (e/throw-potamic-error err)))))

(defn kvres
  "(Kvrocks only) Matches standard Carmine response signature. If a vector
  greater than 1 is returned, treat it is a pipelined response, like Carmine."
  [resp as-pipeline?]
  (let [slice (subvec resp 1)]
    (if as-pipeline?
      slice
      (if (= (count slice) 1)
        (first slice)
        slice))))

(defmacro wcar*
  "Rewrites calls to `taoensso.carmine/wcar` for different backends
  (e.g. :redis vs :kvrocks).

  Examples:

  - Use same as `wcar`."
  [conn & [x & xs :as args]]
  `(let [pipeline?# (= :as-pipeline (quote ~x))
         conn# ~conn]
    (case (:backend conn#)
      :redis (if pipeline?#
               (wcar ~conn :as-pipeline ~@xs)
               (wcar ~conn ~@args))
      :kvrocks (let [db# (get-in conn# [:spec :db])]
                 (if pipeline?#
                   (kvres (wcar ~conn :as-pipeline (car/auth db#) ~@xs)
                          pipeline?#)
                   (kvres (wcar ~conn (car/auth db#) ~@args)
                          pipeline?#))))))
