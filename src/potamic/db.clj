(ns potamic.db
  "Redis DB functionality."
  (:require [potamic.db.validation :as dbv]
            [potamic.errors :as e]
            [potamic.util :as pu]
            [potamic.validation :as v]
            [taoensso.carmine :as car :refer [wcar]])
  (:gen-class))

(defn kvrocks-response
  "(Kvrocks only) Matches standard Carmine response signature. The following
  is performed on Potamic/Kvrocks responses:

  1. Call `(subvec resp 1)` to remove the injected AUTH command response.
  2. If `:as-pipeline` is true, do not further modify response.
  3. If length of subvec is 1, return the first value (to mimic Carmine).
  4. Otherwise, return vector, as-is."
  [resp pipeline?]
  (println "--> potamic.db/kvrocks-response > 0. resp:" resp)
  (let [slice (subvec resp 1)
        ret (if pipeline?
              slice
              (if (= 1 (count slice))
                (first slice)
                slice))]
    (println "--> potamic.db/kvrocks-response > 1. slice:" slice)
    (println "--> potamic.db/kvrocks-response > 2. pipeline?:" pipeline?)
    (println "--> potamic.db/kvrocks-response > 2. ret:" ret)
    ret
    ))

(defmacro wcar*
  "Rewrites calls to `taoensso.carmine/wcar` for different backends
  (e.g. :redis vs :kvrocks).

  Examples:

  - Use same as `wcar`."
  [conn & [x & xs :as args]]
  `(let [conn# ~conn]
     (case (:backend conn#)
       :redis (wcar ~conn ~@args)
       :kvrocks
       (let [db# (get-in conn# [:spec :db])
             pipeline?# (= :as-pipeline (quote ~x))]
         (if pipeline?#
           (kvrocks-response
             (wcar ~conn :as-pipeline (vec (cons (car/auth db#) ~@xs)))
             pipeline?#)
           (kvrocks-response
             (wcar ~conn (vec (cons (car/auth db#) ~@args)))
             pipeline?#))))))

(def kvrocks-namespaces_
  "Set of Kvrocks namespaces in use. Simple schema of `int:int` (e.g. `0:0`)
  for `namespace:token` is used to mimic Redis `DB`'s, while allowing the
  `AUTH` command to be used to switch namespaces, much like Redis standard
  `SELECT` does.

  > _NOTE_: This is automatically handled via `potamic.db/make-conn`
  > and potamic.db/wcar*`.

  See also:

  - `potamic.db/make-conn`
  - `potamic.db/wcar*`"
  (atom #{}))

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
                          :potamic/err-fn 'potamic.db/make-conn
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
                               :potamic/err-fn 'potamic.db/make-conn
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
  (println "==================================================")
  (println "--> potamic.db/key-exists? > -1. conn:" (type conn) (dissoc conn :pool))
  (println "--> potamic.db/key-exists? > 0. k:" (type k) k)
  (let [k-str (pu/->str k)
        cmd (list 'wcar* 'conn (list 'car/exists :as-pipeline (pu/->str k)))
        ? (wcar* conn (car/exists :as-pipeline (pu/->str k)))
        ]
    (println "--> potamic.db/key-exists? > 0b. k-str:" (type k-str) k-str)
    (println "--> potamic.db/key-exists? > 1. cmd:" (type cmd) cmd)
    (println "--> potamic.db/?ey-exists? > 2. ?:" (type ?) ?)
    (println "==================================================")
    )
  (try
    (let [r (wcar* conn (car/exists :as-pipeline (pu/->str k)))
         ;; (wcar* conn :as-pipeline (car/exists (pu/->str k)))
          _ (println "--> potamic.db/key-exists? > 3. r:" (type r) r)
          r (if (string? r) (Integer/parseInt r) r)
          _ (println "--> potamic.db/key-exists? > 4. r:" (type r) r)
          ]
      (> r 0))
    (catch Exception e
      (let [err (e/error {:potamic/err-type :potamic/internal-err
                          :potamic/err-fn 'potamic.db/key-exists?
                          :potamic/err-fatal? false
                          :potamic/err-msg (.getMessage e)
                          :potamic/err-data {:args {:k k
                                                    :conn (dissoc conn :pool)}
                                             :err (Throwable->map e)}})]
        (e/throw-potamic-error err)))))
