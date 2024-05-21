(ns potamic.db
  "Redis DB functionality."
  (:require [potamic.errors :as e]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]
            [taoensso.carmine :as car])
  (:gen-class))

(defn make-conn
  "Creates a connection for Redis. Returns `conn` or throws Potamic Error.
  On success, `conn` will be usable by `potamic.queue` and the underlying
  `taoensso.carmine/wcar` library.

  **Examples:**

  ```clojure
  (require '[potamic.db :as db])

  (db/make-conn :uri \"redis://localhost:6379/0\")
  ;= {:spec
  ;=  {:uri \"redis://localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool[..]}
  ```"
  [& opts]
  (let [{:keys [uri pool] :as args} (apply hash-map opts)]
    (if-let [args-err (v/invalidate dbv/Valid-Make-Conn-Args args)]
      (let [err (e/error {:potamic/err-type :potamic/args-err
                          :potamic/err-fatal? true
                          :potamic/err-msg (str "Invalid args provided to "
                                                "potamic.db/make-conn")
                          :potamic/err-data {:args args :err args-err}})]
        (e/throw-potamic-error err))
      {:spec {:uri uri}
       :pool (or pool (car/connection-pool {}))})))
