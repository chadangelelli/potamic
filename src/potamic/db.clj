(ns potamic.db
  "Redis DB functionality."
  (:require [potamic.errors :as e]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]
            [taoensso.carmine :as car]))

(defn make-conn
  "Creates a connection for Redis. Returns vector of `[?conn ?err]`.
  On success, `?conn` will be usable by `taoensso.carmine/wcar`

  **Examples:**

  ```clojure
  (require '[potamic.db :as db])

  (db/make-conn {:uri \"redis://localhost:6379/0\"})
  ;= [{:spec {:uri \"redis://localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool{..}
  ;=  nil]
  ```"
  [{:keys [uri pool] :as args}]
  (if-let [args-err (v/invalidate dbv/Valid-Make-Conn-Args args)]
    [nil
     (e/error {:potamic/err-type :potamic/args-err
               :potamic/err-msg (str "Invalid args provided to "
                                     "potamic.db/make-conn")
               :potamic/err-data {:args args :err args-err}})]
    [{:spec {:uri uri}
      :pool (or pool (car/connection-pool {}))}
     nil]))
