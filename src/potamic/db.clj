(ns potamic.db
  "Redis DB functionality."
  (:require [potamic.errors :as e]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]))

(defn make-conn
  "Returns vector of `[?conn ?err]`.

  **Examples:**

  ```clojure
  ```

  See also:
  "
  [{:keys [uri pool] :as args}]
  (if-let [args-err (v/invalidate dbv/Valid-Make-Conn-Args args)]
    [nil
     (e/error {:potamic/err-type :potamic/args-err
               :potamic/err-msg (str "Invalid args provided to "
                                     "potamic.db/make-conn")
               :potamic/err-data {:args args :err args-err}})]
    [{:uri uri :pool (or pool {})}
     nil]))
