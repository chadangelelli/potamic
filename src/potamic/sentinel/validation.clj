(ns potamic.sentinel.validation
  "Validation for `st.queue` library."
  {:added "5.0"
   :author "Chad Angelelli"}
  (:require [malli.core :as malli]
            [potamic.queue.validation :as queue-val]
            [potamic.db.validation :as db-val]
            [potamic.validation :as v]))

(def Valid-Create-Sentinel-Args
  (malli/schema
    [:map {:closed true}
     [:queue-conn db-val/Valid-Conn]
     [:queue-name queue-val/valid-queue-value?]
     [:queue-group queue-val/valid-queue-value?]
     [:frequency int?]
     [:handler (v/f fn? "Handler must be a function")]
     [:init-id {:optional true} int?]
     [:start-offset {:optional true} int?]]))

