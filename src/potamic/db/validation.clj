(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]))

(def Valid-Conn
  (malli/schema
    [:map {:closed true}
     [:uri (v/f #(re-find v/re-redis-uri %) "Invalid Redis URI")]
     [:pool {:optional true} map?]]))

(def Valid-Make-Conn-Args
  Valid-Conn)
