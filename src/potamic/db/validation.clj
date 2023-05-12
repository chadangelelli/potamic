(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]))

(def Valid-Conn
  (malli/schema
    [:map {:closed true}
     [:spec [:map {:closed true}
             [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]]]
     [:pool {:optional true} map?]]))

(def Valid-Make-Conn-Args
  [:map {:closed true}
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
   [:pool {:optional true} map?]])
