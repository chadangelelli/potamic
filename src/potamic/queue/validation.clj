(ns potamic.queue.validation
  (:require [malli.core :as malli]
            [potamic.db.validation :as dbv]))

(def Valid-Create-Queue-Args
  (malli/schema
    [:map {:closed true}
     [:queue-name [:or keyword? symbol? string?]]
     [:conn dbv/Valid-Conn]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]
     [:init-id {:optional true} [:or int? string?]]]))


(def Valid-Destroy-Queue-Args
  (malli/schema
    [:map {:closed true}
     [:queue-name [:or keyword? symbol? string?]]
     [:conn dbv/Valid-Conn]
     [:unsafe {:optional true} boolean?]]))

(def Valid-Read-Pending-Args
  (malli/schema
    [:map {:closed true}
     [:count int?]
     [:from [:or keyword? symbol? string?]]
     [:for {:optional true} [:or keyword? symbol? string?]]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:end {:optional true} [:or keyword? symbol? string?]]]))
