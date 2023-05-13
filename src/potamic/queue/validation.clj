(ns potamic.queue.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]
            [potamic.queue.queues :as queues]))

(def queue-exists? (v/f (fn [x] (get @queues/queues_ x)) "Unknown queue"))

(def valid-queue-value? [:or keyword? symbol? string?])

(def Valid-Create-Queue-Args
  (malli/schema
    [:map {:closed true}
     [:queue-name valid-queue-value?]
     [:conn dbv/Valid-Conn]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]
     [:init-id {:optional true} [:or int? string?]]]))

(def Valid-Destroy-Queue-Args
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and valid-queue-value? queue-exists?]]
     [:conn dbv/Valid-Conn]
     [:unsafe {:optional true} boolean?]]))

(def Valid-Read-Pending-Args
  (malli/schema
    [:map {:closed true}
     [:count int?]
     [:from [:and valid-queue-value? queue-exists?]]
     [:for {:optional true} [:or keyword? symbol? string?]]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:end {:optional true} [:or keyword? symbol? string?]]]))

(def Valid-Read-Range-Args
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and valid-queue-value? queue-exists?]]
     [:count {:optional true} [:or int? nil?]]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:end {:optional true} [:or keyword? symbol? string?]]]))
