(ns potamic.queue.validation
  (:require [malli.core :as malli]
            [potamic.db.validation :as dbv]))

(def Valid-Create-Queue-Opts
  (malli/schema
    [:map {:closed true}
     [:queue-name [:or keyword? symbol? string?]]
     [:conn dbv/Valid-Conn]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]
     [:init-id {:optional true} [:or int? string?]]]))

