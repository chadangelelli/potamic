(ns potamic.db
  "Redis DB functionality.")

;;TODO: add validation
;;TODO: on validation: check if connection is reachable (fail early!)
;;TODO: on validation: set failed conn check to fatal
(defn make-conn
  "Returns a Redis connection."
  [{:keys [uri pool]}]
  {:uri uri
   :pool (or pool {})})
