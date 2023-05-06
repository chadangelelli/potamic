(ns repl
  (:require
    [nrepl.server :as nrepl-server]
    [cider.nrepl :refer [cider-nrepl-handler]]
    [rebel-readline.main :as rebel]
    [user :as user]))

(defn -main []
  (user/logo)
  (user/print-init-msg)
  (nrepl-server/start-server :port 16002 :handler cider-nrepl-handler)
  (rebel/-main)
  (System/exit 0))
