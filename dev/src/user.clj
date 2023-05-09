(ns user
  "nREPL config"
  (:require [clojure.tools.namespace.repl :as ns-repl]
            [clojure.repl :refer [dir doc]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; format

(def LOGO
"
 ___     _              _
| _ \\___| |_ __ _ _ __ (_)__
|  _/ _ \\  _/ _` | '  \\| / _|
|_| \\___/\\__\\__,_|_|_|_|_\\__|
")

(def  BOLD   "\033[1m")
(def  ITAL   "\033[3m")
(def  BLUE   "\033[0;34m")
(def  RED    "\033[0;31m")
(def  GREEN  "\033[0;32m")
(def  ORANGE "\033[0;33m")
(def  PURPLE "\033[0;35m")
(def  CYAN   "\033[0;36m")
(def  NC     "\033[0m")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; file watcher

(apply ns-repl/set-refresh-dirs ["src"])

(defn r [] (ns-repl/refresh))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; format

(defn logo [] (println BLUE LOGO NC))

(defn print-init-msg
  []
  (println (format "REPL started at %slocalhost%s:%s16002%s" BLUE NC GREEN NC)))
