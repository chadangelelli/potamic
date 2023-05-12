(ns potamic.errors)

(def error-types
  #{:potamic/args-err
    :potamic/db-err
    :potamic/internal-err})

(defmacro error
  [{err-type :potamic/err-type :as m}]
  (let [{:keys [line column]} (meta &form)
        file *file*]
    (when-not (some error-types [err-type])
      (throw (Exception. (str "Invalid error type provided: " err-type
                              " (at " file " [" line ":" column "])"))))
    (assoc m
           :potamic/err-file file
           :potamic/err-line line
           :potamic/err-column column)))

(defmacro throw-potamic-error
  [error]
  `(throw (Exception. (str ~error))))
