(ns potamic.errors
  (:require [potamic.errors.validation :as pv]
            [potamic.validation :as v])
  (:gen-class))

(defmacro error
  "Returns Potamic Error. Required keys are `:potamic/err-type` (one of
  `potamic.errors.types/error-types`), `:potamic/err-fatal?` (boolean), and
  `:potamic/err-msg` (string). Throws error if required keys not provided.
  Additional keys are allowed. A common paradigm in Potamic is to add
  `:potamic/err-data` (hash).

  Examples:

  ```clojure
  (require '[potamic.errors :as e])

  (e/error {:potamic/err-type :potamic/args-err
            :potamic/err-fatal? false
            :potamic/err-msg (str \"err \" \"test \" \"body \" \"processing\")
            :potamic/err-data {:x 1 :y 2}})
  ;= {:potamic/err-column 12,
  ;=  :potamic/err-fatal? false,
  ;=  :potamic/err-file \"potamic/errors_test.clj\",
  ;=  :potamic/err-line 14,
  ;=  :potamic/err-msg \"err test body processing\",
  ;=  :potamic/err-type :potamic/args-err,
  ;=  :potamic/err-data {:x 1, :y 2}}
  ```

  See also:

  - `potamic.errors/throw-potamic-error`
  - `potamic.errors.types/error-types`
  - `potamic.errors.validation/Valid-Error`"
  [m]
  (let [{:keys [line column]} (meta &form)
        file *file*]
    `(let [?err# (v/invalidate pv/Valid-Error ~m)]
       (when ?err#
         (throw (Exception. (format "%s (at %s:%s:%s)"
                                    ?err#
                                    ~file
                                    ~line
                                    ~column))))
       (assoc ~m
              :potamic/err-file ~file
              :potamic/err-line ~line
              :potamic/err-column ~column))))

(defmacro throw-potamic-error
  "Throws error after stringifying `error` argument.

  Examples:

  ```clojure
  (require '[potamic.errors :as e])

  (def err (e/error {:potamic/err-type :potamic/internal-err
                     :potamic/err-fatal? true
                     :potamic/err-msg (str \"internal \" \"error\")}))
  ;= #:tec{:err-type :potamic/internal-err
  ;=       :err-fatal? true
  ;=       :err-msg \"internal error\"
  ;=       :err-file \"NO_SOURCE_PATH\"
  ;=       :err-line 1
  ;=       :err-column 10}

  (e/throw-tec-error err)
  ;= Execution error at user/eval13399 (REPL:1).
  ;= #:tec{:err-type :potamic/internal-err
  ;=       :err-fatal? true
  ;=       :err-msg \"internal error\"
  ;=       :err-file \"NO_SOURCE_PATH\"
  ;=       :err-line 1
  ;=       :err-column 10}
  ```

  See also:

  - `potamic.errors/error`
  - `potamic.errors.types/error-types`
  - `potamic.errors.validation/Valid-Error`"
  [error]
  `(throw (Exception. (str ~error))))
