(ns potamic.errors.validation
  (:require [malli.core :as malli]
            [potamic.errors.types :as potamic]))

(def Valid-Error
  "Validates input for `potamic.errors/error` macro. Required keys are
  `:potamic/err-type` (one of `potamic.errors.types/error-types`),
  `:potamic/err-fatal?` (boolean), and `:potamic/err-msg` (string).
  Throws error if required keys not provided. Additional keys are allowed.

  See also:

  - `potamic.errors/error`
  - `potamic.errors/throw-potamic-error`
  - `potamic.errors.types/error-types`
  - `potamic.validation/invalidate`"
  (malli/schema
    [:map
     [:potamic/err-type (into [:enum] potamic/error-types)]
     [:potamic/err-fatal? boolean?]
     [:potamic/err-msg string?]]))
