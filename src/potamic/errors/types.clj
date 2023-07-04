(ns potamic.errors.types)

(def error-types
  "Potamic Error types.

  Available values are:

  ```clojure
  #{:potamic/args-err
    :potamic/db-err
    :potamic/internal-err}
  ```

  See also:

  - `potamicdb.errors/error`
  - `potamicdb.errors/throw-potamic-error`
  - `potamicdb.errors.validation/Valid-Error`"
  #{:potamic/args-err
    :potamic/db-err
    :potamic/internal-err})

