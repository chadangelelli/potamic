(ns potamic.util
  "Common utilities."
  (:require [com.rpl.specter :as s]))

(defn ->str
  "Returns a string representation of symbol. This is similar to calling `str`
  on a symbol except that keywords will not contain a preceding colon
  character. The keyword `:x/y` will yield \"x/y\" instead of \":x/y\".

  **Examples:**

  ```clojure
  (require '[potamic.util :as util])

  ;; all of the following are identical
  (util/->str :my/queue)
  (util/->str 'my/queue)
  (util/->str \"my/queue\")
  ;= \"my/queue\"
  ```

  See also:

  - `potamic.util/<-str`"
  [x]
  (cond
    (string? x) x
    (keyword? x) (subs (str x) 1)
    :else (str x)))

(defn <-str
  "Returns keyword for `x` if `x` is a string and doesn't start with a number,
  else returns `x` as-is.

  _NOTE_: Redis Stream IDs won't be coerced to keywords.

  **Examples:**

  ```clojure
  ```

  See also:

  - `potamic.util/->str`"
  [x]
  (if-not (string? x)
    x
    (if (Character/isDigit (first x))
      x
      (keyword x))))

(defn ->int
  "Returns string `s` parsed to integer.

  **Examples:**
  ```clojure
  ```

  See also:
  "
  [s]
  (Integer/parseInt s))

(defn prep-cmd
  "Returns concatenated vector of arguments (without using concat) to be
 applied to a Carmine/Redis command.

  **Examples:**

  ```clojure
  ```

  See also:
  "
  [args]
  (->> args
       (flatten)
       (remove nil?)
       (s/transform [s/ALL #(or (symbol? %) (keyword? %))] ->str)))
