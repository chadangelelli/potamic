(ns potamic.util
  "Common utilities.")

(defn ->str
  "Returns a string representation of symbol. This is similar to calling `str`
  on a symbol except that keywords will not contain a preceding colon
  character. The keyword `:x/y` will yield \"x/y\" instead of \":x/y\".

  **Examples:**

  ```clojure
  (require '[potamic.queue :as q])

  (q/->str :my/queue \"my/queue\")
  ;= \"my/queue\"

  (q/->str 'my/queue \"my/queue\")
  ;= \"my/queue\"

  (q/->str \"my/queue\" \"my/queue\")
  ;= \"my/queue\"
  ```

  See also:
  "
  [x]
  (cond
    (string? x) x
    (keyword? x) (subs (str x) 1)
    :else (str x)))
