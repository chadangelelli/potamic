(ns potamic.util
  "Common utilities.")

(defn ->str
  "Returns a string representation of symbol. This is similar to calling `str`
  on a symbol except that keywords will not contain a preceding colon
  character. The keyword `:x/y` will yield \"x/y\" instead of \":x/y\".

  **Examples:**

  ```clojure
  (require '[potamic.queue :as q])

  ;; all of the following are identical
  (q/->str :my/queue)
  (q/->str 'my/queue)
  (q/->str \"my/queue\")
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

;;TODO: write input validation
(defn time->milliseconds
  "Converts high-level time syntax (e.g. `[5 :seconds]`) to milliseconds.
  If `t` is an integer, or if `:milli`/`millis` is provided as `interval`,
  it is returned as-is. Returns an integer for milliseconds.

  | Intervals            |
  | -------------------- |
  | `:milli`/`:millis`   |
  | `:second`/`:seconds` |
  | `:minute`/`:minutes` |
  | `:hour`/`:hours`     |

  **Examples:**

  ```clojure
  (util/time->milliseconds [5 :seconds])
  ;= 5000

  (util/time->milliseconds [1 :second])
  ;= 1000

  (util/time->milliseconds [5 :hours])
  ;= 18000000

  (util/time->milliseconds [1234 :millis])
  ;= 1234

  (util/time->milliseconds 1234))
  ;= 1234
  ```"
  [t]
  (if (vector? t)
    (let [[n interval] t]
      (case interval
        (:milli :millis) n
        (:second :seconds) (* n 1000)
        (:minute :minutes) (* n 1000 60)
        (:hour :hours) (* n 1000 60 60)))
    t))
