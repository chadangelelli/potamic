(ns potamic.util
  "Common utilities."
  (:require [com.rpl.specter :as s]
            [taoensso.nippy :as nippy])
  (:gen-class))

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
  "Returns concatenated vector of arguments to be applied to a Carmine command.

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

(defn remove-conn
  [x]
  (if (:conn x)
    (dissoc x :conn)
    x))

(defn uuid
  []
  (java.util.UUID/randomUUID))

(defn make-exception
  [e]
  (let [e* (Throwable->map e)]
    (update e* :trace #(subvec % 0 10))))

(def RECURSIVE-WALK
  "_(Implemtation detail)_

  Compiled path for nested data structures."
  (s/comp-paths
    (s/recursive-path []
                      #_:clj-kondo/ignore p*
                      (s/cond-path
                        map? (s/continue-then-stay [s/MAP-VALS p*])
                        sequential? [s/ALL p*]
                        s/STAY))))

(defn encode-map-vals
  "Returns map after encoding map vals by calling `nippy/freeze` on them.
  Nested maps are encoded wholesale as Redis/KeyDB streams don't allow nesting.

  Examples:

  ```clojure
  (require '[potamic.util :as util])

  (def m {:a 111 :b {:c \"333\" :d :my/namespaced-key}})

  (util/encode-map-vals m)
  ;= {:a #object[\"[B\" 0x7225a307 \"[B@7225a307\"]
  ;=  :b #object[\"[B\" 0x141cc645 \"[B@141cc645\"]}
  ```"
  [m]
  (into {} (for [[k v] m] [k (nippy/freeze v)])))
