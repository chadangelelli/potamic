(ns potamic.sentinel
  "Provides time-oriented \"watchers\" for Potamic queues."
  {:added "0.1"
   :author "Chad Angelelli"}
  (:require [clojure.core.async :as async]
            [potamic.db]
            [potamic.errors :as e]
            [potamic.fmt :refer [GREEN NC]]
            [potamic.queue :as p]
            [potamic.sentinel.validation :as sv]
            [potamic.util :as pu]
            [potamic.validation :as v]
            [taoensso.carmine :as car :refer [wcar]]
            [taoensso.timbre :as log]))

(def ^{:private true} STQ-LABEL (str "[" GREEN "potamic.sentinel" NC "]"))

(defprotocol SentinelProtocol
  "Protocol for Sentinel."

  (start-sentinel!
    [this]
    "Starts a `Sentinel`. Returns modified Sentinal (\"this \")
    after instantiation.

    This method has side effects:

    1. It will create the queue (stream) if it doesn't exist.
    2. It logs a message, stating it has started.
    3. It mutates its own internal `:state` atom.
    4. It launches a go loop (via core.async thread pool)
    5. It can only be stopped by `potamic.sentinel/stop-sentinel!` or crashing.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/start-sentinel! s)
    ;= 2023-06-21T19:01:45.533Z m INFO [potamic.sentinel:252] \\
    ;=   - [potamic.sentinel] Started Sentinel for  my/queue
    ;= #potamic.sentinel.Sentinel {:queue-conn [..]
    ;=                     :queue-name my/queue
    ;=                     :queue-group my/group
    ;=                     [..]}
    ;= RUN: 1
    ;= RUN: 2
    ;= RUN: 3
    ;= [...]
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/stop-sentinel!`")

  (stop-sentinel!
    [this]
    "Stops Sentinel.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/start-sentinel! s)
    ;= 2023-06-21T19:01:45.533Z m INFO [potamic.sentinel:252] \\
    ;=   - [potamic.sentinel] Started Sentinel for  my/queue
    ;= #potamic.sentinel.Sentinel {:queue-conn [..]
    ;=                     :queue-name my/queue
    ;=                     :queue-group my/group
    ;=                     [..]}
    ;= RUN: 1
    ;= RUN: 2
    ;= RUN: 3
    ;= [...]

    (q/stop-sentinel! s)
    ;= true
    ;= 2023-06-21T19:01:58.466Z m INFO [potamic.sentinel:262] \\
    ;=   - [potamic.sentinel] Stopped Sentinel for  my/queue
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`")

  (get-sentinel-init-id
    [this]
    "Returns init-id for Sentinel.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-init-id s)
    ;= 0
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-frequency
    [this]
    "Returns frequency for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-frequency s)
    ;= 2000
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-start-offset
    [this]
    "Returns start offset for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-start-offset s)
    ;= 0
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-queue-conn
    [this]
    "Returns queue connection for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-queue-conn s)
    ;= {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=  :pool
    ;=  #taoensso.carmine.connections.ConnectionPool
    ;=  {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool[..]
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-queue-name
    [this]
    "Returns queue name for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-queue-name s)
    ;= my/queue
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-queue-group
    [this]
    "Returns queue group for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-queue-group s)
    ;= my/group
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-handler
    [this]
    "Returns handler for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-handler s)
    ;= #function[user/fn--37246]
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-signal-channel
    [this]
    "Returns signal channel for Sentinel.

    _NOTE_: This value is immutable.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-signal-channel s)
    ;= #object[clojure.core.async.impl.channels.ManyToManyChannel
    ;=         0x44973096
    ;=         \"clojure.core.async.impl.channels.ManyToManyChannel@44973096\"]
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-state
    [this]
    "Returns all state for Sentinel.

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-state s)
    ;= {:started? false, :stopped? false, :n-runs 0}

    (do (q/start-sentinel! s) nil)
    ;= 2023-06-30T17:06:26.341Z m INFO [potamic.sentinel:618] - \\
    ;= [potamic.sentinel] Started Sentinel for  my/queue
    ;= nil
    ;= RUN: 1
    ;= RUN: 2
    ;= RUN: 3

    (q/get-sentinel-state s)
    ;= {:started? true, :stopped? false, :n-runs 4}

    (q/stop-sentinel! s)
    ;= true
    ;= 2023-06-30T17:06:39.462Z m INFO [potamic.sentinel:628] - \\
    ;= [potamic.sentinel] Stopped Sentinel for  my/queue

    (q/get-sentinel-state s)
    ;= {:started? false, :stopped? true, :n-runs 7}
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (get-sentinel-state-attr
    [this attr]
    "Returns specific state attr for Sentinel.

    **State:**

    | Attr        | Type    | Description                                    |
    | ----------- | ------- | ---------------------------------------------- |
    | `:started?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
    | `:stopped?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
    | `:n-runs`   | int     | increments for every interation at `frequency` |

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/get-sentinel-state-attr s :started?)
    ;= false

    (do (q/start-sentinel! s) nil)
    ;= 2023-06-30T17:06:26.341Z m INFO [potamic.sentinel:618] - \\
    ;= [potamic.sentinel] Started Sentinel for  my/queue
    ;= nil
    ;= RUN: 1
    ;= RUN: 2
    ;= RUN: 3

    (q/get-sentinel-state-attr s :started?)
    ;= true
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/set-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`")

  (set-sentinel-state-attr
    [this k v]
    "Sets k to v in state atom. Returns Sentinel. This is primarily intended
    to be used to track encapsulated user state through a Sentinel's lifetime.

    **Built-in State:**

    | Attr        | Type    | Description                                    |
    | ----------- | ------- | ---------------------------------------------- |
    | `:started?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
    | `:stopped?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
    | `:n-runs`   | int     | increments for every interation at `frequency` |

    Examples:

    ```clojure
    (require '[potamic.sentinel :as q])

    (def s (q/create-sentinel
             {:queue-uri \"redis://default:secret@localhost:6379/0\"
              :queue-name 'my/queue
              :queue-group 'my/group
              :frequency 2000
              :handler (fn [this]
                         (println \"RUN:\"
                           (q/get-sentinel-state-attr this :n-runs)))}))
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn
    ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=   :pool #taoensso.carmine.connections.ConnectionPool
    ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--33742]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0} 0x217abcc9]}

    (q/set-sentinel-state-attr s :process-count 52)
    ;= #potamic.sentinel.Sentinel
    ;= {:queue-conn {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
    ;=               :pool #taoensso.carmine.connections.ConnectionPool[..]}
    ;=  :queue-name my/queue
    ;=  :queue-group my/group
    ;=  :init-id 0
    ;=  :frequency 2000
    ;=  :start-offset 0
    ;=  :handler #function[user/fn--37246]
    ;=  :signal-chan
    ;=  #object[clojure.core.async.impl.channels.ManyToManyChannel[..]
    ;=  :state #atom[{:started? false
    ;=                :stopped? false
    ;=                :n-runs 0
    ;=                :process-count 52}
    ;=               0x2a042d0a]}

    (q/get-sentinel-state-attr s :process-count)
    ;= 52
    ```

    See also:

    - `potamic.sentinel/create-sentinel`
    - `potamic.sentinel/get-sentinel-init-id`
    - `potamic.sentinel/get-sentinel-frequency`
    - `potamic.sentinel/get-sentinel-start-offset`
    - `potamic.sentinel/get-sentinel-queue-conn`
    - `potamic.sentinel/get-sentinel-queue-name`
    - `potamic.sentinel/get-sentinel-queue-group`
    - `potamic.sentinel/get-sentinel-handler`
    - `potamic.sentinel/get-sentinel-signal-channel`
    - `potamic.sentinel/get-sentinel-state`
    - `potamic.sentinel/get-sentinel-state-attr`
    - `potamic.sentinel/start-sentinel!`
    - `potamic.sentinel/stop-sentinel!`"))

(defrecord Sentinel [queue-conn
                     queue-name
                     queue-group
                     init-id
                     frequency
                     start-offset
                     handler
                     signal-chan
                     state]
  SentinelProtocol
  (get-sentinel-init-id         [_]        init-id)
  (get-sentinel-frequency       [_]        frequency)
  (get-sentinel-start-offset    [_]        start-offset)
  (get-sentinel-queue-conn      [_]        queue-conn)
  (get-sentinel-queue-name      [_]        queue-name)
  (get-sentinel-queue-group     [_]        queue-group)
  (get-sentinel-handler         [_]        handler)
  (get-sentinel-signal-channel  [_]        signal-chan)
  (get-sentinel-state           [_]        @state)
  (get-sentinel-state-attr      [_ x]      (get @state x))
  (set-sentinel-state-attr      [this k v] (swap! state assoc k v) this)

  (start-sentinel! [this]
    (let [queue-exists? (wcar queue-conn (car/exists (pu/->str queue-name)))
          [_ ?create-err] (when-not queue-exists?
                            (p/create-queue queue-name
                                            queue-conn
                                            :group queue-group
                                            :init-id init-id))]
      (if ?create-err
        (let [err (e/error
                    {:potamic/err-type :potamic/internal-err
                     :potamic/err-msg (str "Could not create queue for "
                                         "Sentinel '" queue-name "'.")
                     :potamic/err-data {:this this :err ?create-err}})]
          (e/throw-potamic-error err))
        (do
          (when (pos-int? start-offset)
            (async/<!! (async/timeout start-offset)))
          (set-sentinel-state-attr this :started? true)
          (set-sentinel-state-attr this :stopped? false)
          (log/info STQ-LABEL "Started Sentinel for " queue-name)
          (async/go-loop
            [this this, n-runs 0]
            (let [n-runs (inc n-runs)
                  this (set-sentinel-state-attr this :n-runs n-runs)
                  [?signal _] (async/alts! [signal-chan
                                            (async/timeout frequency)])]
              (if (= ?signal :stop)
                (do (set-sentinel-state-attr this :started? false)
                    (set-sentinel-state-attr this :stopped? true)
                    (log/info STQ-LABEL "Stopped Sentinel for " queue-name)
                    nil)
                (do
                  (handler this)
                  (recur this n-runs)))))
          this))))

  (stop-sentinel! [this]
    (async/>!! (get-sentinel-signal-channel this) :stop)))

(defn create-sentinel
  "Returns a new `Sentinel` record. Once created, use the Sentinel's methods
  to retrieve config and to manage state.

  **Configuration Options:**

  | Option         | Description             | Required | Default |
  | -------------- | ----------------------- | -------- | ------- |
  | `queue-uri`    | Redis URI               | &check;  | none    |
  | `queue-name`   | key name                | &check;  | none    |
  | `queue-group`  | Consumer Group          | &check;  | none    |
  | `init-id`      | starting stream ID      |          | 0       |
  | `frequency`    | (ns) interval to run at | &check;  | none    |
  | `start-offset` | ms to wait to start     |          | 0       |
  | `handler`      | 1-artity fn of `this`   | &check;  | none    |

  **State:**

  | Attr        | Type    | Description                                    |
  | ----------- | ------- | ---------------------------------------------- |
  | `:started?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
  | `:stopped?` | boolean | set on `q/start-sentinel!`/`q/stop-sentinel!`  |
  | `:n-runs`   | int     | increments for every interation at `frequency` |


  Examples:

  ```clojure
  (require '[potamic.sentinel :as q])

  (def s (q/create-sentinel
           {:queue-uri \"redis://default:secret@localhost:6379/0\"
            :queue-name 'my/queue
            :queue-group 'my/group
            :frequency 2000
            :handler
            (fn [this]
              (println \"RUN:\" (q/get-sentinel-state-attr this :n-runs)))}))
  ;= #potamic.sentinel.Sentinel
  ;= {:queue-conn
  ;=  {:spec {:uri \"redis://default:secret@localhost:6379/0\"}
  ;=   :pool #taoensso.carmine.connections.ConnectionPool
  ;=   {:pool #object[org.apache.commons.pool2.impl.GenericKeyedObjectPool]}}
  ;=  :queue-name my/queue
  ;=  :queue-group my/group
  ;=  :init-id 0
  ;=  :frequency 2000
  ;=  :start-offset 0
  ;=  :handler #function[user/fn--33742]
  ;=  :signal-chan #object[clojure.core.async.impl.channels.ManyToManyChannel]
  ;=  :state #atom[{:started? false
  ;=                :stopped? false
  ;=                :n-runs 0} 0x217abcc9]}
  ```

  See also:

  - `potamic.sentinel/create-sentinel`
  - `potamic.sentinel/SentinelProtocol`
  - `potamic.sentinel/get-sentinel-init-id`
  - `potamic.sentinel/get-sentinel-frequency`
  - `potamic.sentinel/get-sentinel-start-offset`
  - `potamic.sentinel/get-sentinel-queue-conn`
  - `potamic.sentinel/get-sentinel-queue-name`
  - `potamic.sentinel/get-sentinel-queue-group`
  - `potamic.sentinel/get-sentinel-handler`
  - `potamic.sentinel/get-sentinel-signal-channel`
  - `potamic.sentinel/get-sentinel-state`
  - `potamic.sentinel/get-sentinel-state-attr`
  - `potamic.sentinel/set-sentinel-state-attr`
  - `potamic.sentinel/start-sentinel!`
  - `potamic.sentinel/stop-sentinel!`"
  [{:keys [queue-uri
           queue-name
           queue-group
           init-id
           frequency
           start-offset
           handler]
    :or {start-offset 0 init-id 0}}]
  (let [queue-conn (potamic.db/make-conn :uri queue-uri)
        args {:queue-conn queue-conn
              :queue-name queue-name
              :queue-group queue-group
              :init-id init-id
              :frequency frequency
              :start-offset start-offset
              :handler handler}]
    (if-let [args-err (v/invalidate sv/Valid-Create-Sentinel-Args
                                    args)]
      (let [err (e/error
                  {:potamic/err-type :potamic/args-err
                   :potamic/err-msg (str "Invalid args provided to "
                                         "potamic.sentinel/create-sentinel.")
                   :potamic/err-data {:args args
                                :err args-err}})]
        (e/throw-potamic-error err))
      (let [state (atom {:started? false :stopped? false :n-runs 0})
            signal-chan (async/chan)]
        (map->Sentinel (assoc args
                              :state state
                              :signal-chan signal-chan))))))
