(ns vermilionsands.chromatic
  (:import [com.hazelcast.core IAtomicReference HazelcastInstance ITopic MessageListener]
           [clojure.lang IDeref IAtom IRef IReference IMeta]))

(defn- validate [f x]
  (try
    (when (and f (false? (f x)))
      (throw (IllegalStateException. "Invalid reference state!")))
    (catch RuntimeException e
      (throw e))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state!" e)))))

(defn- notify [hazelcast-atom old-val new-val]
  (.publish ^ITopic (.-notification_topic hazelcast-atom) [old-val new-val]))

(defmacro ^:private swap* [this f & args]
  (let [[_ _ rest] args
        f (if rest (list 'apply f) (list f))]
    `(let [old-val# (deref ~this)
           new-val# (~@f old-val# ~@args)]
       (validate ~'local-validator new-val#)
       (if (.compareAndSet ~'state old-val# new-val#)
         (do
           (notify ~this old-val# new-val#)
           new-val#)
         (recur ~f ~@args)))))

(deftype ValueBasedHazelcastAtom
  [^IAtomicReference state
   ^ITopic notification-topic
   ^:unsynchronized-mutable local-meta-map
   ^:unsynchronized-mutable local-validator
   ^:volatile-mutable local-watches]
  IAtom
  (swap [this f]
    (swap* this f))

  (swap [this f x]
    (swap* this f x))

  (swap [this f x y]
    (swap* this f x y))

  (swap [this f x y args]
    (swap* this f x y args))

  (compareAndSet [this old-val new-val]
    (validate local-validator new-val)
    (let [ret (.compareAndSet state old-val new-val)]
      (when ret
        (notify this old-val new-val))
      ret))

  (reset [this new-val]
    (let [old-val (deref this)]
      (validate local-validator new-val)
      @(.setAsync state new-val)
      (notify this old-val new-val)
      new-val))

  IMeta
  (meta [this]
    (locking this
      local-meta-map))

  IReference
  (resetMeta [this m]
    (locking this
      (set! local-meta-map m)))

  (alterMeta [this f args]
    (locking this
      (set! local-meta-map (apply f local-meta-map args))))

  IRef
  (setValidator [this f]
    (validate f (deref this))
    (set! local-validator f))

  (getValidator [_] local-validator)

  (addWatch [this k f]
    (locking this
      (set! local-watches (assoc local-watches k f))
      this))

  (removeWatch [this k]
    (locking this
      (set! local-watches (dissoc local-watches k))
      this))

  (getWatches [_] local-watches)

  IDeref
  (deref [_] (.get state)))

(defn distributed-atom [^HazelcastInstance instance id x]
  (let [state (.getAtomicReference instance id)
        notification-topic (.getReliableTopic instance id)
        atom (->ValueBasedHazelcastAtom state notification-topic nil nil {})]
    (.addMessageListener notification-topic
      (reify MessageListener
        (onMessage [_ message]
          (let [[old-val new-val] (.getMessageObject message)]
            (doseq [[k f] (.getWatches atom)]
              (when f
                (f k atom old-val new-val)))))))
    (when-not @atom
      (reset! atom x))
    atom))