(ns vermilionsands.chromatic
  (:import [com.hazelcast.core IAtomicReference HazelcastInstance ITopic MessageListener IFunction]
           [clojure.lang IDeref IAtom IRef IReference IMeta]))

(deftype HazelcastFn [f]
  IFunction
  (apply [_ x] (f x)))

(defprotocol DistributedAtom
  (set-shared-validator [this f])
  (get-shared-validator [this])
  (add-shared-watch [this k f])
  (remove-shared-watch [this k])
  (get-shared-watches [this]))

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
       (validate (:validator @~'local-ctx) new-val#)
       (validate (:validator (.get ~'shared-ctx)) new-val#)
       (if (.compareAndSet ~'state old-val# new-val#)
         (do
           (notify ~this old-val# new-val#)
           new-val#)
         (recur ~f ~@args)))))

(defn- assoc-validator [f m]
  (assoc m :validator f))

(defn- assoc-shared-watch [k f m]
  (update-in m [:watches k] f))

(defn- remove-shared-watch [k m]
  (update m :watches dissoc k))

(deftype ValueBasedHazelcastAtom [state notification-topic shared-ctx local-ctx]
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
    (validate (:validator @local-ctx) new-val)
    (validate (:validator (.get shared-ctx)) new-val)
    (let [ret (.compareAndSet state old-val new-val)]
      (when ret
        (notify this old-val new-val))
      ret))

  (reset [this new-val]
    (let [old-val (deref this)]
      (validate (:validator @local-ctx) new-val)
      (validate (:validator (.get shared-ctx)) new-val)
      @(.setAsync state new-val)
      (notify this old-val new-val)
      new-val))

  IMeta
  (meta [_]
    (:meta @local-ctx))

  IReference
  (resetMeta [_ m]
    (swap! local-ctx assoc :meta m)
    m)

  (alterMeta [_ f args]
    (let [g #(apply f % args)]
      (:meta (swap! local-ctx update :meta g))))

  IRef
  (setValidator [this f]
    (validate f (deref this))
    (swap! local-ctx assoc :validator f)
    nil)

  (getValidator [_]
    (:validator @local-ctx))

  (addWatch [this k f]
    (swap! local-ctx update :watches conj k f)
    this)

  (removeWatch [this k]
    (swap! local-ctx update :watches dissoc k)
    this)

  (getWatches [_]
    (:wathes @local-ctx))

  IDeref
  (deref [_] (.get state))

  DistributedAtom
  (set-shared-validator [this f]
    (validate f (deref this))
    (.alter ^IAtomicReference shared-ctx (->HazelcastFn (partial assoc-validator f))))

  (get-shared-validator [_]
    (:validator (.get shared-ctx)))

  (add-shared-watch [this k f]
    (.alter ^IAtomicReference shared-ctx (->HazelcastFn (partial assoc-shared-watch k f)))
    this)

  (remove-shared-watch [this k]
    (.alter ^IAtomicReference shared-ctx (->HazelcastFn (partial remove-shared-watch k)))
    this)

  (get-shared-watches [_]
    (:watches (.get shared-ctx))))

(defn distributed-atom
  ""
  [^HazelcastInstance instance id x]
  (let [state (.getAtomicReference instance id)
        shared-ctx (.getAtomicReference instance (str id "-ctx"))
        local-ctx (atom {})
        notification-topic (.getReliableTopic instance id)
        atom (->ValueBasedHazelcastAtom state notification-topic shared-ctx local-ctx)]
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