(ns vermilionsands.chromatic
  (:import [clojure.lang IAtom IDeref IMeta IRef IReference]
           [com.hazelcast.config Config]
           [com.hazelcast.core HazelcastInstance IAtomicReference IFunction ITopic MessageListener]
           [com.hazelcast.topic TopicOverloadPolicy]
           [java.io Closeable]))

(def ^:private reference-service-name  "hz:impl:atomicReferenceService")

(deftype HazelcastFn [f]
  IFunction
  (apply [_ x] (f x)))

(defn- hazelcast-fn
  ([f]
   (->HazelcastFn f))
  ([f x & args]
   (->HazelcastFn (apply partial f x args))))

(defprotocol DistributedAtom
  (set-shared-validator! [this f]
    "Like clojure.core/set-validator! but sets a validator that would be shared among all instances.
    Validator function has to be available on all instances using this atom.")

  (get-shared-validator [this]
    "Returns shared validator for this atom.")

  (add-shared-watch [this k f]
    "Like clojure.core/add-watch but the watch would be shared amon all instances, and would be executed on
    each instane upon notification.
    Watch function should has to be avaialable on all instances using this atom.")

  (remove-shared-watch [this k]
    "Removes shared watch under key k.")

  (get-shared-watches [this]
    "Returns shared watches for this atom."))

(defn- validate
  "Executes f on x and throws an exception if result is false, or rethrows an exception.
  Otherwise returns nil."
  [f x]
  (try
    (when (and f (false? (f x)))
      (throw (IllegalStateException. "Invalid reference state!")))
    (catch RuntimeException e
      (throw e))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state!" e)))))

(defn- notify [hazelcast-atom old-val new-val]
  (if-let [topic (.-notification_topic hazelcast-atom)]
    (.publish ^ITopic topic [old-val new-val])
    (doseq [[k f] (concat (.getWatches hazelcast-atom) (get-shared-watches hazelcast-atom))]
      (when f
        (f k hazelcast-atom old-val new-val)))))

(defn- value-swap* [hazelcast-atom f args]
  (let [[x y rest] args
        old-val (deref hazelcast-atom)
        new-val (if rest
                  (apply f old-val x y rest)
                  (apply f old-val args))]
    (doseq [g [(deref (.-local_ctx hazelcast-atom)) (.get ^IAtomicReference (.-shared_ctx hazelcast-atom))]]
      (validate (:validator g) new-val))
    (if (.compareAndSet ^IAtomicReference (.-state hazelcast-atom) old-val new-val)
      (do
        (notify hazelcast-atom old-val new-val)
        new-val)
      (recur hazelcast-atom f args))))

(defn- assoc-validator [f m]
  (assoc m :validator f))

(defn- assoc-shared-watch [k f m]
  (update-in m [:watches k] (constantly f)))

(defn- dissoc-shared-watch [k m]
  (update m :watches dissoc k))

(deftype HazelcastAtom [state notification-topic shared-ctx local-ctx]
  IAtom
  (swap [this f]
    (value-swap* this f nil))

  (swap [this f x]
    (value-swap* this f [x]))

  (swap [this f x y]
    (value-swap* this f [x y]))

  (swap [this f x y args]
    (value-swap* this f [x y args]))

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
    (swap! local-ctx update :watches assoc k f)
    this)

  (removeWatch [this k]
    (swap! local-ctx update :watches dissoc k)
    this)

  (getWatches [_]
    (:watches @local-ctx))

  IDeref
  (deref [_] (.get state))

  DistributedAtom
  (set-shared-validator! [this f]
    (validate f (deref this))
    (.alter ^IAtomicReference shared-ctx (hazelcast-fn assoc-validator f)))

  (get-shared-validator [_]
    (:validator (.get shared-ctx)))

  (add-shared-watch [this k f]
    (.alter ^IAtomicReference shared-ctx (hazelcast-fn assoc-shared-watch k f))
    this)

  (remove-shared-watch [this k]
    (.alter ^IAtomicReference shared-ctx (hazelcast-fn dissoc-shared-watch k))
    this)

  (get-shared-watches [_]
    (:watches (.get shared-ctx)))

  Closeable
  (close [_]
    (when notification-topic
      (.removeMessageListener ^ITopic notification-topic (:listener @local-ctx)))))

(defn- find-reference [^HazelcastInstance instance id]
  (->> (.getDistributedObjects instance)
       (filter
           #(and (= (.getServiceName %) reference-service-name)
                 (= (.getName %) id)))
       first))

(defn- retrieve-shared-objects [instance id]
  (let [state         (.getAtomicReference instance id)
        ctx           (.getAtomicReference instance (str id "-ctx"))
        notifications (when (:notifications? (.get ctx)) (.getReliableTopic instance id))]
    {:state state :ctx ctx :notifications notifications}))

(defn- init-shared-objects [instance id init notifications?]
  (let [lock (.getLock instance id)]
    (when-not (find-reference instance id)
      (let [state         (.getAtomicReference instance id)
            ctx           (.getAtomicReference instance (str id "-ctx"))
            notifications (when notifications? (.getReliableTopic instance id))]
        (.set ^IAtomicReference state init)
        (.set ^IAtomicReference ctx {:notifications? notifications?})
        (when notifications?
          (-> (.getReliableTopicConfig (Config.) id)
              (.setTopicOverloadPolicy TopicOverloadPolicy/DISCARD_OLDEST)
              (.setStatisticsEnabled false)))
        (.destroy lock)
        {:state state :ctx ctx :notifications notifications}))))

(defn- atom-id [id]
  (str "chromatic-atom-" (name id)))

(defn- add-listener! [hazelcast-atom topic]
  (let [listener-id
        (.addMessageListener topic
          (reify MessageListener
            (onMessage [_ message]
              (let [[old-val new-val] (.getMessageObject message)]
                (doseq [[k f] (concat (.getWatches hazelcast-atom) (get-shared-watches hazelcast-atom))]
                  (when f
                    (f k hazelcast-atom old-val new-val)))))))]
    (swap! (.-local_ctx hazelcast-atom) assoc :listener listener-id)
    hazelcast-atom))

(defn distributed-atom
  "Creates a distributed atom that uses Hazelcast to store it's state.

  Accepts a Hazelcast instance, id that would be used to identify this atom,
  initial value x and additional options map. Initial value is applied only the first
  time an atom with given id is created on an instance.

  Options:
  :global-notifications - defaults to false, if true, would enable global notifications
  for watches

  When called for the first time (from Hazelcast point of view) it initializes Hazelcast objects,
  sets the initial value and returns an instance of HazelcastAtom.

  Further calls would return a new HazelcastAtom that reuses previously initialized objects.

  If global notifications are enabled it should closed using .close() when not needed.
  To destroy underlying distributed objects use destroy! function."
  [^HazelcastInstance instance id x & [opts]]
  (let [id (atom-id id)
        {:keys [global-notifications]} opts
        {:keys [state ctx notifications]}
        (if-not (find-reference instance id)
          (or (init-shared-objects instance id x global-notifications)
              (retrieve-shared-objects instance id))
          (retrieve-shared-objects instance id))
        hazelcast-atom (->HazelcastAtom state notifications ctx (atom {}))]

    (some->> notifications (add-listener! hazelcast-atom))
    hazelcast-atom))

(defn destroy!
  "Destroys atom state objects cluster-wide."
  [hazelcast-atom]
  (when-let [topic (.-notification_topic hazelcast-atom)]
    (.destroy ^ITopic topic))
  (.destroy ^IAtomicReference (.-local_ctx hazelcast-atom))
  (.destroy ^IAtomicReference (.state hazelcast-atom)))