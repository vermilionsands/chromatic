(ns vermilionsands.chromatic_test
  (:require [clojure.test :refer [deftest is testing]]
            [vermilionsands.chromatic :as chromatic]
            [vermilionsands.test-helpers :as test-helpers])
  (:import [com.hazelcast.config Config]
           [com.hazelcast.core Hazelcast ITopic]
           [java.util.concurrent CountDownLatch]
           [vermilionsands.chromatic HazelcastAtom]))

(defonce hazelcast-instance (Hazelcast/newHazelcastInstance (Config.)))

(defn- future-swap [^CountDownLatch start ^CountDownLatch done a f & args]
  (future
    (.await start)
    (apply swap! a f args)
    (.countDown done)))

(deftest init-test
  (let [a (chromatic/distributed-atom hazelcast-instance "init-test" 1)
        b (chromatic/distributed-atom hazelcast-instance "init-test" 2 {:global-notifications true})
        c (chromatic/distributed-atom hazelcast-instance "atom-with-topic" 2 {:global-notifications true})]
    (testing "Atom should be initialized with init value"
      (is (= 1 @a)))
    (testing "Atom should not have notification channel"
      (is (nil? (.notification_topic ^HazelcastAtom a))))
    (testing "Init should be skipped if atom already exists"
      (is (= 1 @b)))
    (testing "Current options should not be overridden"
      (is (nil? (.notification_topic ^HazelcastAtom b))))
    (testing "Atom has notification topic"
      (is (instance? ITopic (.notification_topic ^HazelcastAtom c))))))

(deftest swap-test
  (let [a (chromatic/distributed-atom hazelcast-instance "swap-test" 0)]
    (testing "Swap arities"
      (is (= 1 (swap! a inc)))
      (is (= 2 (swap! a + 1)))
      (is (= 4 (swap! a + 1 1)))
      (is (= 7 (swap! a + 1 1 1)))
      (is (= 11 (swap! a + 1 1 1 1))))
    (testing "Multiple swaps"
      (reset! a 0)
      (let [n 100
            start (CountDownLatch. 1)
            done (CountDownLatch. n)]
        (doseq [_ (range n)]
          (future-swap start done a inc))
        (.countDown start)
        (.await done)
        (is (= 100 @a))))))

(deftest reset-test
  (let [a (chromatic/distributed-atom hazelcast-instance "reset-test" 0)]
    (is (= 0 @a))
    (reset! a 1)
    (is (= 1 @a))))

(deftest two-instances-test
  (let [a (chromatic/distributed-atom hazelcast-instance "two-instances-test" 0)
        b (chromatic/distributed-atom hazelcast-instance "two-instances-test" 0)]
    (is (zero? @a))
    (is (zero? @b))
    (swap! a inc)
    (is (= 1 @a))
    (is (= 1 @b))))

(deftest meta-test
  (let [a (chromatic/distributed-atom hazelcast-instance "meta-test" 0)
        m {:test :meta}]
    (is (nil? (meta a)))
    (reset-meta! a m)
    (is (= m (meta a)))
    (alter-meta! a assoc :altered :key)
    (is (= {:test :meta :altered :key} (meta a)))))

(deftest meta-is-per-instance-test
  (let [a (chromatic/distributed-atom hazelcast-instance "meta-test" 0)
        b (chromatic/distributed-atom hazelcast-instance "meta-test" 0)]
    (reset-meta! a {:test :meta})
    (is (= {:test :meta} (meta a)))
    (is (nil? (meta b)))))

;; todo add set nil validator test
(deftest validator-test
  (testing "Local validator is not shared between instances"
    (let [a (chromatic/distributed-atom hazelcast-instance "local-validator-test" 0)
          b (chromatic/distributed-atom hazelcast-instance "local-validator-test" 0)]
      (set-validator! a test-helpers/less-than-10)
      (is (= test-helpers/less-than-10 (get-validator a)))
      (is (nil? (get-validator b)))
      (swap! a inc)
      (is (= 1 @a))
      (is (thrown? IllegalStateException (swap! a + 10)))
      (is (= 1 @a))
      (is (= 1 @b))
      (swap! b + 10)
      (is (= 11 @a))
      (is (= 11 @b))))
  (testing "Shared validator is shared between instances"
    (let [a (chromatic/distributed-atom hazelcast-instance "shared-validator-test" 0)
          b (chromatic/distributed-atom hazelcast-instance "shared-validator-test" 0)]
      (chromatic/set-shared-validator! a test-helpers/less-than-10)
      (swap! a inc)
      (is (= 1 @a))
      (is (thrown? IllegalStateException (swap! a + 10)))
      (is (thrown? IllegalStateException (swap! b + 10)))
      (is (= 1 @a))
      (is (= 1 @b))))
  (testing "Both shared and local validators are called"
    (let [a (chromatic/distributed-atom hazelcast-instance "mixed-validator-test" 0)]
      (set-validator! a even?)
      (chromatic/set-shared-validator! a test-helpers/less-than-4)
      ;; shared validator kicks in
      (is (thrown? IllegalStateException (swap! a + 10)))
      ;; local validator kicks in
      (is (thrown? IllegalStateException (swap! a + 1))))))

(deftest local-watch-test
  (testing "Local watch test without notification test"
    (let [a (chromatic/distributed-atom hazelcast-instance "watch-test" 0)
          b (chromatic/distributed-atom hazelcast-instance "watch-test" 0)
          [watch-a state-a] (test-helpers/watch-and-store)
          [watch-b state-b] (test-helpers/watch-and-store)]
      (add-watch a :1 watch-a)
      (add-watch b :1 watch-b)
      (is (= {:1 watch-a} (.getWatches a)))
      (is (= {:1 watch-b} (.getWatches b)))
      (swap! a inc)
      (is (= [[0 1]] @state-a))
      (is (empty? @state-b))))
  (testing "Local watch with notification test"
    (let [a (chromatic/distributed-atom hazelcast-instance "notification-test" 0 {:global-notifications true})
          b (chromatic/distributed-atom hazelcast-instance "notification-test" 0)
          [watch-a state-a] (test-helpers/watch-and-store)
          [watch-b state-b] (test-helpers/watch-and-store)]
      (add-watch a :local watch-a)
      (add-watch b :local watch-b)
      (swap! a inc)
      (Thread/sleep 200) ;; let notification topic do it's job
      (is (= [[0 1]] @state-a))
      (is (= @state-a @state-b)))))

(deftest shared-watch-test
  (let [a (chromatic/distributed-atom hazelcast-instance "shared-watch-test" 0 {:global-notifications true})
        b (chromatic/distributed-atom hazelcast-instance "shared-watch-test" 0)
        [local-watch local-state] (test-helpers/watch-and-store)]
    (add-watch a :local local-watch)
    (chromatic/add-shared-watch a :shared test-helpers/store-to-atom-watch)
    (is (some? (:shared (chromatic/get-shared-watches a))))
    (is (some? (:shared (chromatic/get-shared-watches b))))
    (swap! a inc)
    (Thread/sleep 200)
    (is (= [[0 1]] @local-state))
    (is (= [[0 1] [0 1]] @test-helpers/watch-log))))