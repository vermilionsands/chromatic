(ns vermilionsands.test.chromatic
  (:require [clojure.test :refer [deftest is testing]]
            [vermilionsands.chromatic :as chromatic])
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
      (let [n 1000
            start (CountDownLatch. 1)
            done (CountDownLatch. n)]
        (doseq [_ (range n)]
          (future-swap start done a inc))
        (.countDown start)
        (.await done)
        (is (= 1000 @a))))))

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

(deftest validator-test
  (testing "Local validator is not shared between instances"
    (let [a (chromatic/distributed-atom hazelcast-instance "local-validator-test" 0)
          b (chromatic/distributed-atom hazelcast-instance "local-validator-test" 0)]
      (set-validator! a (fn [x] (< x 10)))
      (swap! a inc)
      (is (= 1 @a))
      (is (thrown? IllegalStateException (swap! a + 10)))
      (is (= 1 @a))
      (is (= 1 @b))
      (swap! b + 10)
      (is (= 11 @a))
      (is (= 11 @b)))))
;  (testing "Shared validator is shared between instances"
;    (let [a (chromatic/distributed-atom hazelcast-instance "shared-validator-test" 0)
;          b (chromatic/distributed-atom hazelcast-instance "shared-validator-test" 0))
;      (chromatic/set-shared-validator! a (fn [x] (< x 10)))
;      (swap! a inc)
;      (is (= 1 @a))
;      (is (thrown? IllegalStateException (swap! a + 10)))
;      (is (thrown? IllegalStateException (swap! b + 10)))
;      (is (= 1 @a))
;      (is (= 1 @b)))))