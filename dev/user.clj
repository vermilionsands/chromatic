(ns user
  (:require [vermilionsands.chromatic :as chromatic])
  (:import [com.hazelcast.client HazelcastClient]
           [com.hazelcast.client.config ClientConfig]
           [com.hazelcast.config Config]
           [com.hazelcast.core Hazelcast]))


(defonce ^:dynamic *hazelcast* nil)

(defn get-hz-instance []
  (let [cfg (Config.)]
    (Hazelcast/newHazelcastInstance cfg)))

(defn get-hz-client-instance [& [address]]
  (let [cfg (ClientConfig.)]
    (.addAddress (.getNetworkConfig cfg) (into-array [(or address "127.0.0.1")]))
    (HazelcastClient/newHazelcastClient cfg)))

(defn init! []
  (alter-var-root #'*hazelcast* (fn [_] (get-hz-instance))))

(defn init-client! []
  (alter-var-root #'*hazelcast* (fn [_] (get-hz-client-instance))))