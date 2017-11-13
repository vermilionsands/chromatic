(ns user
  (:require [vermilionsands.chromatic :as chromatic])
  (:import [com.hazelcast.config Config]
           [com.hazelcast.core Hazelcast]))

(defonce ^:dynamic *hazelcast* nil)

(defn get-hz-instance []
  (let [cfg (Config.)]
    (Hazelcast/newHazelcastInstance cfg)))

(defn init! []
  (alter-var-root #'*hazelcast* (fn [_] (get-hz-instance))))