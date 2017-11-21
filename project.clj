(defproject chromatic "0.0.1"
  :description "Distributed atom-like reference type backed by Hazelcast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.hazelcast/hazelcast "3.9"]
                 [com.hazelcast/hazelcast-client "3.9"]]
  :profiles {:dev {:source-paths ["dev"]}
             :test {:aot [vermilionsands.test.util]}})