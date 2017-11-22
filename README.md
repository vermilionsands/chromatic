# chromatic

Atom-like reference type, that uses Hazelcast to store it's state in a distributed fashion. 

## Caveat emptor

This is work in progress.

## Usage

In REPL:
```clj
(require '[vermilionsands.chromatic :as chromatic])
(import '[com.hazelcast.config Config])
(import '[com.hazelcast.core Hazelcast])

;; hazelcast instance
(def hazelcast (Hazelcast/newHazelcastInstance (Config.))

;; distributed atom
(def hz-atom (chromatic/distributed-atom hazelcast :test-atom 0)
```

Repeat the above steps in a different REPL (different JVM instance). After that you can use distributed atom like a regular one.  

Atom value after calls to `swap!` and `reset!` in one JVM would be available in another.

In one REPL:
```clj
;; set value in one REPL
(reset! hz-atom "my shared value")
```

After that in another one:
```clj
;; check that value is visible
@hz-atom ;; => would return "my shared value"
```

## Details

`vermilionsands.chromatic` defines a `HazelcastAtom` type which implements the same interfaces as `clojure.lang.Atom`, as well as a constructor function `distributed-atom`.
 
##### swap! and reset!

`swap!` is implemented using `compareAndSet` and passes data to `IAtomicReference`. Values need to be serializable.
Functions (for swap!) are executed locally and do not need to be serializable.

##### validators

Validator added using `set-validator!` is not shared between instances. Shared validator can be added using 
`set-shared-validator` from `vermilionsands.chromatic/DistributedAtom` protocol. This functions would be 
shared between instances.
  
**Note:** validator function has to be present on all JVM instances and has to be serializable.
    
Validation is called only on the instance which is changing the data.
 
##### watches
 
Watches added using `add-watch` are local (like validator). Shared watches can be added using `add-shared-watch` 
from `DistributedAtom` protocol are shared between instances. Functions need to meet the same requirements as shared validators.
  
By default notification that triggers watches is executed only on the instance which is changing the data (like with validation). 
There is an option to enable global notifications, which would trigger watches on all instances.
 
In order to turn on global notifications pass `{:global-notifications true}` to `distributed-atom` function when first atom instance is created.
This would create a notification topic that would be used to trigger watches when a change occurs.

Notifications are implemented using `ReliableTopic`. Each notification is a vector od `[old-value new-value]`.  
 
##### metadata

Metadata is stored on instance and is not shared

##### misc
  
* distributed atom is not partitioned (due to `IAtomicReference` not being partitioned).
* to free resources when no longer needed call `(destroy! distributed-atom)`. This would destroy all underlying distributed objects
* when notifications are enabled atom needs to be closed using `.close()` from `java.io.Closeable`
  Otherwise notification listener would not be removed. 
 

## License

Copyright © 2017 vermilionsands

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
