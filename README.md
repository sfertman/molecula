# molecula
Clojure refs on redis with one line of code (experimental).

require
```clojure
(require '[molecula.core :as mol :refer [redis-ref]])
```

redis connection
```clojure
(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
```

def ref
```clojure
;; was: (def rr (ref {:so {:much "data"}})), and now:
(def rr (redis-ref conn :ref-key {:so {:much "data"}}))
```

dosync
```clojure
;; was (dosync ..., and now:
(mol/dosync conn
  (alter this inc)
  (println "hello")
  (alter that dec)
  (println "all-done"))
```

Everything else is (or should be) the same as refs except redis-ref uses optimistic locking instead of implementing STM on Redis.

Some stuff is still on TODO list:
- transaction timeout
- dispatch agents
- some exceptions are not exactly the same
- tests, tests and more tests