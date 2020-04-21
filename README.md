# molecula
Clojure refs on redis with one line of code (experimental).

[![Clojars Project](https://img.shields.io/clojars/v/molecula.svg)](https://clojars.org/molecula)
![Clojure CI](https://github.com/sfertman/molecula/workflows/Clojure%20CI/badge.svg?branch=master)

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

## Testing
Running the test suite requires redis backend service which can be easily created with [docker-compose](https://docs.docker.com/compose/install/).
To start a local backend:
```shell
$ cd redis
$ docker-compose up -d
```
This will start a redis server on `6379` and a redis-commander on `8081`. If you are a fan of redis-cli, run `redis-cli.sh` script in the same dir for the console.
