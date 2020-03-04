(ns molecula.core-test
  (:require
    [clojure.test :refer :all]
    [molecula.core :as mol :refer [redis-ref]]
    [taoensso.carmine :as redis]))

(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
(defmacro wcar* [& body] `(redis/wcar conn ~@body))

(wcar* (redis/flushall))
