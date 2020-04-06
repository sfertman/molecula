(ns molecula.common-test
  (:require
    [molecula.core :as mol :refer [redis-ref]]
    [taoensso.carmine :as redis]))

(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
(defmacro rr [& args] `(redis-ref conn ~@args))
(defmacro mds [& body] `(mol/dosync conn ~@body))
(defmacro wcar* [& body] `(redis/wcar conn ~@body))

(defn flushall [] (wcar* (redis/flushall)))