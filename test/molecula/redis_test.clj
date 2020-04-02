(ns molecula.redis-test
  (:require
    [clojure.test :refer :all]
    [molecula.redis :as sut]
    [taoensso.carmine :as redis]))

(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
(defmacro wcar* [& body] `(redis/wcar conn ~@body))

(wcar* (redis/flushall))


(deftest deref-multi
  (wcar* (redis/set :deref-multi-k1 {:data 42}))
  (wcar* (redis/set :deref-multi-k2 {:data 43}))
  (wcar* (redis/set :deref-multi-k3 {:data 44}))
  (is (= '(42 43 44) (sut/deref-multi conn [:deref-multi-k1 :deref-multi-k2 :deref-multi-k3] ))))

(deftest conflicts)
(deftest cas-multi-or-report)