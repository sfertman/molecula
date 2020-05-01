(ns molecula.common-test
  (:require
    [clojure.test :refer [is]]
    [molecula.core :as mol :refer [redis-ref]]
    [molecula.transaction :as tx]
    [taoensso.carmine :as redis]))

(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})

(defmacro rr [& args] `(redis-ref conn ~@args))

(defmacro mds [& body] `(mol/dosync conn ~@body))

(defmacro with-tx [t & body]
  `(binding [tx/*t* (assoc ~t :conn conn)] ~@body))

(defmacro with-new-tx [& body]
  `(binding [tx/*t* (tx/->transaction conn)] ~@body))

(defmacro wcar* [& body] `(redis/wcar conn ~@body))

(defn flushall [] (wcar* (redis/flushall)))

(defmacro try-catch
  [e & forms]
  `(try
    ~@forms
    (is (= 0 1 "No exception thrown!"))
    (catch Exception e#
      (let [ex# ~e]
        (is (= (class ex#) (class e#)) )
        (is (= (.getMessage ex# ) (.getMessage e#)))))))
