(ns molecula.redis-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn wcar* flushall]]
    [molecula.redis :as sut]
    [taoensso.carmine :as redis]))

(flushall) ;; got to put is somewhere else

(deftest deref-multi
  ; (testing "deref-multi no keys" ;; this should fail!
  ;   (is (= '() (sut/deref-multi conn []))))
  (testing "deref-multi 1 keys"
    (wcar* (redis/set :deref-multi|k11 {:data 42}))
    (is (= '(42) (sut/deref-multi conn [:deref-multi|k11]))))
  (testing "deref-multi 3 keys"
    (wcar* (redis/set :deref-multi|k31 {:data 42}))
    (wcar* (redis/set :deref-multi|k32 {:data 43}))
    (wcar* (redis/set :deref-multi|k33 {:data 44}))
    (is (= '(42 43 44) (sut/deref-multi conn [:deref-multi|k31 :deref-multi|k32 :deref-multi|k33]))))
  (testing "deref-multi 3 keys; 1 missing"
    (wcar* (redis/set :deref-multi|k3-11 {:data 42}))
    (wcar* (redis/set :deref-multi|k3-12 {:data 43}))
    (is (= '(42 43 nil) (sut/deref-multi conn [:deref-multi|k3-11 :deref-multi|k3-12 :deref-multi|k3-13])))))

(deftest set-multi!
  ; (testing "Set empty list" ;; this hsould fail!
  ;   (prn (sut/set-multi! conn [:a 42])))
  (testing "Set 1 kv"
    (sut/set-multi! conn [:set-multi|k11 42])
    (is (= [{:data 42}] (wcar* (redis/mget :set-multi|k11)))))
  (testing "Set 3 kvs"
    (sut/set-multi!
      conn
      [:set-multi|k31 42
       :set-multi|k32 43
       :set-multi|k33 44])
    (is (= (map #(hash-map :data %) [42 43 44])
           (wcar* (redis/mget :set-multi|k31
                              :set-multi|k32
                              :set-multi|k33))))))
(deftest conflicts)
(deftest cas-multi-or-report)