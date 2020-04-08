(ns molecula.redis-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn wcar* flushall]]
    [molecula.redis :as sut]
    [taoensso.carmine :as redis]))

(flushall) ;; got to put this somewhere else

(deftest deref-multi-test
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

(deftest set-multi!-test
  (testing "Set 1 kv"
    (sut/set-multi! conn [:set-multi|k11 42])
    (is (= [{:data 42}] (wcar* (redis/mget :set-multi|k11))))
    (sut/set-multi! conn [:set-multi|k12] [42])
    (is (= [{:data 42}] (wcar* (redis/mget :set-multi|k12)))))
  (testing "Set 3 kvs"
    (sut/set-multi!
      conn
      [:set-multi|k31 42
       :set-multi|k32 43
       :set-multi|k33 44])
    (is (= (map #(hash-map :data %) [42 43 44])
           (wcar* (redis/mget :set-multi|k31
                              :set-multi|k32
                              :set-multi|k33))))
    (sut/set-multi!
      conn
      [:set-multi|k31 :set-multi|k32 :set-multi|k33]
      [42 43 44])
    (is (= (map #(hash-map :data %) [42 43 44])
           (wcar* (redis/mget :set-multi|k31
                              :set-multi|k32
                              :set-multi|k33))))))
(deftest conflicts-test
  (let [rk1 :conflicts|k1
        rk2 :conflicts|k2
        rk3 :conflicts|k3
        rv1 42 rv2 43 rv3 44]
    (sut/set-multi! conn [rk1 rk2 rk3] [rv1 rv2 rv3])
    (testing "No conflicts"
      (is (empty? (sut/conflicts conn [rk1 rk2 rk3] [rv1 rv2 rv3]))))
    (testing "Some conflicts"
      (is (= [rk1] (sut/conflicts conn [rk1 rk2 rk3] [99 rv2 rv3])))
      (is (= [rk1 rk2] (sut/conflicts conn [rk1 rk2 rk3] [99 98 rv3])))
      (is (= [rk1 rk2 rk3] (sut/conflicts conn [rk1 rk2 rk3] [99 98 97]))))))

(deftest cas-multi-or-report-test) ;; TODO: this next