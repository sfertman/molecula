(ns molecula.RedisRef-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer :all]
    [molecula.transaction :as tx]
    [molecula.RedisRef]
    [molecula.redis :as r]))

(flushall)


(deftest -init-test)
(deftest -key-test)
(deftest -conn-test)

(deftest -deref-test
  (testing "Should return redis value when called outside transaction"
    (let [rr1 (rr :deref1|k1 42 )]
      (is (= 42 @rr1)))
    (let [rr2 (rr :deref1|k2 {:k 42})]
      (is (= {:k 42} @rr2))))
  (testing "Shoud return transaction value when called inside transaction"
    (let [rk1 :deref2|k1
          rr1 (rr rk1 42)]
      (is (= 42 @rr1))
      (mds
        (alter rr1 inc)
        (is (= 43 @rr1) "Value within transaction has changed")
        (is (= 42 (r/deref* conn rk1)) "Value on redis is still the old one before transaction commited"))
      (is (= 43 @rr1) "Value on redis is the new value once transaction is done"))))

(deftest -set-test
  (testing "Can only be called in transaction")
  (testing "Should do-set")
)
(deftest -commute-test)
(deftest -alter-test
  (testing "Can only be called in transaction")
  (testing "Should do-set with f")
  )
(deftest -touch-test
  (testing "Can only be called in transaction")
  (testing "Should do-ensure"))