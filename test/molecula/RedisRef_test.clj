(ns molecula.RedisRef-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :as ct :refer [rr mds flushall]]
    [molecula.transaction :as tx]
    [molecula.RedisRef]
    ))

(flushall)


(deftest -init-test)
(deftest -key-test)
(deftest -conn-test)

(deftest -deref-test
  (testing "Should return redis value when called outside transaction"
    (let [r1 (rr :deref|r1 42 )]
      (is (= 42 @r1)))
    (let [r2 (rr :deref|r2 {:k 42})]
      (is (= {:k 42} @r2))))
  (testing "Shoud return transaction when called inside transaction"
    (let [r3 (rr :deref|r3 42)]
      (prn "REDIS-REF R3" r3)
      (prn "@r3 is" @r3)
      (mds
        (prn "REDIS-REF inside-tx" r3)
        (prn "@r3 is" @r3)
        (clojure.pprint/pprint tx/*t*)
        (alter r3 inc)
        (prn "after inc @r3 is" @r3)
        (is (= 43 @r3))))))

(deftest -set-test
  (let [rr (RedisRef. {} )])
  (testing "Can only be called in transaction"
    (sut/-set {} ))
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