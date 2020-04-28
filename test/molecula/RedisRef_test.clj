(ns molecula.RedisRef-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer :all]
    [molecula.error :as ex]
    [molecula.transaction :as tx]
    [molecula.RedisRef]
    [molecula.redis :as r]))

(flushall)

(deftest -init-test
  (let [rr1 (RedisRef. conn :init|k1)]
    (is (= {:conn conn :k :init|k1} (.state rr1))))
  (let [rr2 (RedisRef. conn :init|k2 42)]
    (is (= {:conn conn :k :init|k2} (.state rr2))))
  (let [rr3 (RedisRef. conn :init|k3 42 {:metatron 456})]
    (is (= {:conn conn :k :init|k3} (.state rr3)))
    (is (= {:metatron 456} (meta rr3)))))

(deftest -key-test
  (let [rr1 (RedisRef. conn :key|k1 42)]
    (is (= :key|k1 (.key rr1)))))

(deftest -conn-test
  (let [rr1 (RedisRef. conn :conn|k1 42)]
    (is (= conn (.conn rr1)))))

(deftest -deref-test
  ;; TODO: this test should be in core because it's testing integration stuff
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
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :set|k1 42)]
      (try-catch (ex/no-transaction) (. rr1 set 43))))
  (testing "Should do-set"))

(deftest -commute-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :commute|k1 42)]
      (try-catch (ex/no-transaction) (. rr1 commute + '(1 2 3)))))
  (testing "Should do-commute"))

(deftest -alter-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :alter|k1 42)]
      (try-catch (ex/no-transaction) (. rr1 alter + '(1 2 3)))))
  (testing "Should do-set with f"))

(deftest -touch-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :touch|k1 42)]
      (try-catch (ex/no-transaction) (. rr1 touch))))
  (testing "Should do-ensure"))