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
  (testing "Should return redis value when called outside transaction"
    (let [rr1 (RedisRef. conn :deref1|k1 42 )]
      (with-redefs [r/deref* (constantly "redis-val-here") ]
        (is (= "redis-val-here" (.deref rr1))))))
  (testing "Shoud do-get when called inside transaction"
    (let [rr1 (RedisRef. conn :deref2|k1 42)]
      (with-redefs [r/deref* (constantly "redis-val-here")
                    tx/do-get (constantly "tval-here")]
        (is (= "redis-val-here" (.deref rr1)))
        (with-new-tx
          (is (= "tval-here" (.deref rr1))))))))

(deftest -set-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :set1|k1 42)]
      (try-catch (ex/no-transaction) (.set rr1 43))))
  (testing "Should do-set when called inside transaction"
    (let [rr1 (RedisRef. conn :set2|k1 42)]
      (with-redefs [tx/do-set (constantly "did-set")]
        (with-new-tx
          (is (= "did-set" (.set rr1 43))))))))

(deftest -commute-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :commute1|k1 42)]
      (try-catch (ex/no-transaction) (.commute rr1 + '(1 2 3)))))
  (testing "Should do-commute when called inside transaction"
    (let [rr1 (RedisRef. conn :commute2|k1 42)]
      (with-redefs [tx/do-commute (constantly "did-commute")]
        (with-new-tx
          (is (= "did-commute" (.commute rr1 + '(1 2 3)))))))))

(deftest -alter-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :alter1|k1 42)]
      (try-catch (ex/no-transaction) (.alter rr1 + '(1 2 3)))))
  (testing "Should do-set with f and args when called inside transaction"
    (let [rr1 (RedisRef. conn :alter2|k1 42)]
      (with-redefs [tx/do-get (constantly 42)
                    tx/do-set (fn [_ val] val)]
        (with-new-tx
          (is (= 48 (.alter rr1 + '(1 2 3)))))))))

(deftest -touch-test
  (testing "Can only be called in transaction"
    (let [rr1 (RedisRef. conn :touch1|k1 42)]
      (try-catch (ex/no-transaction) (.touch rr1))))
  (testing "Should do-ensure when called inside transaction"
    (let [rr1 (RedisRef. conn :touch2|k1 42)
          ensure-result (atom nil)]
      (with-redefs [tx/do-ensure (fn [_] (reset! ensure-result "did-ensure"))]
        (with-new-tx
          (.touch rr1)
          (is (= "did-ensure" @ensure-result)))))))