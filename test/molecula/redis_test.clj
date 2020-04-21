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
    (sut/set-multi! conn [:set-multi1|k1 42])
    (is (= [{:data 42}] (wcar* (redis/mget :set-multi1|k1))))
    (sut/set-multi! conn [:set-multi1|k2] [42])
    (is (= [{:data 42}] (wcar* (redis/mget :set-multi1|k2)))))
  (testing "Set 3 kvs"
    (sut/set-multi!
      conn
      [:set-multi3|k1 42
       :set-multi3|k2 43
       :set-multi3|k3 44])
    (is (= (map #(hash-map :data %) [42 43 44])
           (wcar* (redis/mget :set-multi3|k1
                              :set-multi3|k2
                              :set-multi3|k3))))
    (sut/set-multi!
      conn
      [:set-multi3|k1 :set-multi3|k2 :set-multi3|k3]
      [42 43 44])
    (is (= (map #(hash-map :data %) [42 43 44])
           (wcar* (redis/mget :set-multi3|k1
                              :set-multi3|k2
                              :set-multi3|k3))))))
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

(deftest cas-multi-or-report-test-conflicts
  (testing "Should return true when update only and no conflicts"
    (let [rks [:mcas1|k1 :mcas1|k2 :mcas1|k3]
          oldvals [42 43 44]
          newvals [12 13 14]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [updates (interleave rks oldvals newvals)]
        (is (true? (sut/cas-multi-or-report conn [] updates))))))
  (testing "Sould return conflicting key(s) when updates only and has conflic(s)"
    (let [rks [:mcas2|k1 :mcas2|k2 :mcas2|k3]
          oldvals [42 43 44]
          newvals [52 53 54]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [updates (interleave rks [42 33 44] newvals)]
        (is (= [(second rks)] (sut/cas-multi-or-report conn [] updates))))))
  (testing "Should return false when updates only and multi/exec fails not due to conflics")
  (testing "Should return true when updates, ensures and no conflics"
    (let [rks [:mcas1|k1 :mcas1|k2 :mcas1|k3]
          oldvals [42 43 44]
          newvals oldvals]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [updates (interleave rks oldvals newvals)]
        (is (true? (sut/cas-multi-or-report conn [] updates))))))
  (testing "Should return conflicts when updates, ensures & stale updates")
  (testing "Should return conflicts when updates, ensures & stale ensures")
  (testing "Should return conflicts when updates, ensures & stale updates, ensures")

  (testing "ensures only") ;; I'm not sure what's the behoviour supposed to be here
  (testing "timeout") ;; not implemented

  )