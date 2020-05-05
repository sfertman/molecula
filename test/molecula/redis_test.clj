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

(deftest mcas-or-report!-test-conflicts
  (testing "Should return true when update and no conflicts"
    (let [rks [:mcas1|k1 :mcas1|k2 :mcas1|k3]
          oldvals [42 43 44]
          newvals [12 13 14]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [updates (interleave rks oldvals newvals)]
        (is (true? (sut/mcas-or-report! conn [] updates))))))
  (testing "Sould return conflicts when updates & stale updates"
    (let [rks [:mcas2|k1 :mcas2|k2 :mcas2|k3]
          oldvals [42 43 44]
          newvals [52 53 54]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [updates (interleave rks [42 33 44] newvals)]
        (is (= [(second rks)] (sut/mcas-or-report! conn [] updates))))))
  (testing "Should return true when ensures and no conflicts"
    (let [rks [:mcas3|k1 :mcas3|k2 :mcas3|k3]
          oldvals [42 43 44]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [ensures (interleave rks oldvals)]
        (is (true? (sut/mcas-or-report! conn ensures []))))))
  (testing "Should return conflicts when ensures & stale ensures"
    (let [rks [:mcas4|k1 :mcas4|k2 :mcas4|k3]
          oldvals [42 43 44]]
      (is (= "OK" (sut/set-multi! conn rks oldvals)))
      (let [ensures (interleave rks [42 33 44])]
        (is (= [(second rks)] (sut/mcas-or-report! conn ensures []))))))
  #_(testing "Should return false when updates only and multi/exec fails not due to conflics"
    (comment "not sure how to make this happen"))
  (testing "Should return true when ensures, updates and no conflics"
    (let [rks-ens [:mcas5|k1 :mcas5|k2 :mcas5|k3]
          oval-ens [32 93 34]
          rks-upd [:mcas5|k4 :mcas5|k5 :mcas5|k6]
          oval-upd [42 43 44]
          nval-upd [52 53 54]]
      (is (= "OK" (sut/set-multi! conn rks-ens oval-ens)))
      (is (= "OK" (sut/set-multi! conn rks-upd oval-upd)))
      (let [ensures (interleave rks-ens oval-ens)
            updates (interleave rks-upd oval-upd nval-upd)]
        (is (true? (sut/mcas-or-report! conn ensures updates))))))
  (testing "Should return conflicts when ensures, updates & stale updates")
  (testing "Should return conflicts when ensures, updates & stale ensures")
  (testing "Should return conflicts when ensures, updates & stale updates, ensures")
  ;; TODO ^
  (testing "timeout") ;; not implemented

  )