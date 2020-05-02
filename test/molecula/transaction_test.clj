(ns molecula.transaction-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn rr wcar*]]
    [molecula.core :refer [redis-ref]]
    [molecula.error :as ex]
    [molecula.redis :as r]
    [molecula.transaction :as sut]
    [taoensso.carmine :as redis]))

(defmacro with-tx [t & body]
  `(binding [sut/*t* (assoc ~t :conn conn)] ~@body))

(defmacro with-new-tx [& body]
  `(binding [sut/*t* (sut/->transaction conn)] ~@body))

(deftest tconn-test
  (testing "throw when no tx running"
    (try (sut/tconn)
      (catch IllegalStateException e
        (is (= (.getMessage (ex/no-transaction)) (.getMessage e))))))
  (testing "get conn in tx"
    (with-new-tx (is (= conn (sut/tconn))))))

(deftest do-get-test
  (testing "do-get with tx"
    (let [rr1 (rr :do-get1|k1 42)]
      (with-new-tx
        (is (= 42 (sut/do-get rr1))) ;; first do-get fetches from redis
        (is (= 42 (sut/do-get rr1))) ;; second do-get fetches from redis again
        (set! sut/*t* (update sut/*t* :tvals assoc :do-get1|k1 "fake-update"))
        ;; ^ puts some fake data in :tvals for this ref
        (is (= "fake-update" (sut/do-get rr1)))))) ;; third do-get fetches from tval because now it is set within tx
  (testing "do-get ref-unbound"
    (let [rk1 :do-get2|k1
          rr1 (rr rk1 42)]
      (wcar* (redis/del rk1))
      (with-new-tx
        (try
          (sut/do-get rr1)
          (catch IllegalStateException e
            (is (= (.getMessage (ex/ref-unbound rr1)) (.getMessage e)))))))))
(deftest do-set-test
  (testing "Should fail after commute"
    (let [rr2 (rr :do-set|k2 42)]
      (with-new-tx
        (set! sut/*t* (update sut/*t* :commutes assoc :do-set|k2 "fake-commute"))
        (try
          (sut/do-set rr2 43)
          (catch IllegalStateException e
            (is (= (.getMessage (ex/set-after-commute)) (.getMessage e)))))
        (is (= 42 @rr2) "do-set should fail to change to new value"))
      (is (= 42 @rr2) "do-set should not change redis val")))
  ;; TODO: more granular tests!
  (testing "do-set with tx"
    (let [rr3 (rr :do-set|k3 42)]
      (with-new-tx
        (let [result (sut/do-set rr3 43)]
          (is (= 43 result)) "do-set should return new value")
        (is (= 43 @rr3) "deref in tx should return new value"))
      (is (= 42 @rr3) "dedo-set should not change redis val because we never committed!"))))

(deftest do-ensure-test
  (testing "Should update oldvals with redis value when this is the first time we touch this ref")
  (testing "Should update ensures when new ensure")
  (testing "do-ensure with tx" ;; this is fine but not granular enough
    (let [rk2 :do-ensure|k2
          rr2 (rr rk2 42)]
      (with-new-tx
        (sut/do-ensure rr2)
        (is (= rk2 (sut/tget :ensures rk2)))
        ))))

(deftest do-commute-test
  ;; TODO: more tests

  (testing "do-commute with tx"
    (let [rk2 :do-commute|k2
          rr2 (rr rk2 42)]
      (with-new-tx
        (is (nil? (sut/tget :commutes rk2)) "Must not have rr2 commutes")
        (is (nil? (sut/tget :tvals rk2)) "Must not have rr2 tvals")
        (let [result1 (sut/do-commute rr2 + [58])
              commutes1 (sut/tget :commutes rk2)]
          (is (= 1 (count commutes1)))
          (is (= (sut/->CFn + [58]) (first commutes1)))
          (is (= 100 result1 (sut/tget :tvals rk2)))
          (let [result2 (sut/do-commute rr2 - [30 3])
                commutes2 (sut/tget :commutes rk2)]
            (is (= 2 (count commutes2)))
            (is (= (sut/->CFn - [30 3]) (second commutes2)))
            (is (= 67 result2 (sut/tget :tvals rk2)))))))))


(deftest validate-test
  (testing "Should validate given keys")
  (testing "Should validate updatables if no inputs given")
  (testing "Should throw 'Invalid reference state' on validation failure")
)

(deftest commit-test
  (testing "Should return no-more-retries when run out f retries"
    (is (= {:error :no-more-retries :retries 0} (sut/commit 0))))
  (testing "Should return stale-old-vals when sets are conflicting"
    (let [rk1 :commit1|k1 rr1 (rr rk1)
          rk2 :commit1|k2 rr2 (rr rk2)
          rk3 :commit1|k3 rr3 (rr rk3)
          refs (zipmap [rk1 rk2 rk3] [rr1 rr2 rr3])]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3} :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3} :commutes {rk1 [:stuff]} :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3} :ensures #{rk1} :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))))
  (testing "Should return stale-old-vals when ensures are conflicting"
    (let [rk1 :commit1|k1 rr1 (rr rk1)
          rk2 :commit1|k2 rr2 (rr rk2)
          rk3 :commit1|k3 rr3 (rr rk3)
          refs (zipmap [rk1 rk2 rk3] [rr1 rr2 rr3])]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3}  :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3} :commutes {rk1 [:stuff]} :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3} :sets #{rk1} :refs refs}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))))
  (testing "Should retry conflicting commutes"
    (let [rk1 :commit3|k1 rr1 (rr rk1)
          rk2 :commit3|k2 rr2 (rr rk2)
          rk3 :commit3|k3 rr3 (rr rk3)
          refs (zipmap [rk1 rk2 rk3] [rr1 rr2 rr3])]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]} :refs refs}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]} :ensures #{rk2 rk3} :refs refs}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]} :ensures #{rk2} :sets #{rk2 rk3} :refs refs}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))))
  (testing "Should validate retried commutes") ;; TODO
)



(deftest notify-watches-test)

(deftest run-test
  (testing "Should throw ex/retry-limit if no transaction retries left"
    (with-redefs [sut/RETRY_LIMIT 0]
      (try
        (sut/run (fn [] 42))
        (catch RuntimeException re
          (is (= (.getMessage (ex/retry-limit)) (.getMessage re)))))))

  #_(testing "TODO: Should throw ex/timeout if not transaction timeout exceeded"
    )
  (testing "Should validate all ref values after f is called"

    )
  (testing "Should commit ref updates"
    )
  (testing "Should notify watches"
    )
  (testing "Should return result of (f)"
    )
  (testing "Should throw ex/retry-limit when commit fails with :no-more-retries"
    )
  #_(testing "TODO: Should throw ex/timeout when commit fails with :operation-timed-out"
    )
  (testing "Should retry transaction when commit fails with :stale-oldvals"
    )
  )

(deftest run-in-transaction-test
  (testing "Should run in tx when tx exists"
    (with-tx {:tx "that-exists"}
      (with-redefs [sut/run (fn [_] sut/*t*)]
        (let [res (sut/run-in-transaction conn
                    (fn [] (+ 1 1) (* 2 2)))]
          (is (= {:tx "that-exists" :conn conn} res))))))
  (testing "Should create new tx if not in tx"
    (with-redefs [sut/run (fn [_] sut/*t*)]
      (let [res (sut/run-in-transaction conn
                  (fn [] (+ 1 1) (* 2 2)))]
        (is (= (sut/->transaction conn) res))))))