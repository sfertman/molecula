(ns molecula.transaction-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn rr wcar*]]
    [molecula.core :refer [redis-ref]]
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
        (is (= "No transaction running" (.getMessage e))))))
  (testing "get conn in tx"
    (with-new-tx (is (= conn (sut/tconn))))))

(deftest tput-refs-test
  (let [rr1 (rr :tput-refs|k1 42)
        tx (sut/->transaction conn)
        result (sut/tput-refs tx rr1) ]
    (is (= rr1 (get-in result [:refs :tput-refs|k1])))))

(deftest tput-oldvals-test)
(deftest tput-tvals-test)
(deftest tput-sets-test)
(deftest tput-ensures-test)
(deftest tput-commutes-test)
(deftest tput!-test)
;; TODO: for the sake of completion write these tests

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
            (is (= (str rr1 " is unbound.") (.getMessage e)))))))))
(deftest do-set-test
  (testing "Should fail after commute"
    (let [rr2 (rr :do-set|k2 42)]
      (with-new-tx
        (set! sut/*t* (update sut/*t* :commutes assoc :do-set|k2 "fake-commute"))
        (try
          (sut/do-set rr2 43)
          (catch IllegalStateException e
            (is (= "Can't set after commute" (.getMessage e)))))
        (is (= 42 @rr2) "do-set should fail to change to new value"))
      (is (= 42 @rr2) "do-set should not change redis val")))
  (testing "do-set with tx"
    (let [rr3 (rr :do-set|k3 42)]
      (with-new-tx
        (let [result (sut/do-set rr3 43)]
          (is (= 43 result)) "do-set should return new value")
        (is (= 43 @rr3) "deref in tx should return new value"))
      (is (= 42 @rr3) "dedo-set should not change redis val because we never committed!"))))

(deftest do-ensure-test
  (testing "do-ensure with tx"
    (let [rk2 :do-ensure|k2
          rr2 (rr rk2 42)]
      (with-new-tx
        (sut/do-ensure rr2)
        (is (= rk2 (sut/tget :ensures rk2)))
        ))))

(deftest do-commute-test
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

(deftest commit-test
  ;; Do redis/cas-multi-or-report first
  ;; also need to handle
  ;;  no updates case
  ;;  what happens when there's nothing ot update but something to ensure?
  (testing "Should return no-more-retries when run out f retries"
    (is (= {:error :no-more-retries :retries 0} (sut/commit 0))))

  (testing "Should return stale-old-vals when sets are conflicting"
    (let [rk1 :commit1|k1
          rk2 :commit1|k2
          rk3 :commit1|k3]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3} :commutes {rk1 [:stuff]}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :sets #{rk1 rk2 rk3} :ensures #{rk1}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))))

  (testing "Should return stale-old-vals when ensures are conflicting"
    (let [rk1 :commit2|k1
          rk2 :commit2|k2
          rk3 :commit2|k3]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3} :commutes {rk1 [:stuff]}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :ensures #{rk1 rk2 rk3} :sets #{rk1}}
          (is (= {:error :stale-oldvals :retries 100} (sut/commit 100)))))))
  (testing "Should retry conflicting commutes"
    (let [rk1 :commit3|k1
          rk2 :commit3|k2
          rk3 :commit3|k3]
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]}}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]} :ensures #{rk2 rk3}}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))
      (with-redefs [r/cas-multi-or-report (fn [& _] [rk1])]
        (with-tx {:conn conn :commutes {rk1 [:stuff]} :ensures #{rk2} :sets #{rk2 rk3}}
          (is (= {:error :no-more-retries :retries 0} (sut/commit 1)))))))
)
(deftest run-test
  "Should throw ex-retry-limit is no more retries"

  )

(deftest run-in-transaction-test
  (testing "Should run in tx when tx exists"
    (with-tx {:tx "that-exists"}
      (with-redefs [sut/run (fn [_] sut/*t*)]
        (let [res (sut/run-in-transaction conn
                    (fn [] (+ 1 1) (* 2 2)))]
          (is (= {:tx "that-exists" :conn conn} res))))))
  (testing "Should create new tx if not in tx")
    (with-redefs [sut/run (fn [_] sut/*t*)]
      (let [res (sut/run-in-transaction conn
                  (fn [] (+ 1 1) (* 2 2)))]
        (is (= (sut/->transaction conn) res)))))