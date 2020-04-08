(ns molecula.transaction-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn rr]]
    [molecula.core :refer [redis-ref]]
    [molecula.transaction :as sut]))

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
  (testing "do-get without tx"
    (let [rr1 (rr :do-get|k1 42)]
      (try (sut/do-get rr1)
        (catch IllegalStateException e
          (is (= "No transaction running" (.getMessage e)))))))
  (testing "do-get with tx"
    (let [rr2 (rr :do-get|k2 42)]
      (with-new-tx
        (is (= 42 (sut/do-get rr2))) ;; first do-get fetches from redis
        ;; TODO: make sure to assert refs, oldvals and tvals after first do-get
        (is (= 42 (sut/do-get rr2))) ;; second do-get fetches from tvals (should be the same value)
        (set! sut/*t* (update sut/*t* :tvals assoc :do-get|k2 "fake-update" ))
        ;; ^ puts some fake data in :tvals for this ref
        (is (= "fake-update" (sut/do-get rr2))))))) ;; third do-get fetches from tval also but this time a fake value

(deftest do-set-test
  (testing "do-set without tx"
    (let [rr1 (rr :do-set|k1 42)]
      (try (sut/do-set rr1 43)
        (catch IllegalStateException e
          (is (= "No transaction running" (.getMessage e)))))))
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
  (testing "do-ensure without tx"
    (let [rr1 (rr :do-ensure|k1 42)]
      (try (sut/do-ensure rr1)
        (catch IllegalStateException e
          (is (= "No transaction running" (.getMessage e)))))))
  (testing "do-ensure with tx"
    (let [rk2 :do-ensure|k2
          rr2 (rr rk2 42)]
      (with-new-tx
        (is (nil? (sut/tval rk2)))
        (sut/do-ensure rr2)
        (is (= rk2 (sut/tget :ensures rk2)))
        (is (= 42 (sut/tval rk2)))))))

(deftest do-commute-test
  (testing "do-commute without tx"
    (let [rr1 (rr :do-commute|k1 42)]
      (try (sut/do-commute rr1 + [58])
        (catch IllegalStateException e
          (is (= "No transaction running" (.getMessage e)))))))
  (testing "do-commute with tx"
    (let [rk2 :do-commute|k2
          rr2 (rr rk2 42)]
      (with-new-tx
        (is (nil? (sut/tget :commutes rk2)) "Must not have rr2 commutes")
        (is (nil? (sut/tval rk2)) "Must not have rr2 tvals")
        (let [result1 (sut/do-commute rr2 + [58])
              commutes1 (sut/tget :commutes rk2)]
          (is (= 1 (count commutes1)))
          (is (= (sut/->CFn + [58]) (first commutes1)))
          (is (= 100 result1 (sut/tval rk2)))
          (let [result2 (sut/do-commute rr2 - [30 3])
                commutes2 (sut/tget :commutes rk2)]
            (is (= 2 (count commutes2)))
            (is (= (sut/->CFn - [30 3]) (second commutes2)))
            (is (= 67 result2 (sut/tval rk2)))))))))
(deftest commit-test
  )
(deftest run-test)
(deftest run-in-transaction-test)