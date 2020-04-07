(ns molecula.transaction-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :refer [conn rr]]
    [molecula.core :refer [redis-ref]]
    [molecula.transaction :as sut]))

(defmacro with-tx [& body]
  `(binding [sut/*t* (sut/->transaction conn)] ~@body))

(deftest tconn-test
  (testing "throw when no tx running"
    (try (sut/tconn)
      (catch IllegalStateException e
        (is (= "No transaction running" (.getMessage e))))))
  (testing "get conn in tx"
    (with-tx (is (= conn (sut/tconn))))))

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
      (with-tx
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
      (with-tx
        (set! sut/*t* (update sut/*t* :commutes assoc :do-set|k2 "fake-commute"))
        (try
          (sut/do-set rr2 43)
          (catch IllegalStateException e
            (is (= "Can't set after commute" (.getMessage e)))))
        (is (= 42 @rr2) "do-set should fail to change to new value"))
      (is (= 42 @rr2) "do-set should not change redis val")))
  (testing "do-set with tx"
    (let [rr3 (rr :do-set|k3 42)]
      (with-tx
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
    (let [k2 :do-ensure|k2
          rr2 (rr k2 42)]
      (with-tx
        (is (nil? (sut/tval k2)))
        (sut/do-ensure rr2)
        (is (= k2 (sut/tget :ensures k2)))
        (is (= 42 (sut/tval k2)))))))

(deftest do-commute-test)

(deftest run-test)
(deftest run-in-transaction-test)