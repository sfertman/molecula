(ns molecula.RedisRef-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :as ct :refer [rr mds flushall]]
    [molecula.transaction :as tx]
    ))

(flushall)

(deftest deref-test
  (testing "Should deref outside transaction"
    (let [r1 (rr :deref|r1 42 )]
      (is (= 42 @r1)))
    (let [r2 (rr :deref|r2 {:k 42})]
      (is (= {:k 42} @r2))))
  (testing "Shoud deref inside transaction"
    (let [r3 (rr :deref|r3 42)]
      (prn "REDIS-REF R3" r3)
      (prn "@r3 is" @r3)
      (mds
        (prn "REDIS-REF inside-tx" r3)
        (prn "@r3 is" @r3)
        (clojure.pprint/pprint tx/*t*)
        (alter r3 inc)
        (prn "after inc @r3 is" @r3)
        #_(is (= 43 @r3))))))
