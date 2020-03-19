(ns molecula.transaction-test
  (:require
    [clojure.test :refer :all]
    [molecula.transaction :as tx]))


(deftest do-get)
(deftest do-set)
(deftest do-ensure)
(deftest do-commute)

(deftest run)
(deftest run-in-transaction)