(ns molecula.error
  (:import (clojure.lang Util)))

(defn no-transaction [] (IllegalStateException. "No transaction running"))
(defn set-after-commute [] (IllegalStateException. "Can't set after commute"))
(defn ref-unbound [ref] (IllegalStateException. (str ref " is unbound.")))
(defn retry-limit []
  (. Util runtimeException "Transaction failed after reaching retry limit"))
(defn invalid-ref-state
  ([] (IllegalStateException. "Invalid reference state"))
  ([e] (IllegalStateException. "Invalid reference state" e)))
