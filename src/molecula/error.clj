(ns molecula.error
  (:import (clojure.lang Util)))

(defn set-after-commute [] (IllegalStateException. "Can't set after commute"))
(defn ref-unbound [ref] (IllegalStateException. (str ref " is unbound.")))
(defn retry-limit []
  (. Util runtimeException "Transaction failed after reaching retry limit"))
