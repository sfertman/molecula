(ns molecula.core
  (:refer-clojure :exclude [dosync]) ;; suppress warning
  (:require
    [molecula.RedisRef]
    [molecula.redis :refer [setnx*]]
    [molecula.transaction :as tx]))

(defmacro mol-sync
  [conn flags-ignored-for-now & body]
  `(tx/run-in-transaction ~conn (fn [] ~@body)))

(defmacro dosync
  "Runs the exprs (in an implicit do) in a transaction that encompasses
  exprs and any nested calls. Starts a transaction if none is already
  running on this thread. Any uncaught exception will abort the
  transaction and flow out of dosync. The exprs may be run more than
  once, but any effects on Refs will be atomic."
  ;; ^^ rewrite docstring for redis
  ;; TODO: make sure that: Any uncaught exception will abort the transaction and flow out of dosync
  [conn & exprs]
  `(mol-sync ~conn nil ~@exprs))

(defn redis-ref
  ([redis-atom]
    (let [rr (redis-ref
               (:conn (.state redis-atom))
               (:k (.state redis-atom))
               @redis-atom
               :meta (meta redis-atom)
               :validator (.getValidator redis-atom))]
      (doseq [w (.getWatches redis-atom)]
        (add-watch rr (key w) (val w)))
      rr))
  ([conn k] (RedisRef. conn k))
  ([conn k val] (let [rr (redis-ref conn k)] (setnx* conn k val) rr))
  ([conn k val & {:keys [meta validator]}]
    (tx/validate* validator val)
    ;; NOTE: must validate here, before a value is set on backend
    (let [rr (redis-ref conn k val)]
      (when meta (.resetMeta rr meta))
      (when validator (.setValidator rr validator))
      rr)))

;; TODO: configurables?
;; - mcas timeout
;; - tx retries
;; actually, not sure. those are more of a transaction properties and not refs
;; maybe these should be configurable with dosync