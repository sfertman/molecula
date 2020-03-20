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
  exprs and any nested calls.  Starts a transaction if none is already
  running on this thread. Any uncaught exception will abort the
  transaction and flow out of dosync. The exprs may be run more than
  once, but any effects on Refs will be atomic."
  ;; ^^ rewrite docstring for redis
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
  ([conn k val] (let [r (redis-ref conn k)] (setnx* conn k val) r))
  ([conn k val & {:keys [meta validator]}]
    (let [r (redis-ref conn k val)]
      (when meta (.resetMeta r meta))
      (when validator (.setValidator r validator))
      r)))

; (def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
; (defmacro wcar* [& body] `(r/wcar conn ~@body))

; (let [t (wcar* (r/time))]

;   (wcar* (r/set :a t)))

; (wcar*
;   (wcar* (r/time)
;     (wcar* (r/get :a)
;       (wcar* (r/set :a "1231242334")))))

; )