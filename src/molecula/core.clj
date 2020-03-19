(ns molecula.core
  (:refer-clojure :exclude [dosync]) ;; suppress warning
  (:require
    [taoensso.carmine :as r]
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
    (redis-ref
      (:conn (.state redis-atom))
      (:k (.state redis-atom))
      @redis-atom
      ;; get meta
      ;; get validator
      ))
      ;; should create a ref using the redis atom blueprint
      ;; this could be an actual RedisRef constructor
  ([conn k] (RedisRef. conn k))
  ([conn k val] (let [r (redis-ref conn k)] (setnx* conn k val) r))
  ([conn k val & {mta :meta v-tor :validator}]
    (let [r (redis-ref conn k val)]
      (when mta (.resetMeta r mta))
      (when v-tor (.setValidator r v-tor))
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