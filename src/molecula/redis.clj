(ns molecula.redis
  (:require
    [molecula.util :as u]
    [taoensso.carmine :as r]))

(defn deref* [conn k]
  (if-let [redis-val (r/wcar conn (r/get k))]
    (:data redis-val)
    {:mol-redis-err :ref-key-nx}))

(defn setnx* [conn k newval] (r/wcar conn (r/setnx k {:data newval})))

(defn deref-multi [conn ks]
  (map :data (r/wcar conn (apply r/mget ks))))

;; Note: watch out for LazySeq
(defn set-multi!
  ([conn kvs]
    (let [kvs* (apply concat (map (fn [[k v]] [k {:data v}]) (partition 2 kvs)))]
      (r/wcar conn (apply r/mset kvs*))))
  ([conn ks vs]
    (let [kvs (apply concat (map (fn [k v] [k {:data v}]) ks vs))]
      (r/wcar conn (apply r/mset kvs)))))

(defn conflicts
  ;; ^ there might be a more efficient way to write this fn
  "Returns a list of keys that failed compare to oldvals"
  [conn ks oldvals]
  (->> (deref-multi conn ks)
       (u/zipseq ks oldvals)
       (filter (fn [[_ ov nv]] (not= ov nv)))
       (map first)))

(defmacro multi-exec
  [conn & forms]
  `(r/wcar ~conn (r/multi) ~@forms (r/exec)))


(defn cas-multi-or-report
  "this is basically our cas-multi but with a twist
  it reports which keys failed compare to oldval
  Returns:
    - true when everything went fine
    - false if soemthing went wrong in watch/multi/exec
    - an array of ref keys that failed compare to oldvals
    // TODO: ^^ maybe make the api a bit friendlier?
  Usage:
  ```
  (cas-multi-or-report
    conn
    [:kte1 oldval1
     :kte2 oldval2
     :kte3 oldval3] ;; keys to ensure
    [:ktu1 oldval1 newval1
     :ktu2 oldval2 newval2
     :ktu3 oldval3 newval3] ;; keys to update
    :timeout 10) ;; timeout in seconds (not implemented yet)
  ```
  TODO: figure out a way to fail a transaction that takes too long
  TODO(ocd): optimize let difinitions
  "
  [conn ensures updates]
  (let [eks (take-nth 3 ensures) ;; ensure keys
        eov (take-nth 3 (drop 1 ensures)) ;; oldvals to ensure
        uks (take-nth 3 updates) ;; update keys
        uov (take-nth 3 (drop 1 updates)) ;; oldvals to update
        unv (take-nth 3 (drop 2 updates)) ;; newvals for update
        ks (concat eks uks) ;; all keys to watch while comparing
        ovs (concat eov uov)] ;; all oldvals to compare
    (r/wcar conn (apply r/watch ks))
    (let [cf (conflicts conn ks ovs)]
      (if (seq cf)
        (do (r/wcar conn (r/unwatch))
            cf)
        (if (nil? (multi-exec conn (set-multi! conn uks unv)))
          (if-let [cf (seq (conflicts conn ks ovs))]
            cf
            false) ;; return what changed while trying multi-exec
          true)))))


(defn mcas-or-report
  [conn ensures updates & {:keys [timeout-ms]}]
  (u/with-timeout timeout-ms
    ;; I can also do this in tx with rks as inputs and transaform to inputs as above
    (cas-multi-or-report conn ensures updates)))
    ;;^^ TODO: gotta catch that ::operation-timed-out flag and throw whatever is expected here