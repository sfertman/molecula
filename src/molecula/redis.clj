(ns molecula.redis
  (:require [taoensso.carmine :as r]))

(defn watch [conn k]
  (r/wcar conn (r/watch k)))

(defn deref* [conn k] (:data (r/wcar conn (r/get k))))

(defn setnx* [conn k newval] (r/wcar conn (r/setnx k {:data newval})))

(defn deref-multi* [conn ks] (map :data (r/wcar conn (apply r/mget ks))))
;; Note: watch out for LazySeq
;; don't really need this in a ref transaction -- need only for cas-multi

(defn compare-and-set* [conn k oldval newval]
  ;; I need cas to know which values to watch for changes....
  (r/wcar conn (r/watch k))
  (if (not= oldval (deref* conn k))
    (do (r/wcar conn (r/unwatch))
        false)
    (some? (r/wcar conn
                   (r/multi)
                   (r/set k {:data newval})
                   (r/exec)))))


(defn time* [conn]
;; doesn't seem like I actually have a need to do this with refs
  (let [t (r/wcar conn (r/time))]
    (+ (* (Long/parseLong (first  t)) 1.0)
       (/ (Long/parseLong (second t)) 1E+6))))

;; cas with timestamp
(defn cas [conn k oldval newval]
;; doesn't seem like I actually have a need to do this with refs
  (let [t (time* conn)]
    (r/wcar conn (r/watch k))
    (if (not= oldval (deref* conn k))
      (do (r/wcar conn (r/unwatch))
          false)
      (some? (r/wcar conn
                     (r/multi)
                     (r/set k {:data newval :updated-at t})
                     (r/exec))))))



(defn compare-and-set-multi*
;; doesn't seem like I actually have a need to do this with refs

  "Usage:
  ```
  (compare-and-set-multi
    conn
    [:ktw1 :ktw2 :ktw3] ;; vector of keys to watch
    :ktm1 oldval1 newval1
    :ktm2 oldval2 newval2
    :ktm3 oldval3 newval3) ;; keys to modify
  ```"
  [conn ks-to-watch & args]
  (let [ks-to-set (take-nth 3 args)
        oldvals (take-nth 3 (drop 1 args))
        newvals (take-nth 3 (drop 2 args))]
    (r/wcar conn (apply r/watch (into #{} (concat ks-to-watch ks-to-set))))
    (if (some true? (map not= (deref-multi* conn ks-to-set) oldvals))
      ;; ^^ if any of the keys to set don't match oldvals
      (do (r/wcar conn (r/unwatch))
          false)
      (some?
        (r/wcar conn
                (r/multi)
                (doseq [k ks-to-set
                        newval newvals]
                  (r/set k {:data newval}))
                (r/exec))))))



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
  [conn ensures updates & {:keys [timeout]}]
  (let [eks (take-nth 3 ensures) ;; ensure keys
        eov (take-nth 3 (drop 1 ensures)) ;; oldvals to ensure
        uks (take-nth 3 updates) ;; update keys
        uov (take-nth 3 (drop 1 updates)) ;; oldvals to update
        unv (take-nth 3 (drop 2 updates)) ;; newvals for update
        ks (concat eks uks) ;; all keys to watch while comparing
        ovs (concat eov uov)] ;; all oldvals to compare
    (r/wcar conn (apply r/watch ks))
    (let [compare-fails (map not= (deref-multi* conn ks) ovs)]
      (if (seq compare-fails)
        (do (r/wcar conn (r/unwatch))
            compare-fails)
        (some?
          (r/wcar conn
                  (r/multi)
                  (doseq [k uks
                          nv unv]
                    (r/set k {:data nv}))
                  (r/exec)))))))
