(ns molecula.redis
  (:require [taoensso.carmine :as r]))

(defn deref* [conn k] (:data (r/wcar conn (r/get k))))

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

(defn compare-and-set-multi*
  "Usage:
  ```
  (compare-and-set-multi
    conn
    [:ktw1 :ktw2 :ktw3] ;; vector of keys to watch;; hmm... what happens if I watch the same key twice?
    :ktm1 oldval1 newval1
    :ktm2 oldval2 newval2
    :ktm3 oldval3 newval3) ;; keys to modify
  ```"
  [conn ks-to-watch & args]
  (let [ks-to-set (take-nth 3 args)
        oldvals (take-nth 3 (drop 1 args))
        newvals (take-nth 3 (drop 2 args))]
    (r/wcar conn (apply r/watch (into #{} (concat ks-to-watch ks-to-set))))
    (if (some (map not= (deref-multi* ks-to-set) oldvals))
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
