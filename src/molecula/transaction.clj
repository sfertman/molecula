(ns molecula.transaction
  (:require
    [molecula.error :as ex]
    [molecula.redis :as r]
    [molecula.util :as u])
  (:import
    (clojure.lang IFn ISeq)))

(def RETRY_LIMIT 10000)
(def MCAS_TIMEOUT 1000)

(def ^:dynamic *t* nil)

(defrecord CFn [f args]
  IFn
    (invoke [this oldval] (apply (.f this) oldval (.args this))))

(defn ->transaction [conn] {
  :conn conn
  :refs {}
  :oldvals {}
  :tvals {}
  :sets #{}
  :commutes {}
  :ensures #{}
})

(defn running? [] (some? *t*))
(defn throw-when-nil-t
  "Throws IllegalStateException if *t* is nil. Returns nil otherwise.
  This is equivalent to clojure.lang.LockingTransaction/getEx()
  Used in RedisRef to guard methods from being called outside
  transaction."
  []
  (when (nil? *t*)
    (throw (ex/no-transaction))))

(defmacro tconn [] (:conn *t*))

(defn tcontains? [op rk] (contains? (*t* op) rk))

(defn tget [& ks] (get-in *t* ks))

(defmacro tput-ref!
  [ref] `(set! *t* (update *t* :refs assoc (.key ~ref) ~ref)))
(defmacro tput-oldval!
  [rk oldval] `(set! *t* (update *t* :oldvals assoc ~rk ~oldval)))
(defmacro tput-tval!
  [rk tval] `(set! *t* (update *t* :tvals assoc ~rk ~tval)))
(defmacro tput-set!
  [rk] `(set! *t* (update *t* :sets conj ~rk)))
(defmacro tput-ensure!
  [rk] `(set! *t* (update *t* :ensures conj ~rk)))
(defmacro tput-commute!
  [rk cfn]
  `(set! *t*
    (if (seq (get-in *t* [:commutes ~rk]))
      (update-in *t* [:commutes ~rk] conj ~cfn)
      (update *t* :commutes assoc ~rk [~cfn]))))

(defn do-get
  [ref]
  (let [rk (.key ref)]
    (if (tcontains? :tvals rk)
      (tget :tvals rk)
      (let [redis-val (r/deref* (tconn) rk)]
        (when (= {:mol-redis-err :ref-key-nx} redis-val)
          (throw (ex/ref-unbound ref)))
        redis-val))))

(defn do-set
  [ref value]
  (let [rk (.key ref)]
    (when (tcontains? :commutes rk)
      (throw (ex/set-after-commute)))
    (tput-ref! ref)
    (when-not (tcontains? :oldvals rk)
      (tput-oldval! rk (do-get ref)))
    (tput-set! rk)
    (tput-tval! rk value)
    value))

(defn do-ensure
  [ref]
  (let [rk (.key ref)]
    (when-not (tcontains? :oldvals rk)
      (tput-oldval! rk (do-get ref)))
    (tput-ensure! rk)))

(defn do-commute
  [ref f args]
  (let [rk (.key ref)
        cfn (->CFn f args)
        rv (do-get ref)
        ret (cfn rv)]
    (tput-ref! ref)
    (when-not (tcontains? :oldvals rk)
      (tput-oldval! rk rv))
    (tput-tval! rk ret)
    (tput-commute! rk cfn)
    ret))

(defn commute-ref
  "Applies all ref's commutes starting with ref's oldval"
  [rk]
  (let [cfns (apply comp (tget :commutes rk))]
    (cfns (tget :oldvals rk))))

(defn validate*
  "This is a clojure re-implementation of clojure.lang.ARef/validate because it cannot be accessed by subclasses. It is needed to invoke when changing ref state"
  [^clojure.lang.IFn vf val]
  (try
    (if (and (some? vf) (not (vf val)))
      (throw (ex/invalid-ref-state)))
    (catch RuntimeException re
      (throw re))
    (catch Exception e
      (throw (ex/invalid-ref-state e)))))

(defn updatables
  "Returns a set of refs that have been altered or commuted"
  [] (apply conj (tget :sets) (keys (tget :commutes))))

(defn validate
  "Validates all updatables given the latest tval"
  ([] (validate (updatables)))
  ([rks]
    (doseq [rk rks]
      (validate* (.getValidator (tget :refs rk)) (tget :tvals rk)))))

(defn commit
  "Returns:
  - nil if everything went ok
  - an error \"object\" if anything went wrong"
  [retries]
  (if (<= retries 0)
    {:error :no-more-retries
     :retries retries}
    (let [ensures (apply concat (map (fn [rk] [rk (tget :oldvals rk)]) (tget :ensures)))
          updates (apply concat (map (fn [rk] [rk (tget :oldvals rk) (tget :tvals rk)]) (updatables)))
          result (u/with-timeout MCAS_TIMEOUT (r/mcas-or-report! (tconn) ensures updates))]
          ;; TODO: handle :operation-timed-out result
          ;; TODO: handle false result -- maybe crash?? this really should never happen
          ;; TODO: maybe find better name for this result
      (when-not (true? result)
        (if (seq (filter #(or (tcontains? :sets %)
                              (tcontains? :ensures %)) result))
          {:error :stale-oldvals
           :retries retries}  ;; entire tx needs retry anything outside commute is stale
          (do ;; else, assume all conflicts are commutes
            (doseq [rk result
                    rv (r/deref-multi (tconn) result)]
              (tput-oldval! rk rv) ;; refresh oldvals
              (tput-tval! rk (commute-ref rk))) ;; recalculate commutes only
            (validate result)
            (recur (dec retries)))))))) ;; retry commit

(defn notify-watches
  "Notifies watches on all updatables given the latest oldval and latest tval"
  ([] (notify-watches (updatables)))
  ([rks]
    (doseq [rk rks]
      (.notifyWatches (tget :refs rk) (tget :oldvals rk) (tget :tvals rk)))))

(defn dispatch-agents [] 42) ;; TODO: this at some point?

(defn run ;; TODO
  [^clojure.lang.IFn f]
  (loop [retries RETRY_LIMIT] ;; TODO: add timeout
    (when (<= retries 0)
      (throw (ex/retry-limit)))
    (let [ret (f)]
      (validate)
      (let [result (commit retries)]
        (if (nil? result)
          (do
            (notify-watches)
            ret)
          (cond
            (= :no-more-retries (:error result))
              (throw (ex/retry-limit))
            (= :stale-oldvals (:error result))
              (recur (dec (:retries result)))))))))

(defn run-in-transaction
  [conn ^clojure.lang.IFn f]
  (if (nil? *t*)
    (binding [*t* (->transaction conn)]
      (run f))
    (run f)))
  ; So, info is to track whether tx is running or committing (also for start point in time) which doesn't seem applicable for optimistic locking in redis