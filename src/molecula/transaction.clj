(ns molecula.transaction
  (:require
    [molecula.redis :as r])
  (:import
    (clojure.lang IFn ISeq)
    #_(clojure.lang.Util runtimeException)))

(defn ex-set-after-commute [] (IllegalStateException. "Can't set after commute"))
(defn ex-ref-unbound [ref] (IllegalStateException. (str ref " is unbound.")))
;; TODO: create errors namespace and put all ex-* fns there
(defn ex-retry-limit [] ;; this is a function because I need a NEW exception at the point of failure
  (RuntimeException. "Transaction failed after reaching retry limit"))
;; ^^ should be clojure.lang.Util. runtimeException; having some trouble importing it

(def ^:dynamic *t* nil)

(defrecord CFn [f args]
  IFn
    (invoke [this oldval] (apply (:f this) oldval (:args this))))

;; hmm... most times we need to update more than one key in *t*; could I use a bunch of actual clojure refs for this?
;; Could be pretty fun but not sure if necessary because everything is going to be happening on a single thread
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
    (throw (IllegalStateException. "No transaction running"))))

(defn tconn [] (:conn *t*))

(defn tcontains? [op rk] (contains? (*t* op) rk))

(defn tget [& ks] (get-in *t* ks))

(defn tput-refs
  [t ref] (update t :refs assoc (.key ref) ref))
(defn tput-oldvals
  [t rk oldval] (update t :oldvals assoc rk oldval))
(defn tput-tvals
  [t rk tval] (update t :tvals assoc rk tval))
(defn tput-sets
  [t rk] (update t :sets conj rk))
(defn tput-ensures
  [t rk] (update t :ensures conj rk))
(defn tput-commutes
  [t rk cfn]
  (if (seq (get-in t [:commutes rk]))
    (update-in t [:commutes rk] conj cfn)
    (update t :commutes assoc rk [cfn])))

(def tput-fn {
  :refs     tput-refs
  :oldvals  tput-oldvals
  :tvals    tput-tvals
  :sets     tput-sets
  :ensures  tput-ensures
  :commutes tput-commutes})

(defn tput! [op & args] (set! *t* (apply (tput-fn op) *t* args)))

(defn do-get
  [ref]
  (let [rk (.key ref)]
    (if (tcontains? :tvals rk)
      (tget :tvals rk)
      (let [redis-val (r/deref* (tconn) rk)]
        (when (= {:mol-redis-errr :ref-key-nx} redis-val)
          (throw (ex-ref-unbound ref)))
        redis-val))))

(defn do-set
  [ref value]
  (let [rk (.key ref)]
    (when (tcontains? :commutes rk)
      (throw (ex-set-after-commute)))
    (when-not (tcontains? :refs rk)
      (tput! :refs ref))
    (when-not (tcontains? :oldvals rk)
      (tput! :oldvals rk (do-get ref)))
    (when-not (tcontains? :sets rk)
      (tput! :sets rk))
    (tput! :tvals rk value)
    value))

(defn do-ensure
  [ref]
  (let [rk (.key ref)]
    (when-not (tcontains? :oldvals rk)
      (tput! :oldvals rk (do-get ref)))
    (when-not (tcontains? :ensures rk)
      (tput! :ensures rk))))

(defn do-commute
  [ref f args]
  (let [rk (.key ref)
        cfn (->CFn f args)
        rv (do-get ref)
        ret (cfn rv)]
    (when-not (tcontains? :refs rk)
      (tput! :refs ref))
    (when-not (tcontains? :oldvals rk)
      (tput! :oldvals rk rv))
    (tput! :tvals rk ret)
    (tput! :commutes rk cfn)
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
      (throw (IllegalStateException. "Invalid reference state")))
    (catch RuntimeException re
      (throw re))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state" e)))))

(defn updatables
  "Returns a set of refs that have been altered or commuted"
  [] (apply conj (tget :sets) (keys (tget :commutes))))

(defn validate
  "Validates all updatables given the latest tval"
  []
  (doseq [rk (updatables)]
    (validate* (.getValidator (tget :refs rk)) (tget :tvals rk))))

(defn commit
  "Returns:
  - nil if everything went ok
  - an error \"object\" if anything went wrong"
  [retries]
  ;; TODO: this needs to handle the case when there is nothing to commit
  (if (<= retries 0)
    {:error :no-more-retries
     :retries retries}
    (let [ensures (apply concat (map (fn [rk] [rk (tget :oldvals rk)]) (tget :ensures)))
          updates (apply concat (map (fn [rk] [rk (tget :oldvals rk) (tget :tvals rk)]) (updatables)))
          result (r/cas-multi-or-report (tconn) ensures updates)]
      (when-not (true? result)
        (let [rks (map (fn [rk] (when (tcontains? :commutes rk) rk)) result)]
          (if (or (some nil? rks)
                  (some identity (map (fn [rk] (when (or (tcontains? :ensures rk) (tcontains? :sets rk)) rk) ) rks)))
            {:error :stale-oldvals
             :retries retries}  ;; entire tx needs retry anything outside commute is stale
            (do
              (doseq [rk rks
                      rv (r/deref-multi (tconn) rks)]
                (tput! :oldvals rk rv) ;; refresh oldvals
                (tput! :tvals rk (commute-ref rk))) ;; recalculate commutes only
              (recur (dec retries))))))))) ;; retry commit

(defn notify-watches
  "Validates all updatables given the latest oldval and latest tval"
  []
  (doseq [rk (updatables)]
    (.notifyWatches (tget :refs rk) (tget :oldvals rk) (tget :tvals rk))))

(defn dispatch-agents [] 42) ;; TODO: this at some point?

(defn run ;; TODO
  [^clojure.lang.IFn f]
  (loop [retries 100] ;; TODO: add timeout
    (when (<= retries 0)
      (throw (ex-retry-limit)))
    (let [ret (f)] ;; add a try catch around this thing?
      (validate)
      (let [result (commit retries)]
        (if (nil? result)
          (do
            (notify-watches)
            ret)
        (cond
          (= :no-more-retries (:error result))
            (throw (ex-retry-limit))
          (= :stale-oldvals (:error result))
            (recur (dec retries))))))))

(defn run-in-transaction
  [conn ^clojure.lang.IFn f]
  (if (nil? *t*)
    (binding [*t* (->transaction conn)]
      (run f))
    (run f)))
  ; So, info is to track whether tx is running or committing (also for start point in time) which doesn't seem applicable for optimistic locking in redis