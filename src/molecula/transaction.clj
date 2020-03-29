(ns molecula.transaction
  (:require
    [molecula.redis :as r]
    [molecula.util :as u])
  (:import
    (clojure.lang IFn ISeq)
    #_(clojure.lang.Util runtimeException)))

(def ^:dynamic *t* nil)

(defrecord CFn [f args]
  IFn
    (invoke [this oldval] (apply (:f this) oldval (:args this))))

; (defn ->cfn [^clojure.lang.IFn f ^clojure.lang.ISeq args] (->CFn f args))

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

(defn- throw-when-nil-t
  []
  (when (or (nil? *t*) #_(nil? (:info *t*))) ;; I'm not sure why I need info here but will keep the comment in case it does end up this way
    (throw (IllegalStateException. "No transaction running"))))
    ; Note: this should never actually happen and if it does then
    ; something went terribly wrong and I should take a long hard
    ; look at my code!

(defn get-ex [] (throw-when-nil-t) *t*)

(defn tcontains? [op ref] (contains? (get (get-ex) op) ref))

(defn tget
  ([op] (get (get-ex) op))
  ([op ref] (get-in (get-ex) [op ref]))) ;; <- I hope I don't have to implement comparable for this...

(defn- oldval [ref] (tget :oldvals ref))
(defn- tval [ref] (tget :tvals ref))

(defmulti tput* (fn [op & _] op))

(defmethod tput* :refs
  [_ ref]
  (set! *t* (update *t* :refs assoc (.key ref) ref)))

(defmethod tput* :oldvals
  [_ ref val]
  (set! *t* (update *t* :oldvals assoc ref val)))

(defmethod tput* :tvals
  [_ ref val]
  (set! *t* (update *t* :tvals assoc ref val)))

(defmethod tput* :commutes
  [_ ref cfn]
  (when (nil? (tget :commutes ref))
    (set! *t* (update *t* :commutes assoc ref [])))
  (set! *t* (update-in *t* [:commutes ref] conj cfn)))

(defmethod tput* :ensures
  [_ ref]
  (set! *t* (update *t* :ensures conj ref)))

(defmethod tput* :sets
  [_ ref]
  (set! *t* (update *t* :sets conj ref)))

(defn tput!
  [op ref & args]
  (throw-when-nil-t)
  (when-not (tcontains? :refs (.key ref))
    (tput* :refs ref))
  (apply tput* op ref args))

(defn do-get
  [ref]
  (if (tcontains? :tvals ref)
    (tval ref)
    (let [redis-val (u/deref* ref)]
      (tput! :oldvals ref redis-val)
      (tput! :tvals ref redis-val)
      redis-val)))

(defn do-set
  [ref value]
  (when (tcontains? :commutes ref)
    (throw (IllegalStateException. "Can't set after commute")))
  (when-not (tcontains? :sets ref)
    (tput! :sets ref))
  (do-get ref) ;; adds ref to oldval if not already there
  (tput! :tvals ref value) ;; put new val in tvals
  value)

(defn do-ensure
  [ref]
  (when-not (tcontains? :ensures ref)
    (let [value (do-get ref)]
      (tput! :ensures ref)
      value)))

(defn do-commute
  [ref f args]
  (let [cfn (->CFn f args)
        ret (cfn (do-get ref))]
    (tput! :tvals ref ret)
    (tput! :commutes ref cfn)
    ret))

(defn- commute-ref
  "Applies all ref's commutes starting with ref's oldval"
  [ref]
  (let [cfns (apply comp (tget :commutes ref))]
    (cfns  (oldval ref))))

(defn- validate*
  "This is a clojure re-implementation of clojure.lang.ARef/validate because it cannot be accessed by subclasses. It is needed to invoke when changing ref state"
  [^clojure.lang.IFn vf val]
  (try
    (if (and (some? vf) (not (vf val)))
      (throw (IllegalStateException. "Invalid reference state")))
    (catch RuntimeException re
      (throw re))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state" e)))))

(defn- updatables
  "Returns a set of refs that have been altered or commuted"
  [] (apply conj (tget :sets) (keys (tget :commutes))))

(defn- validate
  "Validates all updatables given the latest tval"
  []
  (doseq [ref (updatables)]
    (validate* (.getValidator ref) (tval ref))))

(defn- commit
  "Returns:
  - nil if everything went ok
  - an error \"object\" if anything went wrong"
  [retries]
  ;; needs to syncronize number of retries with outter loop
  ;; it is possible that it's better to do everything in (run) for simplicity
  ;; problem is that it's going to be a mega function and I kind of want to avoid that
  (if (<= retries 0)
    {:error :no-more-retries
     :retries retries}
    (let [ensures (apply concat (map (fn [ref] [(.key ref) (oldval ref)]) (tget :ensures)))
          updates (apply concat (map (fn [ref] [(.key ref) (oldval ref) (tval ref)]) (updatables)))
          result (r/cas-multi-or-report (:conn *t*) ensures updates)]
      (when-not (true? result)
        (let [refs (map (fn [rk] (tget :commutes (tget :refs rk))) result)]
          (if (some nil? refs)
            {:error :stale-oldvals
             :retries retries}  ;; should be retried in outer loop
            (do
              (doseq [ref refs]
                (commute-ref ref))
              (recur (dec retries))))))))) ;; should be retried in here

(defn- notify-watches
  "Validates all updatables given the latest oldval and latest tval"
  []
  (doseq [ref (updatables)]
    (.notifyWatches ref (oldval ref) (tval ref))))

(defn- dispatch-agents [] 42) ;; TODO: this at some point?

(defn ex-retry-limit [] ;; this is a function because I need a NEW exception at the point of failure
  (RuntimeException. "Transaction failed after reaching retry limit"))
;; ^^ should be clojure.lang.Util. runtimeException; having some trouble importing it

(defn run ;; TODO this next
  [^clojure.lang.IFn f]
  ;; loop [retry limit < some-max limit and mebbe timers too]
  (loop [retries 100]
    (when (<= retries 0)
      (throw (ex-retry-limit)))

    (let [ret (f)] ;;  add a try catch arpund this thing
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
  (binding [*t* (->transaction conn)]
    (run f)))
  ;; there's lots more weird stuff in LockingTransaction class but doesn't seem applicable for now. Let's start simple and see how it goes.

  ; So, info is to track whether tx is running or committing (also for start point in time)