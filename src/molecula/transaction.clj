(ns molecula.transaction
  (:require
    [molecula.redis :as r])
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

(defn tcontains? [op rk] (contains? (get (get-ex) op) rk))

(defn tget*
  ([t op] (get t op))
  ([t op rk] (get-in t [op rk])))

(defn tget [& args]
  (throw-when-nil-t)
  (apply tget* *t* args))

(defn- get-ref [rk] (get-in (get-ex) [:refs rk]))
; (defn- put-ref [ref]
;   (throw-when-nil-t)
;   (when-not (tcontains? :refs (.key ref))
;     (set! *t* (update *t* :refs assoc (.key ref) ref))))

(defn- oldval [rk] (tget :oldvals rk))
(defn- tval [rk] (tget :tvals rk))

(defmulti tput* (fn [op & _] op))

(defmethod tput* :refs
  ([_ ref] (tput* :refs (.key ref) ref))
  ([_ rk ref] (set! *t* (update *t* :refs assoc rk ref))))

(defmethod tput* :oldvals
  [_ rk val]
  (set! *t* (update *t* :oldvals assoc rk val)))

(defmethod tput* :tvals
  [_ rk val]
  (set! *t* (update *t* :tvals assoc rk val)))

(defmethod tput* :commutes
  [_ rk cfn]
  (when (nil? (tget :commutes rk))
    (set! *t* (update *t* :commutes assoc rk [])))
  (set! *t* (update-in *t* [:commutes rk] conj cfn)))

(defmethod tput* :ensures
  [_ rk]
  (set! *t* (update *t* :ensures conj rk)))

(defmethod tput* :sets
  [_ rk]
  (set! *t* (update *t* :sets conj rk)))

(defn tput!
  [op rk & args]
  (throw-when-nil-t)
  (apply tput* op rk args))

(defn deref* [rk] (r/deref* (:conn *t*) rk ))
;; this should probably be in molecula.redis

(defn do-get
  [ref]
  (let [rk (.key ref)]
    (if (tcontains? :tvals rk)
      (tval ref)
      (let [redis-val (deref* rk)]
        (tput! :refs ref)
        (tput! :oldvals rk redis-val)
        (tput! :tvals rk redis-val)
        redis-val))))

(defn do-set
  [ref value]
  (let [rk (.key ref)]
    (when (tcontains? :commutes rk)
      (throw (IllegalStateException. "Can't set after commute")))
    (when-not (tcontains? :sets rk)
      (tput! :sets rk))
    (do-get ref) ;; adds ref to oldval if not already there
    (tput! :tvals rk value) ;; put new val in tvals
    value))

(defn do-ensure
  [ref]
  (let [rk (.key ref)]
    (when-not (tcontains? :ensures rk)
      (let [value (do-get ref)]
        (tput! :ensures rk)
        value))))

(defn do-commute
  [ref f args]
  (let [rk (.key ref)
        cfn (->CFn f args)
        ret (cfn (do-get ref))]
    (tput! :tvals rk ret)
    (tput! :commutes rk cfn)
    ret))

(defn- commute-ref
  "Applies all ref's commutes starting with ref's oldval"
  [rk]
  (let [cfns (apply comp (tget :commutes rk))]
    (cfns (oldval rk))))

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
  (doseq [rk (updatables)]
    (let [ref (get-ref rk)]
      (validate* (.getValidator ref) (tval ref)))))

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
    (let [ensures (apply concat (map (fn [rk] [rk (oldval rk)]) (tget :ensures)))
          updates (apply concat (map (fn [rk] [rk (oldval rk) (tval rk)]) (updatables)))
          result (r/cas-multi-or-report (:conn *t*) ensures updates)]
      (when-not (true? result)
        (prn "result")
        (clojure.pprint/pprint result)
        (let [rks (map (fn [rk] (tget :commutes rk)) result)]
          (prn "stale?")
          (clojure.pprint/pprint rks)
          (if (some nil? rks)
            {:error :stale-oldvals
             :retries retries}  ;; should be retried in outer loop
            (do
              (doseq [rk rks]
                (commute-ref rk))
              (recur (dec retries))))))))) ;; should be retried in here

(defn- notify-watches
  "Validates all updatables given the latest oldval and latest tval"
  []
  (doseq [rk (updatables)]
    (.notifyWatches rk (oldval rk) (tval rk))))

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
      (prn "ran in transaction; retries = " retries )
      (prn "return val is:")
      (clojure.pprint/pprint ret)
      (prn "transaction is:")
      (clojure.pprint/pprint *t*)

      (validate)
      ; (prn "validated")
      (let [result (commit retries)]
        ; (prn "commited")
        ; (prn "return val is:")
        ; (clojure.pprint/pprint result)
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