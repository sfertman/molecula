(ns molecula.transaction
  (:require
    [molecula.redis :as r])
  (:import
    (clojure.lang IFn ISeq)
    #_(clojure.lang.Util runtimeException)))

(defn ex-set-after-commute [] (IllegalStateException. "Can't set after commute"))
;; TODO: create errors namespace and put all ex-* fns there

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

(defn running? [] (some? *t*))
(defn throw-when-nil-t
  []
  (when (or (nil? *t*) #_(nil? (:info *t*))) ;; I'm not sure why I need info here but will keep the comment in case it does end up this way
    (throw (IllegalStateException. "No transaction running"))))
    ; Note: this should never actually happen and if it does then
    ; something went terribly wrong and I should take a long hard
    ; look at my code!

(defn tconn []
  (throw-when-nil-t)
  (:conn *t*))

(defn tcontains? [op rk]
  (throw-when-nil-t)
  (contains? (*t* op) rk))

(defn tget [& ks]
  (throw-when-nil-t)
  (get-in *t* ks))

(defn oldval [rk] (tget :oldvals rk))
(defn tval [rk] (tget :tvals rk))

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

(defn tput!
  [op & args]
  (throw-when-nil-t)
  (set! *t* (apply (tput-fn op) *t* args)))

(defn deref* [rk] (r/deref* (tconn) rk))
;; this should probably be in molecula.redis

(defn do-get
  [ref]
  (let [rk (.key ref)]
    (if (tcontains? :tvals rk)
      (tval rk)
      (let [redis-val (deref* rk)]
        (tput! :refs ref)
        (tput! :oldvals rk redis-val)
        (tput! :tvals rk redis-val)
        redis-val))))

(defn do-set
  [ref value]
  (let [rk (.key ref)]
    (when (tcontains? :commutes rk)
      (throw (ex-set-after-commute)))
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

(defn commute-ref
  "Applies all ref's commutes starting with ref's oldval"
  [rk]
  (let [cfns (apply comp (tget :commutes rk))]
    (cfns (oldval rk))))

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
    (let [ref (tget :refs rk)]
      (validate* (.getValidator ref) (tval ref)))))

(defn commit
  "Returns:
  - nil if everything went ok
  - an error \"object\" if anything went wrong"
  [retries]
  ;; TODO: this needs to handle the case when there is nothing to commit
  (if (<= retries 0)
    {:error :no-more-retries
     :retries retries}
    (let [ensures (apply concat (map (fn [rk] [rk (oldval rk)]) (tget :ensures)))
          updates (apply concat (map (fn [rk] [rk (oldval rk) (tval rk)]) (updatables)))
          result (r/cas-multi-or-report (tconn) ensures updates)]
      (when-not (true? result)
        (let [rks (map (fn [rk] (tget :commutes rk)) result)]
          (if (some nil? rks)
            {:error :stale-oldvals
             :retries retries}  ;; should be retried in outer loop
            (do
              (doseq [rk rks
                      rv (r/deref-multi (tconn) rks)]
                (tput! :oldvals rk rv)
                (commute-ref rk)) ;; I need to update :oldvals before I can commute
              (recur (dec retries))))))))) ;; should be retried in here

(defn notify-watches
  "Validates all updatables given the latest oldval and latest tval"
  []
  (doseq [rk (updatables)]
    (.notifyWatches (tget :refs rk) (oldval rk) (tval rk))))

(defn dispatch-agents [] 42) ;; TODO: this at some point?

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