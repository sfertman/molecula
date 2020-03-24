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
  [& args]
  (throw-when-nil-t)
  (apply tput* args))

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
  - an error \"object\" if anything went wrong
    -"
  []
  (let [ensures (apply concat (map (fn [ref] [(.key ref) (oldval ref)]) (tget :ensures)))
        updates (apply concat (map (fn [ref] [(.key ref) (oldval ref) (tval ref)]]) (updatables)))
        result (r/cas-multi-or-report (:conn *t*) ensures updates)]
    (cond
      (true? result) 42
      (false? result) 43
      (sequential? result)
        (do

          54) ;; figure out what needs to be done when commit fails like that

      )
    ))

(defn- notify-watches
  "Validates all updatables given the latest oldval and latest tval"
  []
  (doseq [ref (updatables)]
    (.notifyWatches ref (oldval ref) (tval ref))))

(defn- dispatch-agents [] 42) ;; TODO: this at some point?

(defn run ;; TODO this next
  [^clojure.lang.IFn f]
  ;; loop [retry limit < some-max limit and mebbe timers too]
  (loop [retries 0]
    (when (< 100 retries) ;; <- some retry limit
      (throw (RuntimeException. "Transaction failed after reaching retry limit")))
              ;; ^^ should be clojure.lang.Util. runtimeException; havong some trouble importing it
    (let [ret (f) refs [] ]
      ;; handle commutes
      ;; handle sets
      ;; collect all refs that need to be updated on backend

      (validate)
      ;; commit
      #_(let [ensures 42
            updates 43
            result (r/cas-multi-or-report (:conn *t*) ensures updates)]
        (cond
          (true? result) ret
          (false? result) (recur (dec retries))
            ;; ^^ can I actually figure out what exactly went wrong here?
            ;; if only commutes got out of sync, I could try again cheaply
          (sequential? result)
            54 ;; figure out what needs to be done when commit fails like that

          )
        )
      (if (commit refs)
        (do
          ; (notify-watches-all refs)
          (notify-watches)
        ;; dispatch agents
        )
        ;; figure out if we can get away with updating commute only
        ;; there's going to be an inner loop here that will add to retry count above. Possible "inner" loop should include "(if cas..."
        ;; Also must make sure to clean up *t* before recurring to top level because f will be called again
      )

    )


      )
  (let [ret (f)]
        ;; ^^ THROWS: clojure.lang.Var$Unbound cannot be cast to java.util.concurrent.Future
    (prn "ret" ret)
    ;; retry exceptions are "eaten" so we can just make a loop with stopping condition here instead and not implement it (it just extends Error)
    (r/cas-multi-or-report
      (:conn *t*)
      (tget :ensures)
      (tget :sets))

      ;; ^ something like this will be file for
      ;; alters and ensures but commutes ned other type of handling
    ret
  )

  )

(defn run-in-transaction
  [conn ^clojure.lang.IFn f]
  (binding [*t* (->transaction conn)]
    (run f)))
  ;; there's lots more weird stuff in LockingTransaction class but doesn't seem applicable for now. Let's start simple and see how it goes.

  ; So, info is to track whether tx is running or committing (also for start point in time)