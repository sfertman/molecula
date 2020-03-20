(ns molecula.transaction
  (:require
    [molecula.redis :as r]
    [molecula.util :as u])
  (:import
    (clojure.lang IFn ISeq)
    (clojure.lang.Util runtimeException)))

(def ^:dynamic *t* nil)

(defrecord CFn [f args])
(defn ->cfn [^clojure.lang.IFn f ^clojure.lang.ISeq args] (->CFn f args))


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

(defn tcontains? [op ref] (contains? (get (get-ex) op) (.key ref)))

(defn tget
  ([op] (get (get-ex) op))
  ([op ref] (get-in (get-ex) [op (.key ref)])))

(defmulti tput* (fn [op & _] op))

(defmethod tput* :oldvals
  [_ ref val]
  (set! *t* (update *t* :oldvals assoc (.key ref) val)))

(defmethod tput* :tvals
  [_ ref val]
  (set! *t* (update *t* :tvals assoc (.key ref) val)))

(defmethod tput* :commutes
  [_ ref f args]
  (when (nil? (tget :commutes ref))
    (set! *t* (update *t* :commutes assoc (.key ref) [])))
  (set! *t* (update-in *t* [:commutes (.key ref)] conj (->cfn f args))))

(defmethod tput* :ensures
  [_ ref]
  (set! *t* (update *t* :ensures conj (.key ref))))

(defmethod tput* :sets
  [_ ref]
  (set! *t* (update *t* :sets conj (.key ref))))

(defn tput!
  [& args]
  (throw-when-nil-t)
  (apply tput* args))

(defn do-get
  [ref]
  (if (tcontains? :tvals ref)
    (tget :tvals ref)
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
  (let [ret (apply f (do-get ref) args)]
    (tput! :tvals ref ret)
    (tput! :commutes ref f args)
    ret))

(defn run ;; TODO this next
  [^clojure.lang.IFn f]
  ;; loop [retry limit < some-max limit and mebbe timers too]
  (loop [retries 0]
    (when (< 100 retries) ;; <- some retry limit
      (throw (runtimeException. "Transaction failed after reaching retry limit"))
    ))
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