(ns molecula.transaction
  (:require
    [molecula.redis :as r]))

(def ^:dynamic *t* nil)

(defn ->transaction [] {
  :start-val {} ;; the ref value at the beginning of the tx
  :tval {}  ;; It need this to track in-tx values
  :alter #{} ;; I need this to track sets of ref
  ;; For commit time I need to know the value we began the transaction with (first deref ever) and the current value of ref
  ;; During tx time, I need just the val and tval is more than enough
  ;; I could make :alter a map from ref key to old
  :alter {}
  :commute {} ;; not sure how commute is handled at this time
  :ensure #{}}) ;; for ensures I only need the ref key

(defn- throw-when-nil-t
  []
  (when (or (nil? *t*) #_(nil? (:info *t*))) ;; Inot sure why I need info here but will keep the comment in case it does end up this way
    (throw (IllegalStateException. "No transaction running"))))
    ; Note: this should never actually happen and if it does then
    ; something went terribly wrong and I should take a long hard
    ; look at my code!

(defn get-ex [] (throw-when-nil-t) *t*)

(defn tcontains? [op ref] (contains? (get (get-ex) op) ref))

(defn tget
  ([op] (get (get-ex) op))
  ([op ref] (get-in (get-ex) [op ref])))

(defmulti tput* (fn [op & _] op))

(defmethod tput* :start-val
  [_ ref val]
  (set! *t* (update *t* :start-val assoc ref val)))
(defmethod tput* :tval
  [_ ref val]
  (set! *t* (update *t* :tval assoc ref val)))
(defmethod tput* :commute
  [_ ref f args]
  (when (nil? (tget :commute ref))
    (set! *t* (update *t* :commute assoc ref [])))
  (set! *t* (update-in *t* [:commute ref] conj `(~f ~args) )))
(defmethod tput* :ensure
  [_ ref]
  (set! *t* (update *t* :ensure conj ref)))
(defmethod tput* :alter
  [_ ref]
  (set! *t* (update *t* :alter conj ref)))

(defn tput!
  [& args]
  (throw-when-nil-t)
  (apply tput* args))

(defn tput!- ;; could also just pass the redis key of the ref
  [op ref & args]
  (throw-when-nil-t)
  (apply tput* op (.key ref) args))

(defn redis-value ;; rename this to something more intelligent please
  [ref]
  (r/deref* (:conn (.state ref)) (:k (.state ref))))
;; should we add to watch list automagically on deref

(defn do-get
  [ref]
  (if (tcontains? :tval ref)
    (tget :tval ref)
    (do
      (r/watch ref) ;; first we watch on redis! (TODO: implement this)
      (let [value (redis-value ref)] ;; then we get the val from redis
        (tput! :tval ref value) ;; then we set in-tx val of ref
        (tput! :start-val ref value)
        value ;; then we return val
      ))))

(defn do-set
  [ref value]
  (when (tcontains? :commute ref)
    (throw (IllegalStateException. "Can't set after commute")))
  (when (not (tcontains? :alter ref))
    (tput! :alter ref (do-get ref)))
  (tput! :tval ref value)
  value)

(defn do-ensure
  [ref]
  (when-not (tcontains? :ensure ref)
    (let [value (do-get ref)] ;; this watches and gets tval value
      (tput! :ensure ref)
      value)))

(defn do-commute
  [ref f args]
  ;; java throws if no tx "running"
  (when-not (tcontains? :tval ref)
    (tput! :tval ref (redis-value ref)))
  (tput! :commute ref f args)
  (let [ret (apply f (tget :tval ref) args)]
    (tput! :tval ref ret)
    ret))
;; ^ perhaps make a CFn equivalent data structure -- quoted lists can get hairy (or possibly not...). Won't know for sure before implementing runInTransaction

(defn run
  [^clojure.lang.IFn f]
  ;; loop [retry limit < some-max limit and mebbe timers too]
  (let [ret (f)]

    ;; retry exceptions are "eaten" so we can just make a loop with stopping condition here instead and not implement it (it just extends Error)
    (r/compare-and-set-multi*
      (:conn (.state ref))
      watch-list
      set-list)

      ;; ^ something like this will be file for
      ;; alters and ensures but
  )

  42)

(defn run-in-transaction
  [^clojure.lang.IFn f]
  (binding [*t* (->transaction)]
    (run f)))
  ;; there's lots more weird stuff in LockingTransaction class but doesn't seem applicable for now. Let's start simple and see how it goes.

  ; So, info is to track whether tx is running or committing (alos for start point in time)