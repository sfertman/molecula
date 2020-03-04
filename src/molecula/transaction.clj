(ns molecula.transaction
  (:require
    [molecula.redis :as r]))

(def ^:dynamic *t* nil)

(defn ->transaction [] {:tval {} :alter #{} :commute {} :ensure #{}})

(defn get-ex
  "Returns a transaction object (*t*).
  Used inside a \"running\" transaction.
  If *t* is nil then throws an exception."
  []
  (if (or (nil? *t*) #_(nil? (:info *t*))) ;; Inot sure why I need info here but will keep the comment in case it does end up this way
    (throw (IllegalStateException. "No transaction running"))
    ; Note: this should never actually happen and if it does then
    ; something went terribly wrong and I should take a long hard
    ; look at my code!
    *t*))

(defn tcontains? [op ref] (contains? (get (get-ex) op) ref))

(defn tget
  ([op] (get (get-ex) op))
  ([op ref] (get-in (get-ex) [op ref])))

(defmulti tput! (fn [op & _] op))

(defmethod tput! :tval
  [_ ref val]
  (set! *t* (update (get-ex) :tval assoc ref val)))
(defmethod tput! :commute
  [_ ref f args]
  (when (nil? (tget :commute ref))
    (set! *t* (update (get-ex) :commutes assoc ref [])))
  (set! *t* (update-in (get-ex) [:commutes ref] conj `(~f ~args) )))
(defmethod tput! :ensure
  [_ ref]
  (set! *t* (update (get-ex) :ensure conj ref)))
(defmethod tput! :alter
  [_ ref]
  (set! *t* (update (get-ex) :alter conj ref)))

(defn redis-value ;; rename this to something more intelligent please
  [ref]
  (r/deref* (:conn (.state ref)) (:k (.state ref))))
;; should we add to watch list automagically on deref

(defn do-get
  [ref]
  (if (tcontains? :tval ref)
    (tget :tval ref)
    (do
      ; (r/watch this)
      ;; ~~first we watch on redis! (TODO: implement this)~~
      ;; Actually we do not need to watch anything before commit time.
      ;; The transaction is runnig on local only and once everythign is
      ;; prepared to be commited to Redis then and only then do we have
      ;; to watch for changes while performing cas-multi!
      ;; The above stmt only needs to save dereffed val as tval
      (let [value (redis-value ref)] ;; then we get the val from redis
        (tput! :tval ref value) ;; then we set in-tx val of ref
        value ;; then we return val
      ))))

(defn do-set
  [ref value]
  (when (tcontains? :commute ref)
    (throw (IllegalStateException. "Can't set after commute")))
  (when (not (tcontains? :alter ref))
    (tput! :alter ref))
  (tput! :tval ref value)
  value)

(defn do-ensure
  [ref]
  (when-not (tcontains? :ensure ref)
    (let [value (do-get ref)] ;; this watches and gets in-tx value
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
  [f]
  42)

(defn run-in-transaction
  [f]
  42)