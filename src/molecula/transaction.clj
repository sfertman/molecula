(ns molecula.transaction
  (:require
    [molecula.redis :as r])
  (:import
    (clojure.lang IFn ISeq)))

(def ^:dynamic *t* nil)

(defrecord CFn [f args])
(defn ->cfn [^clojure.lang.IFn f ^clojure.lang.ISeq args] (->CFn f args))


(defn ->transaction [conn] {
  :oldval {} ;; the ref value when it is first touched inside the tx
  :tval {}  ;; It need this to track in-tx values
  ; :alter #{} ;; I need this to track sets of ref
  ;; For commit time I need to know the value we began the transaction with (first deref ever) and the current value of ref
  ;; During tx time, I need just the val and tval is more than enough
  ;; I could make :alter a map from ref key to old
  :alter {} ;; since :oldval tracks the initial value of ref and :tval tracks the current in-tx value, we might not actually need alter to be more than a set
  :commute {} ;; not sure how commute is handled at this time
  :ensure #{}} ;; this should be a map
  :conn conn ;; constructor has to accept a conn arg
  ) ;; for ensures I only need the ref key

; (defn ->transaction
;   [conn]
;   )
;; another option for defining a transacrion object
; {
;   :ref1 {
;     :oldval 42
;     :tval 43
;     :op :alter
;   }
; }

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

(defmethod tput* :oldval
  [_ ref val]
  (set! *t* (update *t* :oldval assoc ref val)))
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
  [_ ref val]
  (set! *t* (update *t* :alter assoc ref val)))

(defn tput!
  [& args]
  (throw-when-nil-t)
  (apply tput* args))

; (defn tput!- ;; could also just pass the redis key of the ref
;   [op ref & args]
;   (throw-when-nil-t)
;   (apply tput* op (.key ref) args))

(defn redis-value ;; rename this to something more intelligent please
  [ref]
  (r/deref* (:conn (.state ref)) (:k (.state ref))))
;; should we add to watch list automagically on deref

(defn do-get
  [ref]
  (if (tcontains? :tval ref)
    (tget :tval ref)
    (do
      (r/watch (:conn (.state ref)) (:k (.state ref))) ;; first we watch on redis! (TODO: implement this)
      (let [value (redis-value ref)] ;; then we get the val from redis
        (tput! :tval ref value) ;; then we set in-tx val of ref
        (tput! :oldval ref value)
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
(comment
;; I need to be able to transalate this:
{
  :ensure {
    :kte1 oldval11
    :kte2 oldval12
    ;;...
  }
  :alter {
    :ktu1 [oldval21 newval21]
    :ktu2 [oldval22 newval22]
    ;...
  }
  :commute {
    :ktu3 [fn31 fn32 fn33]
    ; ...
  }
  ;; so commutes should also keep the original oldval for cas at commit time
  ;; but also store the functions (+ args) that are applied so that we can
  ;; retry if cas fails
}

;; to this:
[[:kte1 oldval11
  :kte2 oldval12]
 [:ktu1 oldval21 newvall21
  :ktu2 oldval22 newvall22
  :ktu3 oldval31 newvall31]]

; or perhaps this?
[{:kte1 oldval11
  :kte2 oldval12}
 [:ktu1 [oldval21 newvall21]
  :ktu2 [oldval22 newvall22]
  :ktu3 [oldval31 newvall31]]]

; commute (as always) is a problem
; ensures just keep the oldval for reference at commit time
; alters do the same
; commutes are simply functions applied at commit time so it difficult to figure out where these fns should be applied. CAS doesn't seem like the right place


)

(defn run
  [^clojure.lang.IFn f]
  ;; loop [retry limit < some-max limit and mebbe timers too]
  (let [ret (try (f) (catch Throwable e (prn e)))]
        ;; ^^ THROWS: clojure.lang.Var$Unbound cannot be cast to java.util.concurrent.Future
    (prn "ret" ret)
    ;; retry exceptions are "eaten" so we can just make a loop with stopping condition here instead and not implement it (it just extends Error)
    (r/cas-multi-or-report
      (:conn *t*)
      (tget :ensure)
      (tget :alter))

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

  ; So, info is to track whether tx is running or committing (alos for start point in time)