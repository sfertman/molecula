(ns molecula.transaction
  (:require
    [molecula.redis :as r]))

(def ^:dynamic *t* nil)

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

(defn ensures [] (:ensures (get-ex)))
(defn commutes [] (:commutes (get-ex)))
(defn sets [] (:sets (get-ex)))


(defn in-vals? [this] (contains? (:vals (get-ex)) this))
(defn tval [this] (get (:vals (get-ex)) this))
(defn put-tval [this v]
  (set *t* (update (get-ex) :vals assoc this v)))
(defn in-commutes? [this] (contains? (commutes) this))


(defn set-tx-val
  [this val]
  (set! *t* (update (get-ex) :vals assoc this val)))

(defn add-to-commutes
  [this val-mabbe?]
  (set ))


(defn in-sets? [this] (contains? (sets) this))


(defn add-to-tx-sets
  [this]
  (set! *t* (update (get-ex) :sets conj this)))


(defn in-ensures?
  [this]
  (contains? (ensures) this))

(defn add-to-ensures
  [this]
  (set! *t* (update (get-ex) :ensures conj this)))




(defn do-get
  [this]
  (if (in-vals? this)
    (tval this)
    (do
      (r/watch this)
      ;; ~~first we watch on redis! (TODO: implement this)~~
      ;; Actually we do not need to watch anything before commit time.
      ;; The transaction is runnig on local only and once everythign is
      ;; prepared to be commited to Redis then and only then do we have
      ;; to watch for changes while performing cas-multi!
      ;; The above stmt only needs to save dereffed val as tval
      (let [val (current-val this)] ;; then we get the al from redis
        (set-tx-val this val) ;; then we set in-tx val of ref
        val ;; then we return val
      ))))

(defn do-set
  [this val]
  (when (in-commutes? this)
    (throw (IllegalStateException. "Can't set after commute")))
  (when (not (in-sets? this))
    (add-to-tx-sets this))
  (set-tx-val this val)
  val)

(defn do-ensure
  [this]
  (when-not (in-ensures? this)
    (let [val (do-get this)] ;; this watches and gets in-tx value
      (add-to-ensures this)
      val)))

(defn do-commute
; Object doCommute(Ref ref, IFn fn, ISeq args) {
; 	if(!info.running())
; 		throw retryex;
; 	if(!vals.containsKey(ref))
; 		{
; 		Object val = null;
; 		try
; 			{
; 			ref.lock.readLock().lock();
; 			val = ref.tvals == null ? null : ref.tvals.val;
; 			}
; 		finally
; 			{
; 			ref.lock.readLock().unlock();
; 			}
; 		vals.put(ref, val);
; 		}
; 	ArrayList<CFn> fns = commutes.get(ref);
; 	if(fns == null)
; 		commutes.put(ref, fns = new ArrayList<CFn>());
; 	fns.add(new CFn(fn, args));
; 	Object ret = fn.applyTo(RT.cons(vals.get(ref), args));
; 	vals.put(ref, ret);
; 	return ret;
; }
  [this f args]

  (when (nil? (get (commutes) this))
    (set! *t* (update (get-ex) :commutes assoc this [])))
  (set! *t* (update-in (get-ex) [:commutes this] conj `(~f ~args) ))
  ;; ^ make a CFn equivalent data structure -- quoted lists can get hairy (or possibly not...). Won;t know for sure before implementing runInTransaction
  (let [ret (apply f (tval this) args)]
    (put-tval this ret)
    ret)
)



(defn run-in-transaction
  [f]
  42)