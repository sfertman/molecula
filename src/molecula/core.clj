(ns molecula.core
  (:refer-clojure :exclude [dosync])
  (:require
    [taoensso.carmine :as r]))
;; let's figure out the api for this thing...
(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})

(defn molecula
  [& atoms]
  42)
;; I wonder if I can make molecula work with any atom and not necessarily redis atom...
;; This way, redis atom use case would be trivial since it implements IAtom2
; hard to say if it's possible; my desired interface allows in principle to do something like that but there is a practical problem watching clojure atoms because I will prbably hae to implement the entire interface in clojure to be able to make a molecular swap.
; Which actually night not be that big of a deal but still kind of annoying

(defn deref* [conn k] (:data (r/wcar conn (r/get k))))
(defn compare-and-set* [conn k oldval newval]
  ;; I need cas to know which values to watch for changes....
  (r/wcar conn (r/watch k))
  (if (not= oldval (deref* conn k))
    (do (r/wcar conn (r/unwatch))
        false)
    (some? (r/wcar conn
                   (r/multi)
                   (r/set k {:data newval})
                   (r/exec)))))


(defn mol-cas!
  [this oldval newval]
  (validate* (.getValidator this) newval)
  (let [ret (compare-and-set* (:conn (.state this)) (:k (.state this)) oldval newval)]
    (when ret
      (.notifyWatches this oldval newval))
      ;; ^ if mol ends up implementing IAtom2 then we can potentially attach watches to it
    ret))

(def ra1 (redis-atom conn :rk1 41))
(def ra2 (redis-atom conn :rk2 42))
(def ra3 (redis-atom conn :rk3 43))

(def mol2 (molecula ra1 ra2)) ;; let's start with 2 atoms for now

;; regular atom would work like this
(swap! ra1 (fn [a x] (+ a x)) 567)

;; but if I want to set ra1 and also watch ra2?
;; one option is to change swap! interface to something like this

(swap2! mol ra1 f "456" :watch ra2)


;; although, I *am* passing all the atoms when creating molecula so in theory this should be enough to:
(swap! mol2 (fn [a x] (+ a x)) ra1 456)
;; to also watch ra2 for changes; and for
(swap! mol2 (fn [a x] (+ a x)) ra2 789)
;; to also watch ra1 for changes

;; the above will work slightly different than usual because we're not actually going to apply f on ra1; instead we're going to pass @ra1 to f and watch all the atoms in mol2

;; and if I want to change multiple atoms at the same time but still make sure that all other atoms in mol are watched then I will need a new function

(def mol3 (molecula ra1 ra2 ra3))

(defn swap-multi!
  [this & args]
  ;; (count args should be divisable by 3)
  42)
(swap-multi! mol3 ;; nope! we're going to go with refs transactions syntax
  f1 ra1 args1
  f2 ra2 args2)


;; deref a molucula?
@mol2 ; => ?? [@ra1 @ra2 ...] perhaps? nope! deref only the atom you want; it's a RedisAtom

; compare-and-set! ?
(compare-and-set! mol2 oldval newval) ;; => t/f



;; another idea is to implement a sort of ref on redis.
;; it doesn't necessarily have to implement the full ref interface and and STM on top of redis but I believe it could still keep the ref transaction syntax and perform with the same guarantees while only using optimistic locking (watch/multi/exec)

(def ^:dynamic *t* nil)

(defn watch-when-new
  [this]
  (when (not-watched-todo this)
    (watch-on-redis-todo this)
    (add-to-watch-list-todo)))

(defn current-val
;; mebbe just deref from redis since no transaction value exists
  [this]
  (deref* (:conn (.state this)) (:k (.state this))))
  ;; not sure if I should throw ref unbound ex here if no key on redis

(defn has-tx-vals? [this]
  (contains? (:vals *t*) this))

(defn get-tx-val
  [this]
  (get (:vals *t*) this))

(defn set-tx-val
  [this val]
  (set! *t* (update *t* :vals assoc this val)))

(defn in-tx-commutes?
  [this]
  (contains? (:commutes *t*) this))

(defn in-tx-sets?
  [this]
  (contains? (:sets *t*) this))

(defn add-to-tx-sets
  [this]
  (set! *t* (update *t* :sets conj this)))



(defn in-tx-ensures?
  [this]
  (contains? (:ensures *t*) this))

(defn do-get
  [this]
  (if (has-tx-vals? this)
    (get-tx-val this)
    (do
      (r/watch this) ;; first we watch on redis! (TODO: implement this)
      (let [val (current-val this)] ;; then we get the al from redis
        (set-tx-val this val) ;; then we set in-tx val of ref
        val ;; then we return val
      ))))

(defn -deref
  [this]
  (if (nil? *t*)
    (current-val this)
    (do-get this)))



(defn do-set
  [this val]
  (when (in-tx-commutes? this)
    (throw (IllegalStateException. "Can't set after commute")))
  (when (not (in-tx-sets? this))
    (add-to-tx-sets this))
  (set-tx-val this val)
  val)

(defn -alter-IFn-ISeq
  [this f args]
  (do-set this (apply f (do-get this) args))) ;; NOTE: do-get watches on redis

(defn run-in-transaction
  [f]
  42)


(defn ->transaction
  []
  {:vals {} :sets #{} :commutes {} :ensures {}})

(defmacro mol-sync
  [flags-ignored-for-now & body]
  `(binding [~*t* (->transaction)] ;; initialize transaction
    (run-in-transaction (fn [] ~@body)))) ;; from here we can sort of implement the same as java code


(defmacro dosync
  "Runs the exprs (in an implicit do) in a transaction that encompasses
  exprs and any nested calls.  Starts a transaction if none is already
  running on this thread. Any uncaught exception will abort the
  transaction and flow out of dosync. The exprs may be run more than
  once, but any effects on Refs will be atomic."
  ;; ^^ rewrite docstring for redis
  [& exprs]
  `(mol-sync nil ~@exprs))

(comment

;; let's use https://www.braveclojure.com/zombie-metaphysics/ refs example and try to implement it using carmine before attmpting to abstract anything

(def sock-varieties
  #{"darned" "argyle" "wool" "horsehair" "mulleted"
    "passive-aggressive" "striped" "polka-dotted"
    "athletic" "business" "power" "invisible" "gollumed"})

(defn sock-count
  [sock-variety count]
  {:variety sock-variety
   :count count})

(defn generate-sock-gnome
  "Create an initial sock gnome state with no socks"
  [name]
  {:name name
   :socks #{}})


(def sock-gnome (ref (generate-sock-gnome "Barumpharumph")))
(def dryer (ref {:name "LG 1337"
                 :socks (set (map #(sock-count % 2) sock-varieties))}))

;; deref example
(:socks @dryer)


;; try to make this work on redis (possibly with RedisAtom)
(defn steal-sock
  [gnome dryer]
  (dosync
   (when-let [pair (some #(if (= (:count %) 2) %) (:socks @dryer))]
     (let [updated-count (sock-count (:variety pair) 1)]
       (alter gnome update-in [:socks] conj updated-count)
       (alter dryer update-in [:socks] disj pair)
       (alter dryer update-in [:socks] conj updated-count)))))
(steal-sock sock-gnome dryer)

(:socks @sock-gnome)


;; counter example
(def counter (ref 0))
(future
  (dosync
   (alter counter inc)
   (println @counter)
   (Thread/sleep 500)
   (alter counter inc)
   (println @counter)))
(Thread/sleep 250)
(println @counter)


;; commute example
(defn sleep-print-update
  [sleep-time thread-name update-fn]
  (fn [state]
    (Thread/sleep sleep-time)
    (println (str thread-name ": " state))
    (update-fn state)))
(def counter (ref 0))
(future (dosync (commute counter (sleep-print-update 100 "Thread A" inc))))
(future (dosync (commute counter (sleep-print-update 150 "Thread B" inc))))
)