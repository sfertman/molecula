(ns molecula.core
  (:refer-clojure :exclude [dosync]) ;; suppress warning
  (:require
    [molecula.transaction :as tx]))

; (defmacro mol-sync
;   [flags-ignored-for-now & body]
;   `(binding [~tx/*t* (tx/->transaction)] ;; initialize transaction
;     (tx/run-in-transaction (fn [] ~@body)))) ;; from here we can sort of implement the same as java code

(defmacro mol-sync
  [flags-ignored-for-now & body]
  `(tx/run-in-transaction (fn [] ~@body)))
;; possibly init t inside run-in-tx

(defmacro dosync
  "Runs the exprs (in an implicit do) in a transaction that encompasses
  exprs and any nested calls.  Starts a transaction if none is already
  running on this thread. Any uncaught exception will abort the
  transaction and flow out of dosync. The exprs may be run more than
  once, but any effects on Refs will be atomic."
  ;; ^^ rewrite docstring for redis
  [& exprs]
  `(mol-sync nil ~@exprs))

(defn redis-ref
  ([redis-atom] 41) ;; should create a ref using the redis atom blueprint
  ([conn k] 42)
  ([conn k val] 43)
  ([conn k val & {mta :meta v-tor :validator}] 44))

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

(def conn {:pool {} :host "redis://localhost:6379"})

(def redis-ref* (partial redis-ref conn))
(def sock-gnome (redis-ref* :gnome
                            (generate-sock-gnome "Barumpharumph")))
(def dryer (redis-ref* :dryer
                       {:name "LG 1337"
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