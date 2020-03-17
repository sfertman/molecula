(ns molecula.core-test
  (:require
    [clojure.test :refer :all]
    [molecula.core :as mol :refer [redis-ref]]
    [molecula.RedisRef]
    [taoensso.carmine :as redis]))

(def conn {:pool {} :spec {:uri "redis://localhost:6379"}})
(defmacro wcar* [& body] `(redis/wcar conn ~@body))

(wcar* (redis/flushall))

(deftest test-examples

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

  (defn alter
    "Must be called in a transaction. Sets the in-transaction-value of
    ref to:

    (apply fun in-transaction-value-of-ref args)

    and returns the in-transaction-value of ref."
    {:added "1.0"
    :static true}
    [^clojure.lang.IRef ref fun & args]
    (. ref (alter fun args)))

  ;; try to make this work on redis (possibly with RedisAtom)
  (defn steal-sock
    [gnome dryer]
    (mol/dosync conn
      (prn "hello")
      (when-let [pair (some #(if (= (:count %) 2) %) (:socks @dryer))]
        (prn "hello again")
        (let [updated-count (sock-count (:variety pair) 1)]
          (alter gnome update-in [:socks] conj updated-count)
          (alter dryer update-in [:socks] disj pair)
          (alter dryer update-in [:socks] conj updated-count)))))

  (steal-sock sock-gnome dryer)

  (:socks @sock-gnome)


  ; (comment
  ;   ;; counter example
  ; (def counter (ref 0))
  ; (future
  ;   (mol/dosync
  ;   (alter counter inc)
  ;   (println @counter)
  ;   (Thread/sleep 500)
  ;   (alter counter inc)
  ;   (println @counter)))
  ; (Thread/sleep 250)
  ; (println @counter)


  ; ;; commute example
  ; (defn sleep-print-update
  ;   [sleep-time thread-name update-fn]
  ;   (fn [state]
  ;     (Thread/sleep sleep-time)
  ;     (println (str thread-name ": " state))
  ;     (update-fn state)))
  ; (def counter (ref 0))
  ; (future (mol/dosync (commute counter (sleep-print-update 100 "Thread A" inc))))
  ; (future (mol/dosync (commute counter (sleep-print-update 150 "Thread B" inc))))
  ; )
  )