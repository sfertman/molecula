(ns molecula.core-test
  (:require
    [clojure.test :refer :all]
    [molecula.common-test :as ct :refer [rr mds wcar* flushall]]
    [molecula.core :as mol :refer [redis-ref]]
    [molecula.RedisRef]
    [molecula.transaction :as tx]
    [taoensso.carmine :as redis]))

(flushall)

(comment
 """
  From: https://clojure.org/reference/refs

    All reads of Refs will see a consistent snapshot of the 'Ref world' as of the starting point of the transaction (its 'read point'). The transaction will see any changes it has made. This is called the in-transaction-value.

    All changes made to Refs during a transaction (via ref-set, alter or commute) will appear to occur at a single point in the 'Ref world' timeline (its 'write point').

    No changes will have been made by any other transactions to any Refs that have been ref-set / altered / ensured by this transaction.

    Changes may have been made by other transactions to any Refs that have been commuted by this transaction. That should be okay since the function applied by commute should be commutative.

    Readers and commuters will never block writers, commuters, or other readers.

    Writers will never block commuters, or readers.

    I/O and other activities with side-effects should be avoided in transactions, since transactions will be retried. The io! macro can be used to prevent the use of an impure function in a transaction.

    If a constraint on the validity of a value of a Ref that is being changed depends upon the simultaneous value of a Ref that is not being changed, that second Ref can be protected from modification by calling ensure. Refs 'ensured' this way will be protected (item #3), but don’t change the world (item #2).

    The Clojure MVCC STM is designed to work with the persistent collections, and it is strongly recommended that you use the Clojure collections as the values of your Refs. Since all work done in an STM transaction is speculative, it is imperative that there be a low cost to making copies and modifications. Persistent collections have free copies (just use the original, it can’t be changed), and 'modifications' share structure efficiently. In any case:

    The values placed in Refs must be, or be considered, immutable!! Otherwise, Clojure can’t help you.


 """)

(deftest deref-test
  (testing "Should deref outside transaction"
    (let [r1 (rr :deref|r1 42 )]
      (is (= 42 @r1)))
    (let [r2 (rr :deref|r2 {:k 42})]
      (is (= {:k 42} @r2))))
  (testing "Shoud deref inside transaction"
    (let [r3 (rr :deref|r3 42)]
      (prn "REDIS-REF R3" r3)
      (prn "@r3 is" @r3)
      (mds
        (prn "REDIS-REF inside-tx" r3)
        (prn "@r3 is" @r3)
        (clojure.pprint/pprint tx/*t*)
        (alter r3 inc)
        (prn "after inc @r3 is" @r3)
        #_(is (= 43 @r3))))))

(deftest alter-test

)
#_(deftest test-examples

  ;; let's use https://www.braveclojure.com/zombie-metaphysics/ refs example and try to implement it using carmine before attmpting to abstract anything

  (def sock-varieties
    #{"ppp" "darned" "argyle" "wool" "horsehair" "mulleted"
      "passive-aggressive" "striped" "polka-dotted"
      "athletic" "business" "power" "invisible" "gollumed" "zzz"})

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
  (prn sock-gnome sock-gnome)
  (def dryer (redis-ref* :dryer
                        {:name "LG 1337"
                         :socks (set (map #(sock-count % 2) sock-varieties))}))
  (prn "dryer" dryer)
  ;; try to make this work on redis (possibly with RedisAtom)
  (defn steal-sock
    [gnome dryer]
    ; (prn "dryer before tx")
    ; (clojure.pprint/pprint @dryer)
    (mol/dosync conn
      (prn "dryer inside tx")
      (clojure.pprint/pprint @dryer)
      ;; ^ TODO: something bout this deref messes everything up -- make a simple test for this
      (when-let [pair (some #(if (= (:count %) 2) %) (:socks @dryer))]
        (prn "Pair found!")
        (clojure.pprint/pprint pair)
        (let [updated-count (sock-count (:variety pair) 1)]
          (alter gnome update-in [:socks] conj updated-count)
          (alter dryer update-in [:socks] disj pair)
          (alter dryer update-in [:socks] conj updated-count)))))

  (prn "starting test")
  (steal-sock sock-gnome dryer)

  (prn "SHOW ME YOUR SOCKS!!!!")
  (prn (:socks @sock-gnome))

  ; (prn "steal another one") ;; <- for some reason I cannot steal another sock; pair is not found in dryer after stealing just one
  ; (steal-sock sock-gnome dryer)
  ; (prn (:socks @sock-gnome))

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