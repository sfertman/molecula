(ns molecula.core-test
  (:require
    [molecula.transaction :as tx]
    [clojure.test :refer :all]
    [molecula.common-test :as ct :refer [conn rr mds wcar* flushall]]
    [molecula.core :as mol :refer [redis-ref]]
    [molecula.RedisRef]
    [molecula.redis :as r]
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

;; Unit
(deftest dosync-test
  (testing "Should run input expressions in a trnsaction")
  (testing "Should start a new transaction if none is runnign on this thread")
  (testing "Should throw on any uncaught exceptions"))

(deftest redis-ref-test
  (testing "Should return a RedisRef object from RedisAtom")
  (testing "Should return a RedisRef object without any update to backend when given conn and k")
  (testing "Should return a RedisRef object and SET key/value on backend when given conn, k and val and key DOES NOT EXIST on backend")
  (testing "Should return a RedisRef object and NOT SET key/value on backend when given conn, k and val and key EXISTS on backend")
  (testing "Should return a RedisRef object with validator and/or meta when given")

  )


;; Methods

(deftest deref-test
  (testing "Should return redis value when called outside transaction"
    (let [rr1 (rr :deref1|k1 42 )]
      (is (= 42 @rr1)))
    (let [rr2 (rr :deref1|k2 {:k 42})]
      (is (= {:k 42} @rr2))))
  (testing "Shoud return transaction value when called inside transaction"
    (let [rk1 :deref2|k1
          rr1 (rr rk1 42)]
      (is (= 42 @rr1))
      (mds
        (alter rr1 inc)
        (is (= 43 @rr1) "Value within transaction has changed")
        (is (= 42 (r/deref* conn rk1)) "Value on redis is still the old one before transaction commited"))
      (is (= 43 @rr1) "Value on redis is the new value once transaction is done"))))

(deftest ref-set-test)
(deftest commute-test)
(deftest alter-test)
(deftest ensure-test)

;; Integration
(deftest integration-test)


(deftest test-examples

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


  (def redis-ref* (partial redis-ref conn))
  (def sock-gnome (redis-ref* :gnome
                              (generate-sock-gnome "Barumpharumph")))
  ; (prn sock-gnome sock-gnome)
  (def dryer (redis-ref* :dryer
                        {:name "LG 1337"
                         :socks (set (map #(sock-count % 2) sock-varieties))}))
  (defn steal-sock
    [gnome dryer]
    (mol/dosync conn
      (when-let [pair (some #(if (= (:count %) 2) %) (:socks @dryer))]
        (let [updated-count (sock-count (:variety pair) 1)]
          (alter gnome update-in [:socks] conj updated-count)
          (alter dryer update-in [:socks] disj pair)
          (alter dryer update-in [:socks] conj updated-count)))))

  (prn "starting test")
  (steal-sock sock-gnome dryer)

  (prn "SHOW ME YOUR SOCKS!!!!")
  (prn (:socks @sock-gnome))

  (prn "steal another one")
  (steal-sock sock-gnome dryer)
  (prn (:socks @sock-gnome))


  (prn "steal another one")
  (steal-sock sock-gnome dryer)
  (prn (:socks @sock-gnome))

  ;; ^^ all work as expected



  ;; counter example
  (def counter (rr :rr-counter 0)) ;; works as expected
  (future
    (mol/dosync conn
      (alter counter inc)
      (println @counter)
      (Thread/sleep 500)
      (alter counter inc)
      (println @counter)))
  (Thread/sleep 250)
  (println @counter)

  (Thread/sleep 500)
  (prn "done")


  )
(deftest spam
 ;; commute example
  (defn sleep-print-update
    [sleep-time thread-name update-fn]
    (fn [state]
      (Thread/sleep sleep-time)
      (println (str thread-name ": " state))
      (update-fn state)))
  (def counter (rr :rr-commute 0))
  (future (mol/dosync conn (prn "hello A") (commute counter (sleep-print-update 100 "Thread A" inc))))
  (future (mol/dosync conn (prn "hello B") (commute counter (sleep-print-update 150 "Thread B" inc))))
  (Thread/sleep 1000)
  (prn "@counter" @counter)
  (prn "all done")
  ;; TODO: commute example works fine but not the same as stm
  ;; have to finish writing tests for redis/cas-multi and tx/commit -- something is probably wrong there

; (defn sleep-print-update
;   [sleep-time thread-name update-fn]
;   (fn [state]
;     (Thread/sleep sleep-time)
;     (println (str thread-name ": " state))
;     (update-fn state)))
; (def counter (ref 0))
; (future (dosync (commute counter (sleep-print-update 1000 "Thread A" inc))))
; (future (dosync (commute counter (sleep-print-update 1500 "Thread B" inc))))

; (Thread/sleep 5000)

  )