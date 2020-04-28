(ns molecula.RedisRef
  (:require
    [molecula.transaction :as tx]
    [molecula.redis :as r])
  (:import
    (clojure.lang IFn ISeq Keyword))
  (:gen-class
    :name RedisRef
    :extends clojure.lang.Ref
    :implements [
      clojure.lang.IFn
      java.lang.Comparable ;;-- perhaps I can do without? unclear, perhaps will need to make a method for this
      ; clojure.lang.IRef
    ]
    :methods [
      [key [] clojure.lang.Keyword]
      [conn [] clojure.lang.PersistentArrayMap]]
    :state state
    :init init
    :constructors {
      [clojure.lang.PersistentArrayMap clojure.lang.Keyword] [Object]
      [clojure.lang.PersistentArrayMap clojure.lang.Keyword Object] [Object]
      [clojure.lang.PersistentArrayMap clojure.lang.Keyword Object clojure.lang.IPersistentMap] [Object clojure.lang.IPersistentMap]
    }))

(defn -init
  ([conn k]
    [[nil] {:conn conn :k (keyword k)}])
  ([conn k initVal]
    [[initVal] {:conn conn :k (keyword k)}])
  ([conn k initVal meta]
    [[initVal meta] {:conn conn :k (keyword k)}]))
  ;; NOTE: I don't care about initVal; it is here only to
  ;; satisfy super(...). The real initial value of redis-ref
  ;; is on redis and is set molecula.core/redis-ref

(defn -key [this] (:k (.state this)))
(defn -conn [this] (:conn (.state this)))

(defn -deref
  [this]
  (if-not (tx/running?)
    (r/deref* (.conn this) (.key this))
    (tx/do-get this)))

(defn -set
  [this val]
  (tx/throw-when-nil-t)
  (tx/do-set this val))

(defn -commute
  [this f args]
  (tx/throw-when-nil-t)
  (tx/do-commute this f args))

(defn -alter
  [this f args]
  (tx/throw-when-nil-t)
  (tx/do-set this (apply f (tx/do-get this) args)))

(defn -touch
  [this]
  (tx/throw-when-nil-t)
  (tx/do-ensure this))

; (defn -trimHistory [this] (throw (NoSuchMethodException. "Not implemented")))
; (defn -getHistoryCount [this] (throw (NoSuchMethodException. "Not implemented")))

(comment
  ;;; IFn stuff goes here -- don't actually need this if I'm extending clojure.lang.Ref
  (defn -fn [this] (cast clojure.lang.IFn (.deref this)))
  (defn -call [this] (.invoke this))
  (defn -run [this] (.invoke this) nil)
  (defn -invoke
    [this]
    (.invoke (.fn this)))
  (defn -invoke-Object
    [this arg1]
    (.invoke (.fn this) arg1))
  (defn -invoke-Object-Object
    [this arg1 arg2]
    (.invoke (.fn this) arg1 arg2))
  (defn -invoke-Object-Object-Object
    [this arg1 arg2 arg3]
    (.invoke (.fn this) arg1 arg2 arg3))
  (defn -applyTo
    [this ^clojure.lang.ISeq arglist]
    (.applyToHelper this arglist))
)