(ns molecula.RedisRef
  (:require
    [molecula.redis :as r]
    [molecula.transaction :as tx])
  (:import
    (clojure.lang IFn ISeq Keyword))
  (:gen-class
    :name RedisRef
    :extends clojure.lang.ARef ;; this actually needs to extend clojure.lang.Ref to work "with one line of code"
    :implements [
      clojure.lang.IFn
      java.lang.Comparable ;;-- perhaps I can do without? unclear, perhaps will need to make a method for this
      ; clojure.lang.IRef
    ]
    :methods [ ;; this is needed because we're implementing new methods that are not part of the abstract class or interfaces.
      [key [] clojure.lang.Keyword]
      [alter
        [clojure.lang.IFn clojure.lang.ISeq]
        Object]
      [commute
        [clojure.lang.IFn clojure.lang.ISeq]
        Object]
        ;; gotta put all NEW class methods here
        ]
    :state state
    :init init
    :constructors {
      [java.lang.Object] []
      [clojure.lang.PersistentArrayMap clojure.lang.Keyword] []
      [clojure.lang.PersistentArrayMap clojure.lang.Keyword clojure.lang.IPersistentMap] [clojure.lang.IPersistentMap]
    }))

;; is not implemented in java class
; (defn -setValidator
;   [this vf]
;   42)
; (defn -getValidator
;   [this] 42)
; getWatches
; addWatch
; removeWatch

(defn -key [this] (:k (.state this)))

(defn- validate*
  "This is a clojure re-implementation of clojure.lang.ARef/validate because
  cannot be accessed by subclasses Needed to invoke when changing atom state"
  [^clojure.lang.IFn vf val]
  (try
    (if (and (some? vf) (not (vf val)))
      (throw (IllegalStateException. "Invalid reference state")))
    (catch RuntimeException re
      (throw re))
    (catch Exception e
      (throw (IllegalStateException. "Invalid reference state" e)))))


(defn -init
  ([conn k] [[] {:conn conn :k (keyword k)}])
  ([conn k mta] [[mta] {:conn conn :k (keyword k)}]))


(defn- current-val
;; mebbe just deref from redis since no transaction value exists
  [this]
  (r/deref* (:conn (.state this)) (:k (.state this))))
  ;; not sure if I should throw ref unbound ex here if no key on redis



(defn -deref
  [this]
  (if (nil? tx/*t*)
    (current-val this)
    (tx/do-get this)))

(defn -set [this val] (tx/do-set this val))

(defn -commute
  [this f args]
  (tx/do-commute this f args))

(defn -alter
  [this f args]
  (tx/do-set this (apply f (tx/do-get this) args))) ;; NOTE: do-get watches on redis

(defn -touch [this] (tx/do-ensure this))

;;; IFn stuff goes here
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