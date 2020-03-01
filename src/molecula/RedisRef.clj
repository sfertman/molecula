(ns molecula.RedisRef
  (:require
    [molecula.redis :as r]
    [molecula.transaction :as tx])
  (:gen-class
    :name RedisRef
    :extends clojure.lang.ARef
    :implements [
      clojure.lang.IFn
      clojure.lang.IRef
      ; java.lang.Comparable -- perhaps I can do without?
    ]
    :state state
    :init init
    :constructors {
      [java.lang.Object] []
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



(defn -init
  ([conn k] [[] {:conn conn :k k}])
  ([conn k mta] [[mta] {:conn conn :k k}]))


(defn- current-val
;; mebbe just deref from redis since no transaction value exists
  [this]
  (r/deref* (:conn (.state this)) (:k (.state this))))
  ;; not sure if I should throw ref unbound ex here if no key on redis


(defn -deref
  [this]
  (if (nil? (tx/get-ex))
    (current-val this)
    (tx/do-get this)))

(defn -alter-IFn-ISeq
  [this f args]
  (tx/do-set this (apply f (tx/do-get this) args))) ;; NOTE: do-get watches on redis

(defn -commute-IFn-ISeq
  [this f args]
  (tx/do-commute this f args))



(defn -touch [this] (tx/do-ensure this))
