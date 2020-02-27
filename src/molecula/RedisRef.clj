(ns molecula.RedisRef
  (:require [molecula.redis :as r])
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