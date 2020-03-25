(ns molecula.util
  (:require
    [molecula.redis :as r]))

(defn deref* [ref] (r/deref* (.conn ref) (.key ref)))
;; this should probably be in molecula.redis

(defn zipseq
  "Returns a lazy sequence of partitions of interleaved colls"
  [& colls]
  (partition (count colls) (apply interleave colls)))


(defn zipvec
  "Returns a vector of partitions of interleaved colls"
  [& colls]
  (vec (map vec (partition (count colls) (apply interleave colls)))))
