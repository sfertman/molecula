(ns molecula.util)

(defn zipseq
  "Returns a lazy sequence of partitions of interleaved colls.
  Given `c1, c2, ..., cn` of the same length returns:
  ```
  '(((first  c1) (first  c2) ... (first  cn))
    ((second c1) (second c2) ... (second cn))
    ...
    ((last   c1) (last   c2) ... (last   cn)))
  ```
  If lengths are different then the returned seq will have the length of the shortest collection (like with ) return upto the length of the shortest collection and the tails of other collection discarded. There is no padding like with `partition` for example."
  [& colls]
  (partition (count colls) (apply interleave colls)))

(defn zipvec
  "Like zipseq but returns all vectors."
  [& colls]
  (vec (map vec (partition (count colls) (apply interleave colls)))))
