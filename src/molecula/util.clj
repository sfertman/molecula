(ns molecula.util
  (:require
    [molecula.redis :as r]))

(defn deref* [ref] (r/deref* (.conn ref) (.key ref)))
