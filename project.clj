(defproject molecula "0.1.0"
  :description "Clojure refs on Redis"
  :license {
    :name "MIT"
    :url "https://opensource.org/licenses/mit-license.php"}
  :dependencies [
    [com.taoensso/carmine "2.19.1"]
    [org.clojure/clojure "1.10.0"]
    [org.clojure/core.async "0.4.500"]]
  :repl-options {:init-ns molecula.core}
  :aot [molecula.RedisRef])
