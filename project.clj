(defproject molecula "0.1.4"
  :description "Clojure refs on Redis with one line of code"
  :license {
    :name "MIT"
    :url "https://opensource.org/licenses/mit-license.php"}
  :dependencies [
    [com.taoensso/carmine "2.19.1"]
    [org.clojure/clojure "1.10.0"]]
  :repl-options {:init-ns molecula.core}
  :aot [molecula.RedisRef])
