(defproject celery/jepsen-bridge "0.1.0-SNAPSHOT"
  :description "Runs Jepsen's own checkers (total-queue, queue, stats) over
               histories recorded by the Celery correctness harness, as an
               independent cross-check of the Python checkers."
  :license {:name "BSD-3-Clause"}
  ; Pinned to the versions already present in ~/.m2 so `lein -o` works
  ; offline. jepsen 0.3.12 depends on clojure 1.12.5.
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.12"]
                 [cheshire "6.2.0"]
                 ; Pin a few transitive deps to the versions already in
                 ; ~/.m2 (left there by resolving jepsen-io/conductor), so
                 ; offline resolution succeeds.
                 [com.fasterxml.jackson.core/jackson-databind "2.17.1"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.17.1"]
                 [commons-codec "1.20.0"]
                 [org.clojure/tools.reader "1.5.2"]]
  :main jepsen-bridge.check
  :aot [jepsen-bridge.check]
  ; Build output defaults to ./target; set JEPSEN_BRIDGE_TARGET to keep build
  ; artifacts out of the source tree.
  :target-path ~(or (System/getenv "JEPSEN_BRIDGE_TARGET") "target/%s")
  :clean-targets ~(with-meta [:target-path]
                    {:protect (nil? (System/getenv "JEPSEN_BRIDGE_TARGET"))})
  :jvm-opts ["-Djava.awt.headless=true" "-server"]
  :uberjar-name "jepsen-bridge-standalone.jar"
  :profiles {:uberjar {:aot :all}})
