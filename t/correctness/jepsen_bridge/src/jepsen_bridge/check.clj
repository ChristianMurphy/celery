(ns jepsen-bridge.check
  "Command-line entry point for the Celery -> Jepsen bridge.

  Loads an EDN history written by to_edn.py, runs Jepsen's own checkers over
  it, and prints the result map as EDN and/or JSON on stdout.

  Exit codes: 0 = :valid? true, 1 = :valid? false, 2 = :valid? :unknown,
              3 = usage error or malformed input."
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [knossos.model :as model])
  (:import (java.io PushbackReader)
           (multiset.core MultiSet)))

(def checkers
  "Checkers selectable on the command line."
  {"total-queue"
   {:doc  (str "What goes in must come out: every acknowledged enqueue needs "
               "an :ok dequeue (a completed execution). Tolerates duplicates.")
    :make checker/total-queue}

   "queue"
   {:doc  (str "Every :ok dequeue must come from a not-yet-dequeued enqueue, "
               "using an unordered multiset queue model. Stricter than "
               "total-queue: a second completed execution of the same op is "
               "a violation. Does not detect loss.")
    :make #(checker/queue (model/unordered-queue))}

   "stats"
   {:doc  (str "ok/fail/info counts overall and per :f. Invalid if some :f "
               "never succeeded.")
    :make checker/stats}})

(def default-checkers ["total-queue"])

(def exit-codes {true 0, false 1, :unknown 2})

(defn usage []
  (str/join
    \newline
    (concat
      ["Usage: lein run <history.edn> [checker ...] [--format edn|json|both]"
       "       java -jar jepsen-bridge-standalone.jar <history.edn> [checker ...]"
       ""
       (str "Checkers (default: " (str/join " " default-checkers)
            "; 'all' selects every checker):")]
      (for [[n {:keys [doc]}] (sort checkers)]
        (str "  " n (apply str (repeat (- 13 (count n)) \space)) doc))
      [""
       "Exit: 0 valid, 1 invalid, 2 unknown, 3 usage/input error."])))

(defn usage-error [msg]
  (throw (ex-info msg {:type ::usage})))

(defn parse-args
  [args]
  (loop [args args, opts {:format "both", :checkers [], :history nil}]
    (if-let [[a & more] (seq args)]
      (cond
        (#{"-h" "--help"} a) (assoc opts :help true)

        (= "--format" a)
        (let [f (first more)]
          (when-not (#{"edn" "json" "both"} f)
            (usage-error (str "--format takes edn, json or both, not "
                              (pr-str f))))
          (recur (next more) (assoc opts :format f)))

        (str/starts-with? a "-") (usage-error (str "Unknown option " a))

        (nil? (:history opts)) (recur more (assoc opts :history a))

        (= "all" a) (recur more (update opts :checkers into (sort (keys checkers))))

        (contains? checkers a) (recur more (update opts :checkers conj a))

        true (usage-error (str "Unknown checker " (pr-str a))))
      (cond-> opts
        (empty? (:checkers opts)) (assoc :checkers default-checkers)
        true (update :checkers (comp vec distinct))))))

(defn load-history
  "Reads an EDN vector of op maps and builds a dense Jepsen history. Throws if
  indices aren't 0, 1, 2, ..., or if Jepsen's pair index finds a process with
  two outstanding invocations or a completion without an invocation."
  [path]
  (let [ops (with-open [r (PushbackReader. (io/reader path))]
              (edn/read {:eof ::eof} r))]
    (when-not (vector? ops)
      (throw (ex-info (str path " does not contain an EDN vector of ops")
                      {:type ::bad-input})))
    (let [history (h/dense-history ops)]
      ; Force the pair index; this is where Jepsen asserts well-formedness.
      (when (pos? (count history))
        (h/pair-index history 0))
      history)))

(defn- sortish
  [xs]
  (try (vec (sort xs))
       (catch Exception _ (vec (sort-by pr-str xs)))))

(defn normalize
  "Turns multisets into sorted vectors (repetitions kept) and records (e.g.
  Ops, knossos models) into plain maps, so the result reads back as plain EDN
  and encodes as JSON."
  [x]
  (walk/prewalk (fn [x]
                  (cond (instance? MultiSet x)          (sortish (seq x))
                        (instance? clojure.lang.IRecord x) (into {} x)
                        true                             x))
                x))

(defn- json-key [k]
  (cond (keyword? k) (subs (str k) 1)
        (string? k)  k
        true         (pr-str k)))

(defn jsonable
  "Stringifies map keys and anything cheshire can't encode."
  [x]
  (walk/postwalk (fn [x]
                   (cond (map-entry? x) x
                         (map? x) (into {} (map (fn [[k v]] [(json-key k) v])) x)
                         (or (nil? x) (boolean? x) (number? x) (string? x)
                             (keyword? x) (symbol? x) (coll? x)) x
                         true (str x)))
                 x))

(defn run-check
  "Runs the CLI with parsed args; returns an exit code."
  [{:keys [history format] :as opts}]
  (let [t0      (System/nanoTime)
        hist    (load-history history)
        t1      (System/nanoTime)
        names   (:checkers opts)
        c       (checker/compose
                  (into {} (for [n names] [(keyword n) ((:make (checkers n)))])))
        test    {:name       "celery-jepsen-bridge"
                 :start-time 0
                 :history    hist}
        res     (checker/check c test hist {})
        t2      (System/nanoTime)
        res     (-> res
                    normalize
                    (assoc :bridge {:history  history
                                    :op-count (count hist)
                                    :checkers names
                                    :load-ms  (quot (- t1 t0) 1000000)
                                    :check-ms (quot (- t2 t1) 1000000)}))]
    (binding [*print-length* nil
              *print-level*  nil]
      (when (#{"edn" "both"} format)
        (when (= "both" format) (println ";; EDN"))
        (pprint res))
      (when (#{"json" "both"} format)
        (when (= "both" format) (println ";; JSON"))
        (println (json/generate-string (jsonable res)))))
    (get exit-codes (:valid? res) 2)))

(defn -main
  [& args]
  (let [code (try
               (let [opts (parse-args args)]
                 (cond (:help opts)            (do (println (usage)) 0)
                       (nil? (:history opts))  (usage-error "No history file given")
                       true                    (run-check opts)))
               (catch clojure.lang.ExceptionInfo e
                 (binding [*out* *err*]
                   (let [data (ex-data e)]
                     (cond (= ::usage (:type data))
                           (do (println "error:" (ex-message e))
                               (println (usage)))

                           (= ::bad-input (:type data))
                           (println "error:" (ex-message e))

                           ; Jepsen/slingshot errors carry the details in
                           ; their data; the message just repeats it.
                           true
                           (do (println "error: malformed history")
                               (pprint data)))))
                 3)
               (catch Exception e
                 ; IO errors, EDN syntax errors, ... -- never exit 1, which
                 ; would read as "history invalid".
                 (binding [*out* *err*]
                   (println "error:" (str e)))
                 3))]
    (flush)
    (shutdown-agents)
    (System/exit code)))
