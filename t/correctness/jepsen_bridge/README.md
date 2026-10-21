# Jepsen bridge

Runs Jepsen's own checkers over histories recorded by the Celery correctness
harness. The point is an independent cross-check of the Python checkers: the
same history, judged by code we didn't write (`jepsen.checker/total-queue`
and friends from jepsen 0.3.12).

## Usage

```sh
# 1. canonical JSONL -> Jepsen EDN (stdlib Python)
python3 to_edn.py history.jsonl -o history.edn [--process-map procs.json]

# 2. check (run from this directory)
lein run history.edn                       # total-queue (default)
lein run history.edn all                   # total-queue, queue, stats
lein run history.edn total-queue --format json

# or build once and skip Leiningen's startup cost
lein uberjar
java -jar target/jepsen-bridge-standalone.jar history.edn [checker ...]
```

Exit codes: `0` valid, `1` invalid, `2` unknown (a checker threw), `3` usage
error or malformed history. Output goes to stdout as EDN, then JSON (one
line); `--format edn|json` prints just one. Logs go to stderr. On a nonzero
exit, plain `lein run` adds its own "Suppressed exit" line to the output. For
machine consumption, use `lein trampoline run ...` or the uberjar.

Timing for a history of about 600 ops: the check itself takes about 40 ms.
`java -jar` takes about 3.5 s wall, `lein run` about 6.5 s, and the first
`lein run` (AOT compile) about 15 s.

Dependencies are pinned to versions already in `~/.m2`, so `lein -o` (offline)
works. Set `JEPSEN_BRIDGE_TARGET=/some/dir` to build outside the source tree.
`spike_to_canonical.py` converts the early spike's `{"history", "events"}`
files to canonical JSONL.

## Mapping

| canonical op                        | Jepsen op                                     |
|-------------------------------------|-----------------------------------------------|
| `enqueue` invoke/ok/fail/info       | `:f :enqueue`, same `:type`, `:value` op id    |
| `execute` invoke (task start)       | `:f :dequeue :type :invoke :value nil`         |
| `execute` ok (task end)             | `:f :dequeue :type :ok :value` op id           |
| `execute` with no completion        | `:f :dequeue :type :info :value nil`, added at the end |
| `nemesis`                           | `:process :nemesis :type :info :f :nemesis`    |
| `read` and anything else            | dropped                                        |

Processes are renumbered to integers 0, 1, 2, ... in order of first
appearance, because Jepsen treats only integer processes as client processes
(`stats` ignores the rest). Each op keeps `:src-process` and `:src-index`, and
dequeues also carry `:op`, `:token` and `:attempt`. Times become integer
nanoseconds. Indices are rewritten to be dense, and the converter rejects a
process that invokes twice without completing, or completes with nothing
outstanding. Jepsen checks both again when it loads the history.

## Semantics and caveats

- **"Dequeued" means the execution finished, not that it started.** An
  element counts as out of the queue only when its task body ran to the end.
  If task start counted as the dequeue, early-ack + SIGKILL loss would never
  show up, because every lost op did start. One consequence: a worker that
  dies after recording `execute ok` but before acking counts as a dequeue,
  and its redelivery shows up as a duplicate.
- **An execution killed mid-run** becomes an `:info` dequeue with value `nil`.
  This follows Jepsen's convention for a crashed op: the outcome is unknown and
  the process is never reused. Its value is unknown, so `total-queue` ignores
  it. The op is lost unless some other execution of it completes. The `:info`
  goes at the end of the history because the canonical format doesn't say
  which worker each kill hit, so the real time of death isn't known. That
  doesn't matter to the queue checkers, but it would inflate latencies in a
  timeline or perf checker.
- **`total-queue` counts duplicates but they don't fail it.** `:valid?` is
  `(and (empty? lost) (empty? unexpected))`. If two executions of one op both
  complete, that shows up in `:duplicated` and the result can still be valid.
  Use the `queue` checker (unordered-queue model) to make a second completed
  execution fatal. `queue` doesn't detect loss.
- **Dequeuing an indeterminate (`info`) enqueue counts as `:recovered`.**
  That is correct.
- **Dequeuing a *failed* enqueue also counts as `:recovered`**, because
  `total-queue` treats every enqueue *invocation* as an attempt. Under the
  Celery contract ("fail = definitely not published") that's a violation.
  Pass `--drop-failed-enqueues` to `to_edn.py` to leave failed enqueues out,
  so executing one shows up as `:unexpected`.
- An indeterminate enqueue that never executes isn't lost. Only enqueues
  acknowledged with `ok` must come out.
- `total-queue` doesn't depend on order, so clock skew between hosts doesn't
  affect it. `queue` replays ops in history order: an `execute ok` recorded
  before its `enqueue` invoke (skew) would wrongly fail it.
- Final result reads, result values and tokens play no part here. Checks on
  the result backend stay with the Python checkers.
- Like any Jepsen queue checker, `total-queue` assumes the queue was fully
  drained, which means the harness's quiesce phase ended before the last
  record.
