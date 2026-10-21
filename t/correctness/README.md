# Correctness harness

A Jepsen-style harness for Celery: run a workload against real brokers and
result backends, inject faults, record what clients and workers observe, and
check the recorded history against what the configuration promises.

It is not a unit or integration test suite. A run takes minutes, needs Redis
(the `redis-server` binary) and, for RabbitMQ/Postgres/MySQL, Docker.

## Running

From the repository root:

```sh
python -m t.correctness.runner --broker redis --backend redis \
    --acks-late --faults kill-worker,partition-broker --ops 300
python -m t.correctness.runner --broker rabbitmq --confirm-publish --queue-type quorum \
    --backend postgres --acks-late --workload chord --faults partition-broker
python -m t.correctness.runner --help
```

Each run writes to `--out` (default `correctness-runs/<timestamp>/`):
`history.jsonl` (the history), `results.json` (checker output), worker logs
and service data. The exit code is 0 when the history is valid.

With `--jepsen-jar` (see `jepsen_bridge/`), single-task runs are also checked
by Jepsen's own `total-queue` checker, as an independent cross-check.

## Workloads and profiles

`--workload` picks what each client operation publishes:

* `single`: one task.
* `chord`: `--chord-size` headers and a body that records its input.
* `retry`: a task that calls `self.retry()` `--retry-fails` times, then succeeds.
* `eta`: a task published with a countdown drawn from `--eta-range`.
* `chain`: `--chain-length` steps, each recording its parent's return value.

`--profile` sets the options whose documented promise differs:

| profile | options | promise checked |
|---|---|---|
| `p0` | defaults: early ack, prefetch x4 | a running task may be lost on a crash; nothing reserved but unstarted is lost |
| `p1` | `acks_late`, `reject_on_worker_lost`, prefetch x1, `confirm_publish` on RabbitMQ | no accepted task is lost; duplicates allowed |
| `p2` | p1 + `worker_deduplicate_successful_tasks` | as p1 |
| `p3` | p1 + `worker_cancel_long_running_tasks_on_connection_loss` | as p1 |

`--track-started` and `--always-retry` (`result_backend_always_retry`) cut
across profiles.

## What happens in a run

1. Start a broker, a result backend and a *recorder* Redis that faults never
   touch. Each worker reaches the broker and backend through its own TCP
   proxy (`proxy.py`), so faults can target one worker's links.
2. Clients publish tasks at `--rate` and record each publish as
   `invoke` then `ok` / `fail` (definitely not published) / `info`
   (indeterminate). A reader samples task states with **uncached** reads
   (`AsyncResult` caches READY states and would hide changes).
3. Instrumented tasks (`app.py`) log every execution to the recorder as an
   `invoke` at start and `ok` at the end; a killed execution has no `ok`.
4. The nemesis injects faults every `--fault-interval` seconds for
   `--fault-duration` seconds: `kill-worker` (SIGKILL the process group),
   `pause-worker` (SIGSTOP), `partition-broker|backend` (RST and refuse),
   `blackhole-broker|backend` (stall traffic, as in a partition shorter than
   TCP's timeouts), `kill-broker|backend`, `pause-broker|backend`.
   `--crash-hook POINT=P` kills workers at protocol steps that random faults
   rarely hit: `after_store_before_ack`, `after_retry_before_ack`,
   `before_execute`, `child_exit_after_store`.
5. Recovery: heal everything, restart workers, and wait until the broker holds
   no ready or unacked messages and nothing has happened for `--settle`
   seconds. With Redis, abandoned messages come back only after the
   visibility timeout, so the recovery timeout defaults to
   `--visibility-timeout + 120`.
6. Read every task's final state (uncached), kill the workers, merge the
   client, worker and nemesis operations by time and run the checkers.

## Checks

| anomaly | meaning | verdict |
|---|---|---|
| `lost-never-started` | publish acknowledged, task never started | violation unless the broker lost data (killed without durable persistence) |
| `lost-killed` | every execution was killed mid-run | documented with early ack; violation with `acks_late` (unless a pool child died and `reject_on_worker_lost` is off) |
| `duplicate-start`, `duplicate-complete` | task ran more than once | allowed with `acks_late`; violation with early ack |
| `concurrent` | executions of one task overlapped | reported; at-least-once delivery has no mutual exclusion |
| `unexpected` | execution of a task nobody published | violation |
| `g1a` | publish failed definitely, task ran anyway | violation |
| `recovered` | indeterminate publish, task ran | fine |
| `impossible-result` | stored result produced by no execution | violation |
| `result-changed` | a SUCCESS result read later returned a different value | violation |
| `state-regression` | a SUCCESS read later returned a non-SUCCESS state | violation |
| `result-missing` | task completed but its final state is not SUCCESS | violation unless the backend lost data |
| `orphan-execution` | execution vanished without a kill | violation (harness or worker bug) |
| `running-at-end` | still executing after recovery | violation (stall) |
| `chord-wrong-input` | chord body input differs from the stored header results | violation |
| `chord-early` | chord body started before every header completed | violation |
| `chord-stuck` | all headers succeeded, body never ran | violation |
| `chord-body-without-headers` | body ran although a header never completed | violation |
| `stalled-in-broker` | still held by the broker when recovery gave up | violation (stall) |
| `retry-out-of-order` | attempt k started before attempt k-1 raised Retry | violation |
| `retry-exceeded` | more attempts than `max_retries` allows | violation |
| `retry-fanout` | more executions than attempts (duplicates retried independently) | reported |
| `eta-early` | ran before its ETA | violation |
| `eta-late` | first ran more than 5s after its ETA | reported |
| `chain-impossible-input` | a step ran with an input no parent execution produced | violation |
| `chain-fork` | a step ran with two different inputs (its parent ran twice) | reported |
| `chain-cut` | a step never ran because its parent never completed | reported |

A killed execution is assumed to have run until its worker was killed, not
forever, when looking for overlaps.

## Avoiding false alarms

Each of these produced a false positive while building the harness:

* kombu's Redis transport sweeps for abandoned messages only every
  `unacked_restore_interval * unacked_restore_throttle` seconds (100 by
  default); `app.py` sets both to make the sweep run every second.
* `AsyncResult` and `backend.get_task_meta()` cache READY states; reads must
  use `get_task_meta(tid, cache=False)`.
* An execution without a completion must be bounded by its worker's kill.
* Tasks running longer than the visibility timeout are redelivered while
  still running (documented); keep task durations below it unless that is the
  point of the test.

Run positive controls too: early ack with `kill-worker` must report
`lost-killed`, and a broker killed with `--broker-persistence none` must
report lost tasks.

`test_checker.py` checks the checker on literal histories.
