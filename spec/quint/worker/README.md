# Workers, and the designs compared

`worker.qnt`, `../transport/publisher.qnt` and the renewal and pop-token
variants of `../transport/redis_transport.qnt`: is a task run exactly once without a fault, at least
once with one, and which duplicates can a design remove, at what broker cost.
Celery 768e476, kombu 445027e (main) and c61b525 (kombu PR 2831).

## Models

`worker.qnt`: two workers with one pool process and a prefetch count of 2,
two messages on one queue of a Redis broker.

* The pool hand-off: received (or scheduled, for an ETA), written to the
  child, accepted (early ack), finished, result read (late ack).
* `Consumer.on_close()` and `AsynPool.flush()` at a warm shutdown and at a
  reconnect, between two turns of the event loop.
* The broker side: a record per popped message under its delivery tag; an
  ack removes it by tag and is not bound to a connection; a closing channel
  restores what it delivered and did not ack, if it reaches Redis; any
  consumer restores a record older than the visibility timeout.
* An ack that goes through, is applied with its reply lost, blocks in a dead
  connection, or goes through a channel whose connection was collected.

Classes without a fault: `none`, `shutdown` (SIGTERM), `eta` (an ETA
outlasts the visibility timeout), `long` (anything held outlasts it),
`etashutdown`. Fault classes: `kill` (SIGKILL), `crash` (the broker dies and
restarts with what it had), `partition` (commands arrive, replies do not;
it heals or closes), `cut` (nothing passes; it heals or closes), `cutlong`
(and the outage outlasts the timeout), `reset` (the connection dies and
Redis answers a new one), `hang` (a task never returns).

Time is abstract. A record of a live worker that still has a job for it is
restored only with the `timeouts` budget. A record nothing will ever ack
(its worker is gone, the job was dropped, the task hangs) is restored for
free, because the timeout always passes. Renewal blocks both.

Designs, as fields of `CFG.design` (main's value first):

| Field | Values |
|---|---|
| `pop` | `BrpopThenRecord`; `PopScript` (kombu PR 2831) |
| `flush` | `DropUnaccepted` (since celery PR 6863); `DropUnwritten`: keep a job the child already has |
| `renewal` | `NoRenewal`; `RenewAll`; `RenewUnstarted`; `RenewScheduled` (only messages waiting for an ETA) |
| `failedAck` | `Forgotten`; `Resent` after the reconnect |
| `collectedAck` | `WorkerExits` (celery issue 11054); `StillAcks` |
| `closeRestore` | `CloseRestoresAll`; `CloseRestoresIdle`: not a message whose task runs |
| `reconnectRestore` | `WaitsForTimeout`; `ReconnectRestoresAll`; `ReconnectRestoresIdle` |
| `restoreBy` | `ByTag`; `ByPop`: the pop token, a channel restores only a record its own pop wrote |

Instance names are `<design>_<ack mode>_<class>`. `main` and `pr` are the
two branches; `keepwritten`, `renewall`, `renewidle`, `reneweta`, `resend`,
`ackfull` (resend and `StillAcks`), `restoreall`, `restoreidle`, `token`,
`tagrestore` and `ownrecord` each change one mechanism of `pr`; `best` is
`DropUnwritten`, `RenewScheduled`, `Resent`, `StillAcks`, `CloseRestoresIdle`,
`ReconnectRestoresIdle` and `ByPop` together. Each design is a named value
in `workertypes` (`MAIN`, `PR`, `KEEPWRITTEN`, ..., `BEST`);
`tools/gen_worker_instances.py` writes `worker_instances.qnt` from them.

Properties:

| Property | Statement |
|---|---|
| `exactlyOnce` | While no fault was used: a task never starts twice, and when quiet each one finished once and the broker is empty. Warm shutdowns and timeouts are not faults |
| `neverLost` | A published message is on the queue or in `unacked` until a run of it finished (with early ack, a run in a live worker counts) |
| `quietDone` | A quiet system has finished every task and left nothing in the broker |
| `noUnavoidableDup` | No task runs twice with late ack after its first run was in progress when its worker or its connection was lost |
| `noAvoidableDup` | No task runs twice for another reason: the first run had finished, had not started, was acked early, or only outlasted the timeout |
| `restoresOwnOnly` | A closing or reconnecting channel restores only a record its own pop wrote |
| `workerSurvives` | No worker is stopped by an error in the ack path |
| `normalCost` | Without a fault a message costs one publish, one pop (two round trips on main) and one ack |

A second run after a task that hangs counts under neither class: it is the
recovery. A task killed after it finished and before its ack counts as
avoidable, as in the worker runs of the exactly-once report; no transport
design removes it, `worker_deduplicate_successful_tasks` does.

`publisher.qnt`: a RabbitMQ publisher with confirms off or on, two messages,
a durable queue that loses what it accepted and had not written when the
broker is killed, and a network that drops the publisher's data. Properties:
`neverLost` (a message whose publish returned is on its way, accepted or
written), `publishedOnce`, `exactlyOnce`, `quietDone`, `normalCost`.

`redis_transport.qnt` gained `renewal` (`RenewBlind`, the prototype's
`ZADD XX`; `RenewOwn`, compare and set), `putBack` (`ByScore`, the PR;
`ByToken`), the property `noStranded`, and the step a consumer takes after
a pop that emptied the queue (`waitAfterEmpty`: it waits on the wake list
without popping first), which the recorded histories showed was missing.

Not modeled: several queues and priorities, more than one pool process per
worker, a Redis that loses acknowledged writes (appendfsync everysec),
RabbitMQ on the consumer side (an ack bound to its channel, the consumer
timeout), and rate limits.

`worker_projection.qnt` maps each state of `worker.qnt` to the abstract
delivery environment of `../lib/delivery.qnt` (a message is ready when it is
on the queue, taken when the unacked hash has its record) and checks that
every step is a step of that environment (`projectionOk`; `run_all.sh`
samples five instances). On main a lost BRPOP reply is a ready copy that
disappears (`neverTaken` is reachable there and not with kombu PR 2831):
the environment allows that only as a loss by the layer below. The canvas
specs built on the environment (`../canvas/chain.qnt`) therefore hold for
this transport with that loss named.

## Properties

<!-- generated: properties worker.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `exactlyOnce` | safety | While no fault was used a task never starts twice, and when quiet each one finished once and the broker is empty. Warm shutdowns and timeouts are not faults. Assumes: two workers with one pool process and prefetch 2, two messages, Redis. | Bounded existence (exactly one run), while no fault was used | docs/faq.rst:897 (celery 768e476): "No tasks should be lost even with experimental transports as long as the shutdown completes"; docs/userguide/tasks.rst:24-26 (celery 768e476): early ack, "so that a task invocation that already started is never executed again" |
| `neverLost` | safety | A published message is on the queue or in unacked until a run of it finished; with early ack a run in a live worker counts. | Universality, global (at least once as an invariant) | docs/faq.rst:897; docs/userguide/tasks.rst:28-30 (celery 768e476): acks_late is for idempotent tasks; the message is acknowledged after the task returns |
| `quietDone` | liveness | A quiet system has finished every task and left nothing in the broker. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes; the visibility timeout always passes for a record nothing will ack. | Response: published leads to finished and acked (lemma 3 of 3) | docs/userguide/tasks.rst:28-30 (celery 768e476): acks_late is for idempotent tasks; the message is acknowledged after the task returns; docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `noAvoidableDup` | safety | No task runs twice for a reason a design could remove: the first run had finished, had not started, was acked early, or only outlasted the timeout. | Absence, global | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack, "so that a task invocation that already started is never executed again"; docs/faq.rst:897 |
| `noUnavoidableDup` | safety | No task runs twice with late ack after its first run was in progress when its worker or its connection was lost. | Absence, global | docs/userguide/tasks.rst:28-30 (celery 768e476): acks_late is for idempotent tasks; the message is acknowledged after the task returns |
| `restoresOwnOnly` | safety | A closing or reconnecting channel restores only a record its own pop wrote. | Absence, global | kombu 445027e kombu/transport/virtual/base.py:277-314, redis.py restore_by_tag |
| `workerSurvives` | safety | No worker is stopped by an error in the ack path. | Absence, global | kombu 445027e redis.py:1957 (the AttributeError of celery issue 11054) |
| `normalCost` | safety | Without a fault a message costs one publish, one pop (two round trips on main) and one ack. | Universality at the end, while no fault was used | kombu 445027e redis.py:1457-1496; kombu PR 2831 redis.py:223-305 |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: nothing can happen without a new fault. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Absence of a stop outside a quiet state (lemma 2 of 3) | the spec: `quiet`, checked by TLC with deadlock detection on `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. | Existence | sanity |
| `neverTwice` | witness | Some run starts a task twice. | Existence | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
<!-- end generated -->

## Results

`python3 tools/check.py --tier full --only worker`: every reachable state of
every instance. A number is the states in a shortest counterexample; `ok`
means the property holds in every reachable state. In every instance a run
stops only in a quiet state. One table per design (the instance is
`<design>_<ack mode>_<fault class>`):

<!-- generated: designs worker.qnt -->
`ackfull`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 21 | ok | ok | ok | 10 | ok | ok | ok | ok |
| late, crash | 26 | ok | ok | ok | 12 | ok | ok | ok | ok |
| late, cut | 21 | ok | ok | ok | 10 | ok | ok | ok | ok |
| late, cutlong | 27 | 7 | ok | ok | 7 | 8 | ok | ok | ok |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok | ok |

`best`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 17 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, crash | 20 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, cut | 17 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, cutlong | 23 | 7 | ok | ok | 7 | 8 | ok | ok | ok |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 18 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, reset | 16 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok | ok |

`keepwritten`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 28 | ok | ok | ok | 10 | ok | ok | 7 | ok |
| early, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, cut | 30 | ok | ok | ok | 12 | ok | ok | 9 | ok |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok | ok |
| late, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok | ok |

`main`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, partition | 21 | ok | 4 | 11 | 10 | ok | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | 4 | 11 | 10 | ok | ok | 9 | ok |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |

`ownrecord`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| late, etashutdown | 26 | 9 | ok | ok | 9 | ok | ok | ok | ok |

`pr`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 27 | ok | ok | ok | 10 | ok | ok | ok | ok |
| early, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| early, hang | 13 | ok | 6 | 12 | ok | ok | ok | ok | ok |
| early, kill | 19 | ok | 6 | 13 | 8 | ok | ok | ok | ok |
| early, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, reset | 18 | ok | ok | ok | 7 | ok | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 | ok |
| late, cut | 30 | ok | ok | ok | 10 | ok | ok | 9 | ok |
| late, cutlong | 36 | 7 | ok | ok | 7 | 8 | ok | 9 | ok |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 | ok |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok | ok |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |

`renewall`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| early, long | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, cutlong | 30 | ok | ok | ok | 8 | 8 | ok | 9 | ok |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 12 | ok | ok | 8 | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 12 | ok | ok | ok | ok | ok | ok | ok | ok |

`reneweta`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |

`renewidle`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 17 | ok | ok | 14 | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |

`resend`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 21 | ok | ok | ok | 10 | ok | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 | ok |
| late, cut | 27 | ok | ok | ok | 10 | ok | ok | 9 | ok |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 | ok |

`restoreall`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| late, crash | 28 | ok | ok | ok | 11 | 11 | ok | ok | ok |
| late, cut | 21 | ok | ok | ok | 10 | 9 | ok | ok | ok |

`restoreidle`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| late, crash | 26 | ok | ok | ok | 11 | ok | ok | ok | ok |
| late, cut | 20 | ok | ok | ok | 10 | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |

`tagrestore`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| late, etashutdown | 26 | 9 | ok | ok | 9 | ok | 7 | ok | ok |

`token`

| ack, class | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives | normalCost |
|---|---|---|---|---|---|---|---|---|---|
| early, cut | 27 | ok | ok | ok | 10 | ok | ok | ok | ok |
| early, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| early, reset | 18 | ok | ok | ok | 7 | ok | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 | ok |
| late, cut | 30 | ok | ok | ok | 10 | ok | ok | 9 | ok |
| late, cutlong | 36 | 7 | ok | ok | 7 | 8 | ok | 9 | ok |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 | ok |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok | ok |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok | ok |
<!-- end generated -->

Why each violation is expected:

<!-- generated: reasons worker.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `exactlyOnce` | ownrecord_late_etashutdown (9), tagrestore_late_etashutdown (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `exactlyOnce` | main_early_shutdown (9), main_late_shutdown (9), pr_early_shutdown (9), pr_late_shutdown (9), token_early_shutdown (9), token_late_shutdown (9) | open | tracker eo-warm-shutdown-flush-duplicate (celery issue 3802). flush() drops a job the child already runs |
| `exactlyOnce` | main_late_eta (9), pr_early_eta (9), pr_late_eta (9), token_early_eta (9), token_late_eta (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `exactlyOnce` | best_late_long (7), pr_early_long (7), pr_late_long (7), reneweta_late_long (7), renewidle_early_long (7), renewidle_late_long (7), token_late_long (7) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `exactlyOnce` | ackfull_late_cutlong (7), best_late_cutlong (7), pr_late_cutlong (7), token_late_cutlong (7) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". an outage longer than the visibility timeout |
| `neverLost` | main_early_partition (4), main_late_partition (4) | open | tracker reply-kombu-305-pr (kombu issue 305); kombu PR 2831 fixes it. a BRPOP whose reply is lost |
| `neverLost` | pr_early_hang (6), pr_early_kill (6) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack, "so that a task invocation that already started is never executed again". early ack: a task lost with its worker is not run again |
| `quietDone` | main_early_partition (11), main_late_partition (11) | open | tracker reply-kombu-305-pr (kombu issue 305); kombu PR 2831 fixes it |
| `quietDone` | pr_early_hang (12), pr_early_kill (13) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack, "so that a task invocation that already started is never executed again" |
| `quietDone` | renewall_late_hang (8), renewidle_late_hang (14) | by design | tracker reply-kombu-337-renew-scope (kombu issue 337). design variants RenewAll and RenewUnstarted: a hung task is never redelivered. Blocking; the reason RenewScheduled is proposed |
| `noAvoidableDup` | ownrecord_late_etashutdown (9), tagrestore_late_etashutdown (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `noAvoidableDup` | main_early_shutdown (9), main_late_shutdown (9), pr_early_shutdown (9), pr_late_shutdown (9), token_early_shutdown (9), token_late_shutdown (9) | open | tracker eo-warm-shutdown-flush-duplicate (celery issue 3802) |
| `noAvoidableDup` | main_late_eta (9), pr_early_eta (9), pr_late_eta (9), token_early_eta (9), token_late_eta (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `noAvoidableDup` | best_late_long (7), pr_early_long (7), pr_late_long (7), reneweta_late_long (7), renewidle_early_long (7), renewidle_late_long (7), token_late_long (7) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". tracker eo-redis-renew-unacked (kombu issue 337) |
| `noAvoidableDup` | ackfull_late_cutlong (7), best_late_cutlong (7), pr_late_cutlong (7), renewall_late_cutlong (8), token_late_cutlong (7) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `noAvoidableDup` | ackfull_early_cut (10), keepwritten_early_cut (10), main_early_partition (10), pr_early_cut (10), pr_early_kill (8), pr_early_reset (7), resend_early_cut (10), token_early_cut (10), token_early_reset (7) | open | tracker eo-early-ack-duplicates (celery issue 4426). early ack is not at most once across a lost connection or a kill between the start and the ack |
| `noAvoidableDup` | best_late_kill (10), main_late_kill (10), pr_late_kill (10), renewall_late_kill (10), reneweta_late_kill (10), renewidle_late_kill (10), restoreidle_late_kill (10), token_late_kill (10) | documented | docs/userguide/tasks.rst:28-30 (celery 768e476): acks_late is for idempotent tasks; the message is acknowledged after the task returns. killed after the task finished and before its ack; worker_deduplicate_successful_tasks removes it |
| `noAvoidableDup` | ackfull_late_crash (12), ackfull_late_cut (10), ackfull_late_reset (9), keepwritten_late_cut (12), keepwritten_late_reset (9), main_late_partition (10), pr_late_crash (12), pr_late_cut (10), pr_late_partition (10), pr_late_reset (9), resend_late_crash (12), resend_late_cut (10), resend_late_partition (10), restoreall_late_crash (11), restoreall_late_cut (10), restoreidle_late_crash (11), restoreidle_late_cut (10), token_late_crash (12), token_late_cut (10), token_late_partition (10), token_late_reset (9) | open | tracker eo-redis-ack-after-connection-loss (celery issue 11054). a failed ack is forgotten, and flush() at the reconnect drops a written job (tracker eo-warm-shutdown-flush-duplicate (celery issue 3802)) |
| `noUnavoidableDup` | restoreall_late_crash (11), restoreall_late_cut (9) | by design | the variant ReconnectRestoresAll. the reason the variant is rejected: it restores tasks that still run |
| `noUnavoidableDup` | ackfull_late_cutlong (8), ackfull_late_reset (7), best_late_cutlong (8), best_late_kill (8), keepwritten_late_reset (7), main_late_kill (8), pr_late_cutlong (8), pr_late_kill (8), pr_late_reset (7), renewall_late_cutlong (8), renewall_late_kill (8), reneweta_late_kill (8), renewidle_late_kill (8), restoreidle_late_kill (8), token_late_cutlong (8), token_late_kill (8), token_late_reset (7) | documented | docs/userguide/tasks.rst:28-30 (celery 768e476): acks_late is for idempotent tasks; the message is acknowledged after the task returns. the at-least-once contract of late ack: a run in progress when its worker or connection is lost is run again |
| `restoresOwnOnly` | tagrestore_late_etashutdown (7) | open | tracker eo-restore-by-tag-third-delivery |
| `workerSurvives` | keepwritten_early_cut (7), keepwritten_late_cut (9), main_late_partition (9), pr_late_crash (9), pr_late_cut (9), pr_late_cutlong (9), pr_late_partition (9), renewall_late_cutlong (9), resend_late_crash (9), resend_late_cut (9), resend_late_partition (9), token_late_crash (9), token_late_cut (9), token_late_cutlong (9), token_late_partition (9) | open | tracker eo-redis-ack-after-connection-loss (celery issue 11054) |
<!-- end generated -->

The publisher and the renewal and token instances of the transport are in
[../transport/README.md](../transport/README.md).

## What the model says about each design

Outcomes are ranked duplicate, visible failure, hang, silent loss, from
least to most harmful. A design that moves an outcome toward loss is
blocking.

1. `flush()` keeps a job the child already has (`keepwritten`). A warm
   shutdown is exactly once with it, in both ack modes; without it a task
   runs twice in 9 states, in both ack modes: the pool does not look at the
   ack mode. The same drop happens at a reconnect, where it leaves a
   duplicate no ack design removes (`ackfull_late_cut`, 10 states). Broker
   cost: none. Nothing moves toward loss.
2. Renewal. `RenewAll` and `RenewUnstarted` make an ETA exactly once and
   turn the recovery of a hung task, or of a message prefetched behind one,
   into a hang (`renewall_late_hang` 8 states, `renewidle_late_hang` 14):
   blocking as a default. `RenewScheduled` renews only a message waiting
   for its ETA: the ETA runs once, and a hung task and what is prefetched
   behind it time out as on main. A task longer than the timeout still runs
   twice. Cost: none per message, one `ZADD` per worker every third of the
   timeout. Next to kombu PR 2831 the renewal must compare and set
   (`RenewOwn`) or the put-back must use a token (`ByToken`): the blind
   `ZADD XX` moves the score of another consumer's record under the same
   tag, and `PUT_BACK` then does nothing (`renewblind_held_credit`,
   `noStranded`, 11 states).
3. A failed ack sent again, and an ack through a collected channel
   (`resend`, `ackfull`). `Resent` alone leaves the worker exit
   (`workerSurvives`, 9 states); with `StillAcks` no worker stops and a
   finished task is not delivered again. Cost: none without a fault, one
   round trip per failed ack.
4. Restore at reconnect. `ReconnectRestoresAll` restores tasks that still
   run and makes unavoidable duplicates (`restoreall_late_cut`, 9 states).
   `ReconnectRestoresIdle` restores only tags with no active request: the
   wait ends at the reconnect and no running task is duplicated
   (`restoreidle_late_cut` and `_crash` hold `noUnavoidableDup`). It needs
   `StillAcks`, and Celery has to tell kombu which requests are active.
   Cost: one round trip per restored message, after a fault only.
5. Publisher confirms. Off: a publish that returned is lost when the broker
   dies or the data is dropped (3 states). On: nothing is lost, and a lost
   confirm puts a second copy on the queue (7 and 8 states): silent loss
   becomes a duplicate. Cost: two frames per publish where there was one,
   and a wait for the second.
6. Without a per-message cost: `CloseRestoresIdle` (a closing channel leaves
   a running task's message to its ack: the duplicate of a task that was
   mid-run at a connection reset disappears, `pr_late_reset` 7 states
   against `best_late_reset` none), and the pop token.
7. The pop token (`ByPop`, `ByToken`). A message keeps its delivery tag
   when it is delivered again, and main restores by tag: a channel that
   held a message past the timeout and then closes restores the copy
   another worker runs (`tagrestore_late_etashutdown`, `restoresOwnOnly`, 7
   states; `ownrecord_late_etashutdown` holds). With the token a restore, a
   put-back and a renewal act only on a record this channel's pop wrote,
   which also closes the renewal problem of point 2
   (`token_renewblind_held_credit` holds `noStranded`). Acks stay by tag:
   the worker that acks has finished the task, so removing another
   delivery's record loses nothing and saves a run. Cost: no round trip;
   one field written in the script or transaction that records the message,
   and one compared in the transaction that restores it.

Ranking, by what each removes and what it risks:

| Design | Removes | Risk on the ranking | Broker cost |
|---|---|---|---|
| kombu PR 2831 (`PopScript`) | the silent loss of a pop whose reply is lost | none | 3 round trips per message, not 4; 2 per empty poll, not 1 |
| publisher confirms | the silent loss of a returned publish | a duplicate after a lost confirm | 1 more frame per publish, and a wait |
| `ackfull` | a worker exit, and the duplicate of a finished task | none | none without a fault |
| `keepwritten` | the warm-shutdown duplicate, and one at every reconnect | none | none |
| `reneweta` | the ETA duplicate | none | none per message |
| `restoreidle` with `CloseRestoresIdle` | the wait for the timeout after an outage, and the duplicate of a task mid-run at a reset | none (needs the active requests from Celery) | none without a fault |
| pop token | the stray restore, and the stranded put-back next to renewal | none | none per round trip |
| `renewall`, `renewidle` | ETA and long-task duplicates | blocking: a hung task and what is prefetched behind it are never redelivered | none per message |
| `restoreall` | the wait for the timeout | more duplicates of running tasks | none without a fault |

`best` (the first seven together, on Redis) is exactly once in `none`,
`shutdown` and `eta`, and has no duplicate under `crash`, `partition`, `cut`
and `reset`. What it leaves: a task longer than the visibility timeout runs
twice (`long`, 7 states: only `RenewAll` removes it, which is blocking); a
killed worker's running task runs again, and one killed between its end and
its ack too (`kill`); an outage longer than the timeout duplicates
(`cutlong`); and with early ack a killed worker loses its running task
(`pr_early_kill`, `neverLost`, 6 states), which is that mode's contract.

## Tied to the code

Every script names its instance and property and prints one line per case.
`PYTHONPATH` selects the tree.

| Script | Shows | On main | With the variant |
|---|---|---|---|
| `../findings/flush-ack-modes/confirm_flush_ack_modes.py` | `flush()` between a job's write and its accept, early and late ack | the child runs the task, the message is never acked (both modes) | acked (both modes) |
| `../findings/renewal-hung/confirm_renewal_hung.py` | a consumer that never acks, visibility timeout 2 s | the other consumer gets both messages | renew-everything prototype: neither |
| `../findings/ack-after-loss/confirm_ack_after_loss.py` | an ack that fails with its connection | delivered again when the channel closes | resend prototype: not delivered again |
| `../findings/putback-renewal/confirm_putback_renewal.py` | kombu PR 2831 with the renewal patch | control: the message is back on the queue | after a renewal: on no queue, in `unacked` |
| `../findings/restore-by-tag/confirm_restore_by_tag.py` | a channel closes after the timeout redelivered its message | a third delivery while the second still runs (main and the PR) | no prototype |
| `../findings/restore-by-tag/natural_restore_by_tag.sh` | the same with three Celery workers and no injected delay | SIGKILL: 12 of 12 run once; SIGTERM: 11 of 12 twice (PR branch: 12 of 12) | no prototype |
| `../adapter/record_transport.py` | recorded Redis command histories of two consumers, checked by `quint test` | 30 of 30 allowed (`BrpopThenRecord`) | 30 of 30 allowed (`PopScript`) |

The recorded histories go from the code to the spec: Redis `MONITOR` gives
every command with the connection that sent it and the commands the pop
script runs, and each history becomes a run whose actions must be enabled
in that order. 20 of the 30 histories of the PR take the step the spec did
not have before (`waitAfterEmpty`).

The ack through a collected channel needs a worker; those runs are in the
reply drafted for celery issue 11054. Spec mutants: 14 for these three specs
(`tools/mutate_spec.py worker`, `publisher`, `transport`), all caught.

## Redelivery: bounded, and every task ends

`redelivery.qnt`, `redelivery_instances.qnt`: one task message, one or two
workers with one pool process each, on a Redis broker or a RabbitMQ
classic or quorum queue, with a coarse clock. A run takes `runLen` units,
and the visibility timeout (Redis) or `consumer_timeout` (RabbitMQ) is
`timeout` units; time passes only when nothing that is due can happen
first (a delivery to a worker with credit, a start, an end, an ack, a
timeout that is due, a restart). The broker's copies project onto
`../lib/delivery.qnt` at every step (`projectionOk`).

The two brokers differ where it matters here:

* Redis: a delivery writes a record under the message's delivery tag, an
  ack removes the record by tag from any connection, and `restore_visible`
  puts a record older than the visibility timeout back on the queue,
  marked redelivered (kombu redis.py:474-560, 1263-1270). A message keeps
  its tag, so the first run's ack removes the record of the copy that was
  delivered while it ran.
* RabbitMQ: a delivery is unacked on its channel, an ack is valid only on
  that channel, and a closed channel returns what it held. A delivery
  unacked past `consumer_timeout` closes the channel; Celery treats the
  channel error as unrecoverable and stops warm (the run goes on), unless
  `broker_channel_error_retry` reconnects it. A quorum queue drops or
  dead-letters a message past its delivery limit.

Instances: `<broker>_<setting>_<case>`; `long` runs take 3 units against a
timeout of 2, `short` ones 1; `w1` is one worker, `p2` a prefetch count
of 2; `nopf` is `worker_disable_prefetch`; `retry` is
`broker_channel_error_retry`; cases `kill` (SIGKILL of a worker), `cut`
(an AMQP connection that stops carrying anything until heartbeats notice),
`loss` (the broker loses a write it acknowledged).

`noLoop` compares the state each delivery starts from, without counters,
ids and the order of the workers: a delivery that starts from a state an
earlier one started from is a cycle that can repeat for ever. The search
stops a run at four deliveries; `quietDone` and `exactlyOnce` say nothing
about a run stopped there.

<!-- generated: properties redelivery.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noLoop` | safety | No task is redelivered without bound: no delivery starts from a state that an earlier delivery started from (counters, ids and the order of the workers aside), so no cycle of deliveries can repeat for ever. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Absence (a delivery from a state an earlier delivery started from), global | docs/getting-started/backends-and-brokers/redis.rst:318-320 (celery 768e476): "if that happens it will be executed again, and again in a loop" (said of ETA, countdown and retry tasks); docs/userguide/calling.rst:318-334 (celery 768e476): the consumer timeout closes the channel |
| `noSilentLoss` | safety | A published task is on the queue, recorded as unacked, or held by a live worker, or its result is final, or the broker dead-lettered it: it is never gone without anyone told. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Universality (a published task is held, final or dead-lettered), global | docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476); redis.rst:262-273 |
| `earlyAtMostOnce` | safety | With early ack a task that started is never executed again. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Bounded existence (at most one run with early ack), global | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again" |
| `noRerunAfterSuccess` | safety | No run of a task starts while its SUCCESS is stored. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Absence (a run that starts while SUCCESS is stored), global | celery/app/trace.py:487-528 (768e476): worker_deduplicate_successful_tasks skips a redelivered acks_late task whose SUCCESS is stored |
| `noFailWithoutError` | safety | A task whose body never raised is never stored FAILURE, and a deduplication check that cannot read the backend does not end the task without running it. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Absence (FAILURE for a task whose body never raised), global | docs/userguide/configuration.rst:3650-3655 (celery 768e476): deduplication skips only a task whose state is SUCCESS |
| `noRerunAfterDone` | safety | No run of a task starts after a run of it finished, whatever is stored then (an expired result included). Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Absence (a run that starts after a run finished), global | docs/userguide/configuration.rst:3650-3655 (celery 768e476) |
| `outcomeVisible` | liveness | A backend error outside the task body (the deduplication check's read, the STARTED write, the SUCCESS write) never acks a task without storing a state a reader can see: a quiet system that the search did not stop, with no result expired and nothing dropped by the broker, has SUCCESS or FAILURE stored. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Response (a quiet system shows SUCCESS or FAILURE), global | celery/app/trace.py:687-696 (768e476): an error outside the body is reported as an internal error and goes to on_error, which stores FAILURE |
| `projectionOk` | safety | Every step changes the broker's copies of the message as a step of the abstract delivery environment does (publish, deliver, ack, requeue, take or lose), so the canvas specs built on that environment cover these brokers. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Universality (every step projects to steps of lib/delivery.qnt), global | lib/delivery.qnt; the same check as worker/worker_projection.qnt |
| `exactlyOnce` | safety | While no fault was used (a timeout, a warm stop and a supervisor's restart are not faults), the task starts once, and a quiet system has stored its SUCCESS and holds no copy. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Bounded existence (exactly one run), while no fault was used | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `quietDone` | liveness | A quiet system that the search did not stop at the delivery cap has stored the task's SUCCESS, or the broker dead-lettered it: every task ends, with the crash budget as the fairness assumption. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Response (lemma 3 of 3: a quiet system has ended) | docs/userguide/optimizing.rst:180-183 (celery 768e476) |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state; a run stops at CFG.cap deliveries. The spec has no closed-form measure. Assumes: fault budgets are finite; at most CFG.cap deliveries; exhaustive searches of small instances, not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state (one where nothing is due and time changes nothing): TLC runs stepOrQuiet with deadlock detection on. Assumes: a step that is due happens before time passes (maximal progress); a restart is never refused. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends without hitting the delivery cap. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverDone` | witness | Some run stores SUCCESS. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverTimedOut` | witness | A restore_visible or a consumer timeout fires. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverTwice` | witness | The task starts twice. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverSkipped` | witness | Deduplication skips a delivery. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
<!-- end generated -->

<!-- generated: results redelivery.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| classic_confirm_loss | 6 | none | neverQuiet (7), neverDone (6) |
| classic_dedup_kill | 11 | none | neverQuiet (7), neverDone (6), neverTwice (7), neverSkipped (9) |
| classic_dedup_long | 23 | exactlyOnce (9) | neverQuiet (24), neverDone (11), neverTimedOut (7), neverTwice (9), neverSkipped (18) |
| classic_early_cut | 11 | earlyAtMostOnce (8), noRerunAfterSuccess (10), noRerunAfterDone (10) | neverQuiet (6), neverDone (6), neverTwice (8) |
| classic_late_cut | 21 | noRerunAfterSuccess (10), noRerunAfterDone (10) | neverQuiet (7), neverDone (6), neverTimedOut (13), neverTwice (8) |
| classic_late_kill | 12 | noRerunAfterSuccess (9), noRerunAfterDone (9) | neverQuiet (7), neverDone (6), neverTwice (7) |
| classic_late_long | 38 | noLoop (26), noRerunAfterSuccess (18), noRerunAfterDone (18), exactlyOnce (9) | neverDone (11), neverTimedOut (7), neverTwice (9) |
| classic_late_short | 6 | none | neverQuiet (7), neverDone (6) |
| classic_noconfirm_loss | 6 | noSilentLoss (3), outcomeVisible (3), quietDone (3) | neverQuiet (3), neverDone (6) |
| classic_retry_long | 32 | noLoop (22), noRerunAfterSuccess (12), noRerunAfterDone (12), exactlyOnce (9) | neverDone (10), neverTimedOut (7), neverTwice (9) |
| quorum_dlx_kill | 14 | noRerunAfterSuccess (9), noRerunAfterDone (9) | neverQuiet (7), neverDone (6), neverTwice (7) |
| quorum_late_kill | 14 | noSilentLoss (6), noRerunAfterSuccess (9), noRerunAfterDone (9), quietDone (8) | neverQuiet (7), neverDone (6), neverTwice (7) |
| quorum_late_long | 29 | noRerunAfterSuccess (18), noRerunAfterDone (18), exactlyOnce (9) | neverQuiet (30), neverDone (11), neverTimedOut (7), neverTwice (9) |
| redis_dedup_expire | 15 | noRerunAfterDone (13) | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11), neverSkipped (12) |
| redis_dedup_kill | 13 | none | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11), neverSkipped (12) |
| redis_dedup_long_w1 | 12 | none | neverQuiet (13), neverDone (9), neverTimedOut (7), neverSkipped (12) |
| redis_dedup_readerr | 14 | noSilentLoss (11), noFailWithoutError (6), noRerunAfterDone (12), outcomeVisible (7), quietDone (11) | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11), neverSkipped (12) |
| redis_dedup_republish | 12 | noRerunAfterSuccess (9), noRerunAfterDone (9) | neverQuiet (7), neverDone (6), neverTwice (7) |
| redis_dedupretry_readerr | 14 | noRerunAfterSuccess (12), noRerunAfterDone (12) | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11), neverSkipped (12) |
| redis_early_kill | 11 | noSilentLoss (5), outcomeVisible (6), quietDone (6) | neverQuiet (6), neverDone (6), neverTimedOut (8) |
| redis_late_backenderr | 6 | noSilentLoss (5), noFailWithoutError (4), outcomeVisible (5), quietDone (5) | neverQuiet (5), neverDone (6) |
| redis_late_kill | 14 | noRerunAfterSuccess (12), noRerunAfterDone (12) | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11) |
| redis_late_long | 15 | exactlyOnce (9) | neverQuiet (16), neverDone (11), neverTimedOut (7), neverTwice (9) |
| redis_late_long_w1 | 33 | noLoop (19), noRerunAfterSuccess (12), noRerunAfterDone (12), exactlyOnce (12) | neverDone (9), neverTimedOut (7), neverTwice (12) |
| redis_late_long_w1p2 | 16 | noRerunAfterSuccess (12), noRerunAfterDone (12), exactlyOnce (12) | neverQuiet (17), neverDone (10), neverTimedOut (7), neverTwice (12) |
| redis_late_loss | 7 | noSilentLoss (3), outcomeVisible (3), quietDone (3) | neverQuiet (3), neverDone (6) |
| redis_late_short | 6 | none | neverQuiet (7), neverDone (6) |
| redis_nopf_long_w1p2 | 33 | noLoop (19), noRerunAfterSuccess (12), noRerunAfterDone (12), exactlyOnce (12) | neverQuiet (17), neverDone (9), neverTimedOut (7), neverTwice (12) |
| redis_retry_backenderr | 6 | none | neverQuiet (7), neverDone (6) |
<!-- end generated -->

What the model says:

* Redis, a run longer than the visibility timeout: with a second worker
  that has credit, the copy is delivered while the first run goes on, the
  first run's ack removes the copy's record, and the task runs twice
  (`redis_late_long`; ceil(run / timeout) times in general). With one
  process and a prefetch count of 1 (`redis_late_long_w1`, the setting
  optimizing.rst recommends with `acks_late`) or `worker_disable_prefetch`
  (`redis_nopf_long_w1p2`), no consumer takes the copy before the first
  run acks, so the copy gets a record of its own, which times out in turn:
  a loop (19 states). A prefetch count of 2 breaks it
  (`redis_late_long_w1p2`), and so does `worker_deduplicate_successful_tasks`
  (`redis_dedup_long_w1`).
* RabbitMQ, a run longer than `consumer_timeout`: the channel closes and
  returns the message, the run finishes and its ack goes to the closed
  channel, and the next delivery does the same: a loop whether the worker
  exits and is restarted (`classic_late_long`, 26 states) or reconnects
  (`classic_retry_long`). Deduplication ends it (`classic_dedup_long`);
  a quorum queue's delivery limit ends it with SUCCESS stored
  (`quorum_late_long`).
* Early ack across a cut AMQP connection runs the task twice: the ack is
  written into the cut and never arrives, and the broker returns the
  delivery when it closes the channel (`classic_early_cut`, 8 states).
* Silent loss, all documented: an early-acked run lost with its worker, a
  Redis that loses a write it acknowledged, an unconfirmed publish to a
  classic queue, a quorum queue's delivery limit reached by crashes with no
  dead letter exchange (`quorum_late_kill`; with one, `quorum_dlx_kill`
  passes).

* `worker_deduplicate_successful_tasks` (`*_dedup_*`): it skips a
  redelivered copy whose SUCCESS is stored and ends both loops. What it
  cannot see: a result that expired (`redis_dedup_expire`, a rerun), a
  copy the publisher sent again after a lost reply, which is not marked
  redelivered (`redis_dedup_republish`), and a read that fails. The check
  lets only `BackendGetMetaError` through, which the backend raises only
  under `result_backend_always_retry`; any other error from the read ends
  the task as an internal error: FAILURE is stored if the backend takes
  the write, nothing if it is down, and the message is acked either way, so
  a task whose first run was lost never runs (`redis_dedup_readerr`, 10
  states). With `result_backend_always_retry` the task runs
  (`redis_dedupretry_readerr`).
* A backend error outside the body is one fault class (`backendErrors`)
  at three points: the deduplication check's read, the STARTED write
  (`task_track_started`) and the SUCCESS write. All three end in the
  tracer's internal-error path and ack the message; none leaves it for
  redelivery. The task never runs (the first two) or ran with nothing
  final stored (the third), FAILURE only if the backend takes that write
  (`redis_late_backenderr`, `outcomeVisible`). `result_backend_always_retry`
  gets past a transient error (`redis_retry_backenderr` passes); at the
  check it lets the task run.
* The prefork pool keeps the "local cache of successfully executed tasks"
  (configuration.rst:3657-3660) in the parent (`task_ready`,
  state.py:215-221), and the check reads it in the child, so it only
  holds ids the child itself skipped. Not modeled: it costs a backend
  read, not correctness.

On the real code (celery 768e476, kombu 445027e, real prefork workers,
nothing injected but the scaled timers unless named):
[redis-visibility-loop](../findings/redis-visibility-loop/) (8 runs in 40 s
with prefetch 1 or `worker_disable_prefetch`, 3 with prefetch 2, 1 with
deduplication), [rabbitmq-consumer-timeout-loop](../findings/rabbitmq-consumer-timeout-loop/)
(6 runs in 60 s with exits, 7 with `broker_channel_error_retry`, 1 with
deduplication, 3 then dropped with SUCCESS on a quorum queue),
[quorum-delivery-limit](../findings/quorum-delivery-limit/),
[early-ack-amqp-cut](../findings/early-ack-amqp-cut/) (an injected cut: 3
of 3 twice), [broker-loss-silent](../findings/broker-loss-silent/) (an
injected restart: 3 of 3 lost, PENDING, nothing logged),
[dedup-read-error](../findings/dedup-read-error/) (a killed first run, then
a failed read: 3 of 3 FAILURE without a run; with the backend down 2 of 2
PENDING for ever, acked), [dedup-republish](../findings/dedup-republish/)
(injected reply loss: 2 of 3 ran twice). The Redis
prefetch-2 run took ceil(5 / 2) = 3 runs where the instance, with a run of
3 units against 2, takes 2: the same bound.

Why each violation is expected:

<!-- generated: reasons redelivery.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noLoop` | redis_late_long_w1 (19), redis_nopf_long_w1p2 (19) | documented | docs/getting-started/backends-and-brokers/redis.rst:318-320 (celery 768e476): "if that happens it will be executed again, and again in a loop" (said of ETA, countdown and retry tasks); finding redis-visibility-loop (confirmed: 8 runs in 40 s with prefetch 1, 3 with prefetch 2); celery issue 5935. a long acks_late task loops too when no consumer has credit for the restored copy before the first run acks: one process with prefetch 1 (the setting optimizing.rst recommends with acks_late), or worker_disable_prefetch; the first run's ack then finds no record of the copy, the copy's own record times out again. With credit elsewhere the first run's ack removes the copy's record (same tag) and the task runs at most ceil(run / timeout) times |
| `noLoop` | classic_late_long (26), classic_retry_long (22) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only; finding rabbitmq-consumer-timeout-loop (confirmed on 3.13: 6 runs in 60 s with exits, 7 with broker_channel_error_retry). each run outlasts consumer_timeout: the channel closes and returns the message, the worker exits (or reconnects with broker_channel_error_retry), the run finishes and its ack goes to the closed channel; worker_deduplicate_successful_tasks ends it |
| `noSilentLoss` | redis_early_kill (5) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again". a worker killed after the early ack: the result stays PENDING |
| `noSilentLoss` | redis_late_loss (3) | documented | docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): "With the default RDB snapshotting you can lose tasks published since the last snapshot". a ready copy or the unacked record lost by a restart without fsync or a failover |
| `noSilentLoss` | classic_noconfirm_loss (3) | documented | docs/userguide/calling.rst:525-527 (celery 768e476): "Messages can still be silently dropped ... Enable confirm_publish"; tracker entry for the Discussion on the confirm_publish default. a classic queue killed before it wrote a publish that was not confirmed; measured in the exactly-once report (3,659 of 5,100 lost without confirms, 0 with) |
| `noSilentLoss` | quorum_late_kill (6) | documented | docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476): "Once the limit is reached, the task is dropped. Its state stays PENDING ... it doesn't log the drop". two kills with a delivery limit of 1 |
| `noSilentLoss` | redis_dedup_readerr (11) | open | finding dedup-read-error (not in the tracker yet; no upstream thread found). the backend is down for the FAILURE write too: nothing stored, the message acked, PENDING for ever |
| `noSilentLoss` | redis_late_backenderr (5) | open | finding dedup-read-error (a backend error outside the body; the lead reproduced the same path for celery issue 9835: STARTED or SUCCESS writes fail, the message is acked, nothing stored, a chain stops). a backend error at the STARTED write (the body never runs) or the SUCCESS write (it ran): internal error, FAILURE if the backend takes that write, the message acked either way; result_backend_always_retry gets past a transient error (redis_retry_backenderr passes) |
| `earlyAtMostOnce` | classic_early_cut (8) | open | tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026); finding early-ack-amqp-cut (confirmed: 3 of 3 with an injected cut). acks written into a connection that is cut never reach RabbitMQ, the worker gets no error, and the broker returns the delivery when it closes the channel; measured 17 to 37 per run with 3 s and 25 s blackholes |
| `noRerunAfterSuccess` | redis_dedupretry_readerr (12) | by design | celery/app/trace.py:494-497 (768e476): with result_backend_always_retry a failed read raises BackendGetMetaError, which the check lets pass, and the task runs. a read that fails while SUCCESS is stored: a rerun, the outcome the check accepts instead of a skip |
| `noRerunAfterSuccess` | redis_dedup_republish (9) | documented | docs/userguide/configuration.rst:3653-3655 (celery 768e476): "Deduplication occurs only with tasks that have the same identifier, enabled late acknowledgment, were redelivered by the message broker and their state is SUCCESS in the result backend". the publisher sent the message again after a lost reply: a copy not marked redelivered; finding dedup-republish (2 of 3 with injected reply loss) |
| `noRerunAfterSuccess` | classic_early_cut (10) | open | tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026) |
| `noRerunAfterSuccess` | redis_late_kill (12) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | classic_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | quorum_dlx_kill (9), quorum_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | classic_late_cut (10) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". a lost connection after the store and before the ack |
| `noRerunAfterSuccess` | redis_late_long_w1 (12), redis_late_long_w1p2 (12), redis_nopf_long_w1p2 (12) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". the copy waits for the first run to end |
| `noRerunAfterSuccess` | classic_late_long (18), classic_retry_long (12) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `noRerunAfterSuccess` | quorum_late_long (18) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `noFailWithoutError` | redis_dedup_readerr (6) | open | finding dedup-read-error (not in the tracker yet; no upstream thread found). the first run was lost before it stored anything; the redelivered copy's check gets the backend's own error (only BackendGetMetaError, raised under result_backend_always_retry, is let through), the tracer reports an internal error, on_error stores FAILURE and the message is acked: confirmed 3 of 3 (readonce: FAILURE) and 2 of 2 with the backend down (PENDING for ever) |
| `noFailWithoutError` | redis_late_backenderr (4) | open | finding dedup-read-error (a backend error outside the body; the lead reproduced the same path for celery issue 9835: STARTED or SUCCESS writes fail, the message is acked, nothing stored, a chain stops). a backend error at the STARTED write (the body never runs) or the SUCCESS write (it ran): internal error, FAILURE if the backend takes that write, the message acked either way; result_backend_always_retry gets past a transient error (redis_retry_backenderr passes) |
| `noRerunAfterDone` | redis_dedup_expire (13) | documented | docs/userguide/configuration.rst:3653-3655 (celery 768e476): "Deduplication occurs only with tasks that have the same identifier, enabled late acknowledgment, were redelivered by the message broker and their state is SUCCESS in the result backend". the result expired (result_expires) before the copy came: the check reads PENDING |
| `noRerunAfterDone` | redis_dedupretry_readerr (12) | by design | celery/app/trace.py:494-497 (768e476): with result_backend_always_retry a failed read raises BackendGetMetaError, which the check lets pass, and the task runs. a read that fails while SUCCESS is stored: a rerun, the outcome the check accepts instead of a skip |
| `noRerunAfterDone` | redis_dedup_republish (9) | documented | docs/userguide/configuration.rst:3653-3655 (celery 768e476): "Deduplication occurs only with tasks that have the same identifier, enabled late acknowledgment, were redelivered by the message broker and their state is SUCCESS in the result backend". the publisher sent the message again after a lost reply: a copy not marked redelivered; finding dedup-republish (2 of 3 with injected reply loss) |
| `noRerunAfterDone` | classic_early_cut (10) | open | tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026) |
| `noRerunAfterDone` | redis_late_kill (12) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterDone` | classic_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterDone` | quorum_dlx_kill (9), quorum_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterDone` | classic_late_cut (10) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". a lost connection after the store and before the ack |
| `noRerunAfterDone` | redis_dedup_readerr (12), redis_late_long_w1 (12), redis_late_long_w1p2 (12), redis_nopf_long_w1p2 (12) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". the copy waits for the first run to end |
| `noRerunAfterDone` | classic_late_long (18), classic_retry_long (12) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `noRerunAfterDone` | quorum_late_long (18) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `outcomeVisible` | redis_early_kill (6) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again". a worker killed after the early ack: the result stays PENDING |
| `outcomeVisible` | redis_late_loss (3) | documented | docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): "With the default RDB snapshotting you can lose tasks published since the last snapshot". a ready copy or the unacked record lost by a restart without fsync or a failover |
| `outcomeVisible` | classic_noconfirm_loss (3) | documented | docs/userguide/calling.rst:525-527 (celery 768e476): "Messages can still be silently dropped ... Enable confirm_publish"; tracker entry for the Discussion on the confirm_publish default. a classic queue killed before it wrote a publish that was not confirmed; measured in the exactly-once report (3,659 of 5,100 lost without confirms, 0 with) |
| `outcomeVisible` | redis_dedup_readerr (7) | open | finding dedup-read-error (not in the tracker yet; no upstream thread found). the backend is down for the FAILURE write too: nothing stored, the message acked, PENDING for ever |
| `outcomeVisible` | redis_late_backenderr (5) | open | finding dedup-read-error (a backend error outside the body; the lead reproduced the same path for celery issue 9835: STARTED or SUCCESS writes fail, the message is acked, nothing stored, a chain stops). a backend error at the STARTED write (the body never runs) or the SUCCESS write (it ran): internal error, FAILURE if the backend takes that write, the message acked either way; result_backend_always_retry gets past a transient error (redis_retry_backenderr passes) |
| `exactlyOnce` | redis_late_long (9), redis_late_long_w1 (12), redis_late_long_w1p2 (12), redis_nopf_long_w1p2 (12) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `exactlyOnce` | classic_dedup_long (9), classic_late_long (9), classic_retry_long (9) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `exactlyOnce` | quorum_late_long (9) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only. the delivery limit (2) ends the loop with SUCCESS stored |
| `quietDone` | redis_early_kill (6) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again" |
| `quietDone` | redis_late_loss (3) | documented | docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): "With the default RDB snapshotting you can lose tasks published since the last snapshot" |
| `quietDone` | classic_noconfirm_loss (3) | documented | docs/userguide/calling.rst:525-527 (celery 768e476): "Messages can still be silently dropped ... Enable confirm_publish"; tracker entry for the Discussion on the confirm_publish default |
| `quietDone` | quorum_late_kill (8) | documented | docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476): "Once the limit is reached, the task is dropped. Its state stays PENDING ... it doesn't log the drop" |
| `quietDone` | redis_dedup_readerr (11) | open | finding dedup-read-error (not in the tracker yet; no upstream thread found). the backend is down for the FAILURE write too: nothing stored, the message acked, PENDING for ever |
| `quietDone` | redis_late_backenderr (5) | open | finding dedup-read-error (a backend error outside the body; the lead reproduced the same path for celery issue 9835: STARTED or SUCCESS writes fail, the message is acked, nothing stored, a chain stops). a backend error at the STARTED write (the body never runs) or the SUCCESS write (it ran): internal error, FAILURE if the backend takes that write, the message acked either way; result_backend_always_retry gets past a transient error (redis_retry_backenderr passes) |
<!-- end generated -->

## Retries

`retry.qnt`, `retry_instances.qnt`: one `acks_late` task with
`max_retries` 1 whose body returns or calls `self.retry(countdown=0)`, two
workers, on the abstract delivery environment of `../lib/delivery.qnt`.
`retry()` publishes the copy (task.py:958-959), then the tracer stores
RETRY (trace.py:191-202) and the worker acks; at the limit FAILURE is
stored; a failed publish is a `Reject(requeue=False)`, which stores
FAILURE (request.py:681-690). Instances `<body>_<fault class>`: `ok` (the
body may return), `fail` (it always retries); faults `node` (a worker
killed), `dup` (a duplicate delivery), `refuse` (the copy's publish
fails). `retryfirst` is a design: store RETRY before publishing the copy.
Fenced writes (item 7): the design `fenced` stores the attempt number with
the state and refuses a write of an older attempt, and an unready state
over a ready one of the same attempt (a per-attempt epoch: a conditional
write in the store, not in celery); the fault `late` is a write that
`result_backend_always_retry` sent again after a lost reply, whose first
send reaches the store after other writes (base.py:827-860).

<!-- generated: properties retry.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `runsBounded` | safety | While no fault was used, the body runs at most max_retries + 1 times. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Bounded existence (at most max_retries + 1 body runs), while no fault was used | docs/userguide/tasks.rst (celery 768e476), Retrying: max_retries is "the maximum number of attempted retries before giving up" |
| `runsBoundedAlways` | safety | The body runs at most max_retries + 1 times, whatever happens. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Bounded existence (at most max_retries + 1 body runs), global | docs/userguide/tasks.rst (celery 768e476), Retrying: max_retries is "the maximum number of attempted retries before giving up" |
| `noRetryAfterSuccess` | safety | No retry is published while the task's SUCCESS is stored. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a retry published while SUCCESS is stored), global | celery/backends/base.py:1361-1370 (768e476): a stored SUCCESS is final |
| `neverLost` | safety | A retried task is held by the broker or a worker until a final state (SUCCESS or FAILURE) is stored; it is never left in RETRY with nothing to run it. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Universality (a retried task is held or final), global | docs/userguide/tasks.rst (celery 768e476), Retrying: max_retries is "the maximum number of attempted retries before giving up" |
| `noStaleWrite` | safety | No write replaces a newer attempt's stored state, and no write replaces a ready state of its own attempt with an unready one. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only; designs fenced (a per-attempt epoch) and retryFirst; the fault lateCopies (a write sent again by result_backend_always_retry reaches the store after other writes). | Absence (a write that replaces a newer attempt's state, or a ready state of its own attempt with an unready one), global | docs/userguide/tasks.rst (celery 768e476), States: SUCCESS and FAILURE are ready states; an attempt's state is about that attempt |
| `quietDone` | liveness | A quiet task has stored SUCCESS or FAILURE: get() returns. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Response (lemma 3 of 3: a quiet task has a final result) | docs/userguide/tasks.rst (celery 768e476), Retrying: max_retries is "the maximum number of attempted retries before giving up" |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
| `neverRetried` | witness | The body runs twice (a retry ran). Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
| `neverFailed` | witness | FAILURE is stored. Assumes: one acks_late task with max_retries 1 (two attempts), each run's body returns or calls self.retry(countdown=0); two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
<!-- end generated -->

<!-- generated: results retry.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| fail_node | 15 | runsBoundedAlways (7), neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (9), neverRetried (5), neverFailed (6) |
| fail_none | 8 | neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (9), neverRetried (5), neverFailed (6) |
| fenced_fail_late | 9 | none | neverQuiet (9), neverRetried (5), neverFailed (6) |
| fenced_fail_none | 8 | none | neverQuiet (9), neverRetried (5), neverFailed (6) |
| fenced_ok_late | 9 | none | neverQuiet (5), neverRetried (5), neverFailed (6) |
| fenced_ok_node | 15 | runsBoundedAlways (7), noRetryAfterSuccess (7) | neverQuiet (5), neverRetried (5), neverFailed (6) |
| fenced_ok_none | 8 | none | neverQuiet (5), neverRetried (5), neverFailed (6) |
| ok_dup | 16 | runsBoundedAlways (9), noRetryAfterSuccess (7), neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (5), neverRetried (5), neverFailed (6) |
| ok_late | 9 | neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (5), neverRetried (5), neverFailed (6) |
| ok_node | 15 | runsBoundedAlways (7), noRetryAfterSuccess (7), neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (5), neverRetried (5), neverFailed (6) |
| ok_none | 8 | neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (5), neverRetried (5), neverFailed (6) |
| ok_refuse | 8 | neverLost (9), noStaleWrite (7), quietDone (9) | neverQuiet (5), neverRetried (5), neverFailed (4) |
| retryfirst_fail | 8 | none | neverQuiet (9), neverRetried (6), neverFailed (7) |
| retryfirst_late | 9 | neverLost (10), noStaleWrite (8), quietDone (10) | neverQuiet (5), neverRetried (6), neverFailed (7) |
| retryfirst_node | 15 | runsBoundedAlways (8), noRetryAfterSuccess (8), noStaleWrite (10) | neverQuiet (5), neverRetried (5), neverFailed (7) |
| retryfirst_none | 8 | none | neverQuiet (5), neverRetried (6), neverFailed (7) |
<!-- end generated -->

What the model says:

* Without any fault the copy can run on another process and store its
  final state before the original stores RETRY, and RETRY replaces it: the
  task reads RETRY for ever (`ok_none`, `fail_none`, 9 states). The
  key-value stores refuse to replace SUCCESS only (base.py:1369-1370).
  On the real code it takes a slow RETRY write: 4 of 20 with a 0.2 s
  delay, 0 of 2000 without one
  ([retry-over-failure](../findings/retry-over-failure/)). Storing RETRY
  first (`retryfirst_*`) passes every property.
* A worker lost between the copy's publish and the ack, or a duplicate
  delivery, starts a second lineage: `max_retries` counts per message, and
  the body runs up to twice `max_retries + 1` times, a retry can be
  published after SUCCESS is stored (`*_node`, `*_dup`; documented at
  least once). On the real code: 4 runs with `max_retries` 1, 3 of 3
  ([retry-lineages](../findings/retry-lineages/)).
* A refused publish stores FAILURE: visible, not lost (`ok_refuse`).
* No write may replace a newer attempt's state (`noStaleWrite`). Without
  any fault the original's RETRY replaces the copy's final state
  (`ok_none`, `fail_none`: the trigger of retry-over-failure). A late copy
  of a write breaks `retryfirst` too (`retryfirst_late`): storing RETRY
  first orders the writes only while each write lands once. The fenced
  design passes `noStaleWrite`, `neverLost` and `quietDone` in every
  instance, with or without a late copy (`fenced_*`). `retryfirst_late` is
  not run on the real code: it needs a write to reach Redis after the
  client gave up on it and sent it again.
* Child recycling (`max_tasks_per_child`) and autoscale are not modeled:
  the real runs in the exactly-once report (`worker_max_tasks_per_child=7`,
  autoscale 6,1 with load waves) ran every task once.

Why each violation is expected:

<!-- generated: reasons retry.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `runsBoundedAlways` | fail_node (7), fenced_ok_node (7), ok_node (7), retryfirst_node (8) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack a task that already started "will be retried in the event of a power failure or the worker instance being killed abruptly". a worker killed after retry() published the copy and before the ack: the original is redelivered and retries again, so two lineages run; with max_retries 1 the body runs 3 times |
| `runsBoundedAlways` | ok_dup (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a message can be delivered again. the same, from a duplicate delivery |
| `noRetryAfterSuccess` | fenced_ok_node (7), ok_node (7), retryfirst_node (8) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack a task that already started "will be retried in the event of a power failure or the worker instance being killed abruptly". the other lineage succeeded first |
| `noRetryAfterSuccess` | ok_dup (7) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a message can be delivered again. the other lineage succeeded first |
| `neverLost` | ok_dup (9), ok_late (9), ok_node (9), ok_none (9), ok_refuse (9) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681. no fault: the copy runs on another process and stores its final state before the original stores RETRY, which replaces it; the task reads RETRY for ever |
| `neverLost` | fail_node (9), fail_none (9) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681. no fault: the copy fails at max_retries and stores FAILURE before the original's RETRY write, which replaces it |
| `neverLost` | retryfirst_late (10) | open | model only, not run on the real code: result_backend_always_retry sends a write again after a lost reply (base.py:827-860, celery 768e476); a copy that reaches the store after the next attempt's final state replaces it; storing RETRY first does not help, the fenced design does. the original's RETRY lands again after the copy's final state |
| `noStaleWrite` | ok_dup (7), ok_late (7), ok_node (7), ok_none (7), ok_refuse (7) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681. the original's RETRY (attempt 0) replaces the copy's final state (attempt 1) |
| `noStaleWrite` | fail_node (7), fail_none (7) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681. the same, over FAILURE |
| `noStaleWrite` | retryfirst_late (8) | open | model only, not run on the real code: result_backend_always_retry sends a write again after a lost reply (base.py:827-860, celery 768e476); a copy that reaches the store after the next attempt's final state replaces it; storing RETRY first does not help, the fenced design does |
| `noStaleWrite` | retryfirst_node (10) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack a task that already started "will be retried in the event of a power failure or the worker instance being killed abruptly". a second lineage after a lost worker: its attempt 0 writes after the first lineage's attempt 1 |
| `quietDone` | ok_dup (9), ok_late (9), ok_node (9), ok_none (9), ok_refuse (9) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681 |
| `quietDone` | fail_node (9), fail_none (9) | open | finding retry-over-failure; the same class as tracker es-ready-states (a late RETRY replaces a final FAILURE), in the reply on celery discussion 10681 |
| `quietDone` | retryfirst_late (10) | open | model only, not run on the real code: result_backend_always_retry sends a write again after a lost reply (base.py:827-860, celery 768e476); a copy that reaches the store after the next attempt's final state replaces it; storing RETRY first does not help, the fenced design does. the original's RETRY lands again after the copy's final state |
<!-- end generated -->

## Revoke, expiry, shutdown and the pools

`revoke.qnt`, `revoke_instances.qnt`: task 1 may be revoked (one
`app.control.revoke()`, by id or by stamped header) and may carry
`expires`; task 2, in some instances, is an ordinary task beside it. Two
workers with one pool process each, on the abstract delivery environment
of `../lib/delivery.qnt`. A request is reserved when the strategy received
it (strategy.py:162 checks the revoked set and `expires`), handed to the
pool when `execute_using_pool` checks them again (request.py:1003-1006),
and running when a pool process accepted it; nothing checks them there. A
discard stores REVOKED and acks (request.py:464-475). The revoke is a
broadcast that reaches a worker only while its consumer is connected; the
worker adds the id to its set and stores REVOKED unless the task is
active there or already started (control.py:221-246); by stamped header
it stores nothing. The set is written to the state db at exit only
(components.py:216-218) and mingle copies it from the other workers at
every consumer start (mingle.py:36-80). Faults and operator actions: a
lost broker connection (`cut`; reserved requests dropped, runs go on,
their messages go back), SIGKILL (`kill`), a warm, soft or cold shutdown
(`halt`; the warm-shutdown flush is taken as fixed by celery PR 11076, so
a job is either with a child or dropped), the broadcast sent twice
(`ctldup`), and a backend error in a discard's REVOKED write
(`backenderr`, the `backendErrors` class of `redelivery.qnt`). Designs:
`checkaccept` (`on_accepted` reads the revoked set), `syncrevoke` (write
the state db when a revoke arrives), `sticky` (a store that keeps every
final state).

<!-- generated: properties revoke.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noRunAfterRevokeKnown` | safety | No run of task 1 starts in a worker whose revoked set holds its id. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a run of a revoked task starting in a worker that knows the revoke), global | docs/userguide/workers.rst (celery 768e476), revoke: "When a worker receives a revoke request it will skip executing the task" |
| `noRunAfterExpiry` | safety | No run of task 1 starts after its expires time passed. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a run starting after expires), global | docs/userguide/calling.rst:363-364 (celery 768e476): "When a worker receives an expired task it will mark the task as REVOKED"; celery/app/task.py:603-604: "seconds in the future for the task should expire" |
| `noRunMissingRevoke` | safety | Once the revoke was sent, no run of task 1 starts in a worker that neither knows the revoke nor still has the broadcast to read: no worker misses or forgets a revoke. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a run, after the revoke was sent, in a worker that neither knows it nor has it to read), global | docs/userguide/workers.rst (celery 768e476), revoke: "All worker nodes keeps a memory of revoked task ids"; Persistent revokes: workers synchronize revoked tasks at start up |
| `revokeSurvivesRestart` | safety | With --statedb, a worker that knew the revoke when it stopped knows it after its restart. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a restart with --statedb that forgets a known revoke), global | docs/userguide/workers.rst (celery 768e476), Persistent revokes: "If you want to preserve this list between restarts you need to specify a file for these to be stored in by using the --statedb argument" |
| `noParallelRuns` | safety | A lost connection never leaves two runs of one task at once in one worker. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (two runs of one task at once in one worker), global | celery/worker/consumer/consumer.py:489-499 (celery 768e476) and the CANCEL_TASKS_BY_DEFAULT warning: cancelling late-ack runs on connection loss is to become the default in Celery 6.0 |
| `finalStable` | safety | A stored final state (SUCCESS, REVOKED) is never replaced by another state: stored states move one way. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a final state replaced), global | celery/states.py (celery 768e476): READY_STATES are final; celery/backends/base.py:1361-1370 keeps SUCCESS only |
| `ctlOnce` | safety | Each worker applies the revoke command at most once. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Bounded existence (each worker applies the command at most once), global | the item: a control command over the pidbox is applied at most once per worker |
| `noRerunAfterSuccess` | safety | No task starts while its SUCCESS is stored. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Absence (a run starting while SUCCESS is stored), global | docs/userguide/configuration.rst (celery 768e476), worker_deduplicate_successful_tasks: a task that succeeded is not to run again |
| `outcomeVisible` | safety | A quiet system shows SUCCESS or REVOKED for every task: no task is left PENDING, STARTED or RETRY with nothing to finish it. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Universality (a quiet system shows a final state for every task), after quiet | docs/userguide/tasks.rst (celery 768e476), States: a task ends in a ready state |
| `quietDone` | liveness | A quiet system has settled every message: nothing is held unacked for ever. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Response (lemma 3 of 3: a quiet system has settled every message) | the spec: lib/delivery.qnt |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
| `neverRan` | witness | Task 1 runs. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
| `neverRevokedStored` | witness | REVOKED is stored for task 1. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
| `neverRevokeApplied` | witness | Some worker applies the revoke. Assumes: task 1 may be revoked once (by id or by stamped header) and may carry expires, task 2 (in some instances) is an ordinary task; two workers with one pool process each (two in s2); the abstract delivery environment (lib/delivery.qnt); a key-value result store that refuses to replace SUCCESS only. | Existence | sanity |
<!-- end generated -->

<!-- generated: results revoke.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| cancel_cut_s2 | 13 | noRerunAfterSuccess (10) | neverQuiet (7), neverRan (5) |
| checkaccept_expire | 7 | none | neverQuiet (4), neverRan (5), neverRevokedStored (4) |
| checkaccept_revoke | 11 | finalStable (9) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| early_halt | 11 | outcomeVisible (9) | neverQuiet (7), neverRan (5) |
| expire_backenderr | 17 | noRunAfterExpiry (6), noRerunAfterSuccess (13) | neverQuiet (10), neverRan (5), neverRevokedStored (4) |
| expire_backenderr_db | 14 | noRunAfterExpiry (6) | neverQuiet (10), neverRan (5), neverRevokedStored (4) |
| expire_backenderr_retry | 13 | noRunAfterExpiry (6) | neverQuiet (10), neverRan (5), neverRevokedStored (4) |
| expire_dup | 13 | noRunAfterExpiry (6), finalStable (9), noRerunAfterSuccess (10) | neverQuiet (4), neverRan (5), neverRevokedStored (4) |
| late_ctldup | 16 | noRunAfterRevokeKnown (7), finalStable (9), ctlOnce (7) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| late_cut_s2 | 13 | noParallelRuns (10), noRerunAfterSuccess (10) | neverQuiet (7), neverRan (5) |
| late_expire | 7 | noRunAfterExpiry (6) | neverQuiet (4), neverRan (5), neverRevokedStored (4) |
| late_halt | 13 | none | neverQuiet (7), neverRan (5) |
| late_revoke | 11 | noRunAfterRevokeKnown (7), finalStable (9) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| late_revoke_cut | 18 | noRunAfterRevokeKnown (7), noRunMissingRevoke (8), finalStable (9), noRerunAfterSuccess (10) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| late_revoke_cut_nomingle | 18 | noRunAfterRevokeKnown (7), noRunMissingRevoke (8), finalStable (9), noRerunAfterSuccess (10) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| late_revoke_started | 11 | noRunAfterRevokeKnown (7), finalStable (8) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| stamp_statedb_halt | 16 | noRunAfterRevokeKnown (7), noRunMissingRevoke (9), revokeSurvivesRestart (7) | neverQuiet (6), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| statedb_halt | 18 | noRunAfterRevokeKnown (7), noRunMissingRevoke (9), finalStable (9) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| statedb_kill | 17 | noRunAfterRevokeKnown (7), noRunMissingRevoke (8), revokeSurvivesRestart (6), finalStable (9), noRerunAfterSuccess (10) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| sticky_checkaccept_revoke | 11 | none | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| syncrevoke_statedb_kill | 17 | noRunAfterRevokeKnown (7), noRunMissingRevoke (8), finalStable (9), noRerunAfterSuccess (10) | neverQuiet (8), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
| threads_revoke | 17 | noRunAfterRevokeKnown (7), finalStable (9) | neverQuiet (14), neverRan (5), neverRevokedStored (5), neverRevokeApplied (4) |
<!-- end generated -->

What the model says, and what the real code did:

* A revoke or an expiry that comes after the request was handed to the
  pool does not stop it (`noRunAfterRevokeKnown`, `noRunAfterExpiry`, 6
  and 7 states). With prefork the semaphore keeps that short, except after
  a reconnect, which clears it while stale runs hold the processes. The
  threads, gevent and eventlet pools have no semaphore, so every
  prefetched request waits in the pool after its last check: on the real
  code a revoked or expired task waiting behind another ran and stored
  SUCCESS, threads 2 of 2 each, gevent 2 of 2; prefork and solo discarded
  it ([revoke-queued-in-pool](../findings/revoke-queued-in-pool/)).
  Checking in `on_accepted` (`checkaccept_*`) passes both properties.
* A backend error in a discard's REVOKED write leaves before the ack. A
  Redis backend on a Redis broker raises redis' ConnectionError, which the
  consumer takes for a lost broker connection: it reconnects and puts
  back every unacked message, so finished and running tasks run again
  (`expire_backenderr.noRerunAfterSuccess`, 13 states); any other error
  stops the worker. On the real code: injected 3 of 3 reruns, a real
  backend outage 84 reconnects and a rerun in 1 of 2, a RuntimeError 2 of
  2 exits; `result_backend_always_retry` removes it
  ([revoke-backend-error](../findings/revoke-backend-error/); celery issue
  6238 has the exit).
* `--statedb` is written at exit only: after SIGKILL and a restart the
  revoke is gone and the task runs (`statedb_kill.revokeSurvivesRestart`,
  6 states; real code 2 of 2, SIGTERM 2 of 2 discards;
  [revoke-statedb-kill](../findings/revoke-statedb-kill/)). Writing the db
  when a revoke arrives (`syncrevoke_*`) passes it.
* A worker that is reconnecting, restarting or not started when the
  broadcast is sent never learns of it unless mingle finds a worker that
  did (`noRunMissingRevoke`; celery issue 8888).
* A cold shutdown cancels an early-ack run and stores RETRY; the message
  was acked at the start, so the task reads RETRY for ever
  (`early_halt.outcomeVisible`, 9 states; real code 2 of 2,
  [cold-early-retry](../findings/cold-early-retry/)). A late-ack run's
  message goes back and runs again (`late_halt` passes).
* REVOKED is replaced: the revoke command stores it while another worker
  runs the task without `task_track_started`, and the run stores SUCCESS
  over it (documented in workers.rst), or with it STARTED over REVOKED in
  the window between the command's read and write (`finalStable`). Every
  store allows REVOKED over FAILURE and SUCCESS over REVOKED
  (`../result/resultstate.qnt`, the `_rev` instances); a store that keeps
  every final state (`sticky_checkaccept_revoke`) passes all properties.
* A lost connection leaves two runs of one task in one worker with two
  processes (`late_cut_s2.noParallelRuns`, documented in the warning that
  `worker_cancel_long_running_tasks_on_connection_loss` becomes the default
  in 6.0); with the setting it passes.
* The broadcast is published with `retry=True` (kombu pidbox.py:307-330),
  so a lost reply can send it twice (`late_ctldup.ctlOnce`); revoke,
  rate_limit and shutdown are idempotent. A broadcast with `reply=True`
  returns after its timeout whatever arrived (pidbox.py:370-400): the
  caller always returns. Not run on the real code.

Which worker properties carry over to the other pools (celery 768e476):

| Pool | Hand-off to the pool | Revoke and expiry checked | Early ack | Two runs in one worker after a reconnect | Cold shutdown |
|---|---|---|---|---|---|
| prefork | when a process is free (semaphore, prefork.py:155, worker.py:288-289) | at receipt and at the hand-off; not after a reconnect clears the semaphore (consumer.py:585-586) | when the child accepts (request.py:524-533) | yes with -c 2 or more, unless the cancel setting is on | children terminated (request.cancel) |
| threads | at once (no semaphore, base.py:68; thread.py:45-56) | at receipt only: a request waiting in the executor is not checked (confirmed) | when the thread starts it (accept_callback) | yes | no terminate_job (celery issue 10490; celery PR 10491, 11065 open) |
| gevent, eventlet | at once (pool spawn, gevent.py:116-126, eventlet.py:123-135) | at receipt only (gevent confirmed; eventlet not installed here) | when the greenlet starts it | yes | greenlets killed (gevent.py _make_killable_target; celery PR 11065 open) |
| solo | runs inside execute_using_pool (solo.py), the consumer blocked | right before the run (confirmed: discarded) | at the start | no: one run at a time | not read here |

The model sees only the hand-off (`semaphore: false`, `threads_revoke`);
the rest (state db, mingle, the discard's backend error, the stored
states) is the same code in the main process for every pool.

Why each violation is expected:

<!-- generated: reasons revoke.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noRunAfterRevokeKnown` | late_ctldup (7), late_revoke (7), late_revoke_cut (7), late_revoke_cut_nomingle (7), late_revoke_started (7), stamp_statedb_halt (7), statedb_halt (7), statedb_kill (7), syncrevoke_statedb_kill (7), threads_revoke (7) | open | finding revoke-queued-in-pool. the revoke arrives after execute_using_pool handed the job to the pool and before a process started it; nothing checks the revoked set there. Prefork: the hand-off, and after a reconnect the whole wait; threads and gevent: the whole wait in the pool |
| `noRunAfterExpiry` | expire_backenderr (6), expire_backenderr_db (6), expire_backenderr_retry (6), expire_dup (6), late_expire (6) | open | finding revoke-queued-in-pool. expires passes after the hand-off to the pool; calling.rst:363-364 promises the check when the worker receives the task only |
| `noRunMissingRevoke` | stamp_statedb_halt (9) | documented | docs/userguide/workers.rst (celery 768e476), revoke_by_stamped_headers: "The revoked headers mapping is not persistent across restarts" |
| `noRunMissingRevoke` | late_revoke_cut (8), late_revoke_cut_nomingle (8), statedb_halt (9), statedb_kill (8), syncrevoke_statedb_kill (8) | open | celery issue 8888 (open): a revoke sent while a worker is offline is lost; the broadcast reaches only connected consumers (kombu pidbox.py:258-273, redis.py:1704-1716), and mingle copies it only from a worker that has it. a worker reconnecting, restarted, or killed when the broadcast was sent |
| `revokeSurvivesRestart` | stamp_statedb_halt (7) | documented | docs/userguide/workers.rst (celery 768e476), revoke_by_stamped_headers: "The revoked headers mapping is not persistent across restarts" |
| `revokeSurvivesRestart` | statedb_kill (6) | open | finding revoke-statedb-kill. the state db is written at exit only (components.py:216-218) |
| `noParallelRuns` | late_cut_s2 (10) | documented | celery/worker/consumer/consumer.py CANCEL_TASKS_BY_DEFAULT (celery 768e476): late-ack tasks are redelivered on connection loss unless worker_cancel_long_running_tasks_on_connection_loss, the default from Celery 6.0. the run goes on and its redelivered copy starts beside it |
| `finalStable` | checkaccept_revoke (9), expire_dup (9), late_ctldup (9), late_revoke (9), late_revoke_cut (9), late_revoke_cut_nomingle (9), late_revoke_started (8), statedb_halt (9), statedb_kill (9), syncrevoke_statedb_kill (9), threads_revoke (9) | documented | docs/userguide/workers.rst (celery 768e476), revoke: "a running task shows PENDING, so it's marked REVOKED and then stores its result when it's done". REVOKED stored by a worker that does not run the task, then the run stores STARTED or SUCCESS over it; every store allows it (resultstate.qnt rtw_rev, mongo_rev, sql_rev, es_rev) |
| `ctlOnce` | late_ctldup (7) | by design | kombu pidbox.py:307-330 (kombu 445027e): the broadcast publishes with retry=True, so a lost reply can send it twice; revoke, rate_limit and shutdown are idempotent. not run on the real code |
| `noRerunAfterSuccess` | expire_backenderr (13) | open | finding revoke-backend-error |
| `noRerunAfterSuccess` | expire_dup (10) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a message can be delivered again |
| `noRerunAfterSuccess` | statedb_kill (10), syncrevoke_statedb_kill (10) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): late ack reruns a task whose worker was killed |
| `noRerunAfterSuccess` | cancel_cut_s2 (10), late_cut_s2 (10), late_revoke_cut (10), late_revoke_cut_nomingle (10) | documented | celery/worker/consumer/consumer.py CANCEL_TASKS_BY_DEFAULT (celery 768e476): late-ack tasks are redelivered on connection loss. a run finished and stored SUCCESS, its ack was not sent before the connection went |
| `outcomeVisible` | early_halt (9) | open | finding cold-early-retry |
<!-- end generated -->

## Where this stopped

* No spec-to-code replay adapter for `worker.qnt` or `publisher.qnt`: the
  scripts above cover the counterexamples of designs 1 to 3 and the two
  restore findings, not arbitrary traces.
* No prototype of the pop token, `CloseRestoresIdle` or
  `ReconnectRestoresIdle` in kombu: the model is the only evidence that
  they are safe.
* The publisher model was not run against RabbitMQ; its results match the
  runs in the exactly-once report (191 of 2,280 lost without confirms, none
  with).
* The model cannot say why the warm-shutdown duplicate is rarer with early
  ack: it has no time.
