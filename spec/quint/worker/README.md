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
| `projectionOk` | safety | Every step changes the broker's copies of the message as a step of the abstract delivery environment does (publish, deliver, ack, requeue, take or lose), so the canvas specs built on that environment cover these brokers. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Universality (every step projects to steps of lib/delivery.qnt), global | lib/delivery.qnt; the same check as worker/worker_projection.qnt |
| `exactlyOnce` | safety | While no fault was used (a timeout, a warm stop and a supervisor's restart are not faults), the task starts once, and a quiet system has stored its SUCCESS and holds no copy. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Bounded existence (exactly one run), while no fault was used | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `quietDone` | liveness | A quiet system that the search did not stop at the delivery cap has stored the task's SUCCESS, or the broker dead-lettered it: every task ends, with the crash budget as the fairness assumption. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Response (lemma 3 of 3: a quiet system has ended) | docs/userguide/optimizing.rst:180-183 (celery 768e476) |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state; a run stops at CFG.cap deliveries. The spec has no closed-form measure. Assumes: fault budgets are finite; at most CFG.cap deliveries; exhaustive searches of small instances, not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state (one where nothing is due and time changes nothing): TLC runs stepOrQuiet with deadlock detection on. Assumes: a step that is due happens before time passes (maximal progress); a restart is never refused. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends without hitting the delivery cap. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverDone` | witness | Some run stores SUCCESS. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverTimedOut` | witness | A restore_visible or a consumer timeout fires. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
| `neverTwice` | witness | The task starts twice. Assumes: one task message; one or two workers with one pool process each; a coarse clock (run length and timeout in units; maximal progress); a Redis broker, a RabbitMQ classic or quorum queue. | Existence | sanity |
<!-- end generated -->

<!-- generated: results redelivery.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| classic_confirm_loss | 6 | none | neverQuiet (7), neverDone (6) |
| classic_dedup_long | 23 | exactlyOnce (9) | neverQuiet (24), neverDone (11), neverTimedOut (7), neverTwice (9) |
| classic_early_cut | 11 | earlyAtMostOnce (8), noRerunAfterSuccess (10) | neverQuiet (6), neverDone (6), neverTwice (8) |
| classic_late_cut | 21 | noRerunAfterSuccess (10) | neverQuiet (7), neverDone (6), neverTimedOut (13), neverTwice (8) |
| classic_late_kill | 12 | noRerunAfterSuccess (9) | neverQuiet (7), neverDone (6), neverTwice (7) |
| classic_late_long | 38 | noLoop (26), noRerunAfterSuccess (18), exactlyOnce (9) | neverDone (11), neverTimedOut (7), neverTwice (9) |
| classic_late_short | 6 | none | neverQuiet (7), neverDone (6) |
| classic_noconfirm_loss | 6 | noSilentLoss (3), quietDone (3) | neverQuiet (3), neverDone (6) |
| classic_retry_long | 32 | noLoop (22), noRerunAfterSuccess (12), exactlyOnce (9) | neverDone (10), neverTimedOut (7), neverTwice (9) |
| quorum_dlx_kill | 14 | noRerunAfterSuccess (9) | neverQuiet (7), neverDone (6), neverTwice (7) |
| quorum_late_kill | 14 | noSilentLoss (6), noRerunAfterSuccess (9), quietDone (8) | neverQuiet (7), neverDone (6), neverTwice (7) |
| quorum_late_long | 29 | noRerunAfterSuccess (18), exactlyOnce (9) | neverQuiet (30), neverDone (11), neverTimedOut (7), neverTwice (9) |
| redis_dedup_long_w1 | 12 | none | neverQuiet (13), neverDone (9), neverTimedOut (7) |
| redis_early_kill | 11 | noSilentLoss (5), quietDone (6) | neverQuiet (6), neverDone (6), neverTimedOut (8) |
| redis_late_kill | 14 | noRerunAfterSuccess (12) | neverQuiet (7), neverDone (6), neverTimedOut (8), neverTwice (11) |
| redis_late_long | 15 | exactlyOnce (9) | neverQuiet (16), neverDone (11), neverTimedOut (7), neverTwice (9) |
| redis_late_long_w1 | 33 | noLoop (19), noRerunAfterSuccess (12), exactlyOnce (12) | neverDone (9), neverTimedOut (7), neverTwice (12) |
| redis_late_long_w1p2 | 16 | noRerunAfterSuccess (12), exactlyOnce (12) | neverQuiet (17), neverDone (10), neverTimedOut (7), neverTwice (12) |
| redis_late_loss | 7 | noSilentLoss (3), quietDone (3) | neverQuiet (3), neverDone (6) |
| redis_late_short | 6 | none | neverQuiet (7), neverDone (6) |
| redis_nopf_long_w1p2 | 33 | noLoop (19), noRerunAfterSuccess (12), exactlyOnce (12) | neverQuiet (17), neverDone (9), neverTimedOut (7), neverTwice (12) |
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

Why each violation is expected:

<!-- generated: reasons redelivery.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noLoop` | redis_late_long_w1 (19), redis_nopf_long_w1p2 (19) | documented | docs/getting-started/backends-and-brokers/redis.rst:318-320 (celery 768e476): "if that happens it will be executed again, and again in a loop" (said of ETA, countdown and retry tasks). a long acks_late task loops too when no consumer has credit for the restored copy before the first run acks: one process with prefetch 1 (the setting optimizing.rst recommends with acks_late), or worker_disable_prefetch; the first run's ack then finds no record of the copy, the copy's own record times out again. With credit elsewhere the first run's ack removes the copy's record (same tag) and the task runs at most ceil(run / timeout) times |
| `noLoop` | classic_late_long (26), classic_retry_long (22) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only. each run outlasts consumer_timeout: the channel closes and returns the message, the worker exits (or reconnects with broker_channel_error_retry), the run finishes and its ack goes to the closed channel; worker_deduplicate_successful_tasks ends it |
| `noSilentLoss` | redis_early_kill (5) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again". a worker killed after the early ack: the result stays PENDING |
| `noSilentLoss` | redis_late_loss (3) | documented | docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): "With the default RDB snapshotting you can lose tasks published since the last snapshot". a ready copy or the unacked record lost by a restart without fsync or a failover |
| `noSilentLoss` | classic_noconfirm_loss (3) | documented | docs/userguide/calling.rst:525-527 (celery 768e476): "Messages can still be silently dropped ... Enable confirm_publish"; tracker entry for the Discussion on the confirm_publish default. a classic queue killed before it wrote a publish that was not confirmed; measured in the exactly-once report (3,659 of 5,100 lost without confirms, 0 with) |
| `noSilentLoss` | quorum_late_kill (6) | documented | docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476): "Once the limit is reached, the task is dropped. Its state stays PENDING ... it doesn't log the drop". two kills with a delivery limit of 1 |
| `earlyAtMostOnce` | classic_early_cut (8) | open | tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026). acks written into a connection that is cut never reach RabbitMQ, the worker gets no error, and the broker returns the delivery when it closes the channel; measured 17 to 37 per run with 3 s and 25 s blackholes |
| `noRerunAfterSuccess` | classic_early_cut (10) | open | tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026) |
| `noRerunAfterSuccess` | redis_late_kill (12) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | classic_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | quorum_dlx_kill (9), quorum_late_kill (9) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". killed after the store and before the ack |
| `noRerunAfterSuccess` | classic_late_cut (10) | documented | docs/userguide/optimizing.rst:180-183 (celery 768e476): with late ack "a task that's already started executing will be retried in the event of a power failure or the worker instance being killed abruptly". a lost connection after the store and before the ack |
| `noRerunAfterSuccess` | redis_late_long_w1 (12), redis_late_long_w1p2 (12), redis_nopf_long_w1p2 (12) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed". the copy waits for the first run to end |
| `noRerunAfterSuccess` | classic_late_long (18), classic_retry_long (12) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `noRerunAfterSuccess` | quorum_late_long (18) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `exactlyOnce` | redis_late_long (9), redis_late_long_w1 (12), redis_late_long_w1p2 (12), redis_nopf_long_w1p2 (12) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `exactlyOnce` | classic_dedup_long (9), classic_late_long (9), classic_retry_long (9) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only |
| `exactlyOnce` | quorum_late_long (9) | open | tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); docs/userguide/calling.rst:318-334 (celery 768e476) documents the channel close for countdowns only. the delivery limit (2) ends the loop with SUCCESS stored |
| `quietDone` | redis_early_kill (6) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack is sent "just before it's executed, so that a task invocation that already started is never executed again" |
| `quietDone` | redis_late_loss (3) | documented | docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): "With the default RDB snapshotting you can lose tasks published since the last snapshot" |
| `quietDone` | classic_noconfirm_loss (3) | documented | docs/userguide/calling.rst:525-527 (celery 768e476): "Messages can still be silently dropped ... Enable confirm_publish"; tracker entry for the Discussion on the confirm_publish default |
| `quietDone` | quorum_late_kill (8) | documented | docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476): "Once the limit is reached, the task is dropped. Its state stays PENDING ... it doesn't log the drop" |
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
