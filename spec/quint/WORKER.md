# Workers, the publisher and the Redis transport's designs

`worker.qnt`, `publisher.qnt` and the renewal and pop-token variants of
`redis_transport.qnt`: is a task run exactly once without a fault, at least
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
`ReconnectRestoresIdle` and `ByPop` together. `gen_worker_instances.py`
writes `worker_instances.qnt`.

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

## Results

`python3 check.py --tier full --only worker` (and `publisher`,
`transport`): every reachable state of every instance. A number is the
states in a shortest counterexample; `ok` means the property holds in every
reachable state. In every instance a run stops only in a quiet state.
`python3 design_tables.py` prints these tables from `expected.json`.

`ackfull`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 21 | ok | ok | ok | 10 | ok | ok | ok |
| late, crash | 26 | ok | ok | ok | 12 | ok | ok | ok |
| late, cut | 21 | ok | ok | ok | 10 | ok | ok | ok |
| late, cutlong | 27 | 7 | ok | ok | 7 | 8 | ok | ok |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok |

`best`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 17 | ok | ok | ok | ok | ok | ok | ok |
| early, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| early, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok |
| late, crash | 20 | ok | ok | ok | ok | ok | ok | ok |
| late, cut | 17 | ok | ok | ok | ok | ok | ok | ok |
| late, cutlong | 23 | 7 | ok | ok | 7 | 8 | ok | ok |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 18 | ok | ok | ok | ok | ok | ok | ok |
| late, reset | 16 | ok | ok | ok | ok | ok | ok | ok |
| late, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok |

`keepwritten`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 28 | ok | ok | ok | 10 | ok | ok | 7 |
| early, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok |
| late, cut | 30 | ok | ok | ok | 12 | ok | ok | 9 |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok |
| late, shutdown | 17 | ok | ok | ok | ok | ok | ok | ok |

`main`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| early, partition | 21 | ok | 4 | 11 | 10 | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | 4 | 11 | 10 | ok | ok | 9 |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |

`ownrecord`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| late, etashutdown | 26 | 9 | ok | ok | 9 | ok | ok | ok |

`pr`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 27 | ok | ok | ok | 10 | ok | ok | ok |
| early, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok |
| early, hang | 13 | ok | 6 | 12 | ok | ok | ok | ok |
| early, kill | 19 | ok | 6 | 13 | 8 | ok | ok | ok |
| early, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |
| early, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| early, reset | 18 | ok | ok | ok | 7 | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 |
| late, cut | 30 | ok | ok | ok | 10 | ok | ok | 9 |
| late, cutlong | 36 | 7 | ok | ok | 7 | 8 | ok | 9 |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |

`renewall`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| early, long | 12 | ok | ok | ok | ok | ok | ok | ok |
| late, cutlong | 30 | ok | ok | ok | 8 | 8 | ok | 9 |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 12 | ok | ok | 8 | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 12 | ok | ok | ok | ok | ok | ok | ok |

`reneweta`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |

`renewidle`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |
| late, eta | 13 | ok | ok | ok | ok | ok | ok | ok |
| late, hang | 17 | ok | ok | 14 | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |

`resend`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 21 | ok | ok | ok | 10 | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 |
| late, cut | 27 | ok | ok | ok | 10 | ok | ok | 9 |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 |

`restoreall`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| late, crash | 28 | ok | ok | ok | 11 | 11 | ok | ok |
| late, cut | 21 | ok | ok | ok | 10 | 9 | ok | ok |

`restoreidle`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| late, crash | 26 | ok | ok | ok | 11 | ok | ok | ok |
| late, cut | 20 | ok | ok | ok | 10 | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |

`tagrestore`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| late, etashutdown | 26 | 9 | ok | ok | 9 | ok | 7 | ok |

`token`

| ack, fault | D | exactlyOnce | neverLost | quietDone | noAvoidableDup | noUnavoidableDup | restoresOwnOnly | workerSurvives |
|---|---|---|---|---|---|---|---|---|
| early, cut | 27 | ok | ok | ok | 10 | ok | ok | ok |
| early, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok |
| early, reset | 18 | ok | ok | ok | 7 | ok | ok | ok |
| early, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |
| late, crash | 34 | ok | ok | ok | 12 | ok | ok | 9 |
| late, cut | 30 | ok | ok | ok | 10 | ok | ok | 9 |
| late, cutlong | 36 | 7 | ok | ok | 7 | 8 | ok | 9 |
| late, eta | 20 | 9 | ok | ok | 9 | ok | ok | ok |
| late, hang | 19 | ok | ok | ok | ok | ok | ok | ok |
| late, kill | 21 | ok | ok | ok | 10 | 8 | ok | ok |
| late, long | 18 | 7 | ok | ok | 7 | ok | ok | ok |
| late, none | 12 | ok | ok | ok | ok | ok | ok | ok |
| late, partition | 26 | ok | ok | ok | 10 | ok | ok | 9 |
| late, reset | 20 | ok | ok | ok | 9 | 7 | ok | ok |
| late, shutdown | 19 | 9 | ok | ok | 9 | ok | ok | ok |

Publisher:

| instance | D | violated |
|---|---|---|
| `off_none` | 6 | none |
| `off_crash` | 8 | `neverLost` (3), `quietDone` (5) |
| `off_drop` | 6 | `neverLost` (3), `quietDone` (4) |
| `on_none` | 8 | none |
| `on_crash` | 16 | `publishedOnce` (8) |
| `on_drop` | 15 | `publishedOnce` (7) |

Transport, the renewal and token instances (the 15 earlier ones are
unchanged):

| instance | D | violated |
|---|---|---|
| `renewblind_held_credit` | 44 | noStranded (11) |
| `renewown_held_credit` | 44 | none |
| `token_none` | 24 | none |
| `token_crash` | 31 | none |
| `token_credit` | 34 | none |
| `token_reply_timeout` | 33 | none |
| `token_credit_timeout` | 43 | none |
| `token_mismatch` | 24 | none |
| `token_mismatch_crash` | 31 | none |
| `token_credit_crash` | 41 | none |
| `token_held_timeout` | 34 | none |
| `token_renewblind_held_credit` | 44 | none |

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
| `adapter/confirm_flush_ack_modes.py` | `flush()` between a job's write and its accept, early and late ack | the child runs the task, the message is never acked (both modes) | acked (both modes) |
| `adapter/confirm_renewal_hung.py` | a consumer that never acks, visibility timeout 2 s | the other consumer gets both messages | renew-everything prototype: neither |
| `adapter/confirm_ack_after_loss.py` | an ack that fails with its connection | delivered again when the channel closes | resend prototype: not delivered again |
| `adapter/confirm_putback_renewal.py` | kombu PR 2831 with the renewal patch | control: the message is back on the queue | after a renewal: on no queue, in `unacked` |
| `adapter/confirm_restore_by_tag.py` | a channel closes after the timeout redelivered its message | a third delivery while the second still runs (main and the PR) | no prototype |
| `adapter/natural_restore_by_tag.sh` | the same with three Celery workers and no injected delay | SIGKILL: 12 of 12 run once; SIGTERM: 11 of 12 twice (PR branch: 12 of 12) | no prototype |
| `adapter/record_transport.py` | recorded Redis command histories of two consumers, checked by `quint test` | 30 of 30 allowed (`BrpopThenRecord`) | 30 of 30 allowed (`PopScript`) |

The recorded histories go from the code to the spec: Redis `MONITOR` gives
every command with the connection that sent it and the commands the pop
script runs, and each history becomes a run whose actions must be enabled
in that order. 20 of the 30 histories of the PR take the step the spec did
not have before (`waitAfterEmpty`).

The ack through a collected channel needs a worker; those runs are in the
reply drafted for celery issue 11054. Spec mutants: 14 for these three specs
(`mutate_spec.py worker`, `publisher`, `transport`), all caught.

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
