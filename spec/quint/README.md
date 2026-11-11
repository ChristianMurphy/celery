# Quint specs of Celery and kombu

Typed Quint specs of what Celery's result backends, canvas primitives and
workers and kombu's Redis transport and publisher do. Each rule cites the
code it models, each property has an entry in a catalog, and the specs are
tied to the code in both directions: traces generated from the specs are
replayed against the real backends, and histories recorded from real runs
are checked against the specs.

## What is claimed

For each instance (a spec with a configuration and a fault budget, small:
two messages or two header tasks, two or three processes) the tools check
every property in every reachable state, and `expected.json` records the
result: the longest run D and, for each violated property, the number of
states in a shortest counterexample. Nothing is claimed for larger sizes.
Every expected violation has a reason in `properties.json`: Celery
documents it, it is reported or drafted (with the tracker id), it is fixed
in a named commit, or it is a stated trade-off. A counterexample counts as
a finding only once the real code reproduced it ([findings/](findings/README.md)).

<!-- generated: index -->
| Spec | Family | Pinned to | Properties | Instances | Fast tier | Lemma 1 (every run is finite) |
|---|---|---|---|---|---|---|
| [`resultstate.qnt`](result/resultstate.qnt) | [result/](result/README.md) | celery 768e476 | 3 and 1 witnesses | 4 | rtw, es | `progress`, a closed-form measure |
| [`chord.qnt`](canvas/chord.qnt) | [canvas/](canvas/README.md) | celery 768e476 | 9 and 3 witnesses | 69 | redis_dup, redis_wlost_late, kv_dup, unlock_child_reject, once_dup, mixed_dup | `progress`, a closed-form measure |
| [`redis_transport.qnt`](transport/redis_transport.qnt) | [transport/](transport/README.md) | kombu 445027e, kombu PR 2831 c61b525 | 9 and 3 witnesses | 27 | main_crash, paused_credit_timeout, pr_credit_timeout, renewblind_held_credit | finite by exhaustive search with the step counter in the state |
| [`worker.qnt`](worker/worker.qnt) | [worker/](worker/README.md) | celery 768e476, kombu 445027e, kombu PR 2831 c61b525 | 8 and 2 witnesses | 94 | main_late_partition, pr_late_shutdown, pr_late_eta, pr_late_cut, renewall_late_hang, best_late_crash | finite by exhaustive search with the step counter in the state |
| [`publisher.qnt`](transport/publisher.qnt) | [transport/](transport/README.md) | celery 768e476 | 5 and 1 witnesses | 6 | off_crash, on_none | finite by exhaustive search with the step counter in the state |
| [`chain.qnt`](canvas/chain.qnt) | [canvas/](canvas/README.md) | celery 768e476 | 9 and 4 witnesses | 23 | late_none, late_unencodable | finite by exhaustive search with the step counter in the state |
| [`redelivery.qnt`](worker/redelivery.qnt) | [worker/](worker/README.md) | celery 768e476, kombu 445027e | 9 and 5 witnesses | 27 | redis_late_short, redis_late_long_w1 | finite by exhaustive search with the step counter in the state |
<!-- end generated -->

## Layout

| Path | What |
|---|---|
| `lib/` | What several specs share: `observe.qnt` (exactlyOnce, neverLost, quietDone over a record per message), `delivery.qnt` (the abstract at-least-once delivery environment the canvas specs build on) |
| `result/` | The stored result of one task id; four stores |
| `canvas/` | Chords: five completion protocols, the fixes for celery issues 11036 and 11039; scenarios as `quint test` runs. Chains with a link and an errback, and two fix designs |
| `transport/` | kombu's Redis consume path (main and kombu PR 2831) and a RabbitMQ publisher |
| `worker/` | Two workers from the broker to a finished task, and eight design choices; the projection onto `lib/delivery.qnt` |
| `properties.json` | The catalog: one entry per spec and per property |
| `expected.json` | D and the expected violations of every instance |
| `findings/` | One folder per finding confirmed on the real code: trace, confirm script, status |
| `tools/check.py` | The tiers |
| `tools/catalog.py` | Checks the catalog and the checklist; writes the tables in the READMEs |
| `tools/citations.py` | Checks each `@cite` (file, range and anchor at the pinned SHA, anchor on main; `CELERY_REPO`, `KOMBU_REPO`) |
| `tools/tlc.py` | Exhaustive search with TLC |
| `tools/mutate_spec.py` | Spec mutants: each must make a property fail |
| `tools/type_errors.py` | Mistakes the types reject |
| `tools/gen_worker_instances.py`, `tools/design_tables.py` | The worker instances, and their results per design |
| `tools/baseline/` | Result matrices before and after changes that must not change a result |
| `adapter/replay_*.py` | Spec to code: replay ITF traces on Redis, MongoDB, PostgreSQL, Elasticsearch, memcached |
| `adapter/record_*.py`, `adapter/check_histories.py`, `adapter/worker_app.py` | Code to spec: record histories (threads, a real worker), check them with `quint test` |
| `adapter/mutations.py` | Code mutants: each must make a replay mismatch |
| `traces/`, `histories/` | Counterexamples as ITF traces; recorded histories |
| `run_all.sh` | Everything above, by tier |

## How to run

```sh
npm i -g @informalsystems/quint          # 0.33.1; Java 17+ from the full tier on
cd spec/quint
python3 tools/check.py                   # fast tier
python3 tools/check.py --tier full       # exhaustive search, Apalache
python3 tools/check.py --tier long       # Apalache on the large instances too
python3 tools/check.py --only chord --instance once_dup --tier full
python3 tools/catalog.py --write         # regenerate the tables in the READMEs
quint test --main=onceReadOnce canvas/scenarios.qnt
quint run --main=redis_dup canvas/chord_instances.qnt --invariant=atMostOnce
PYTHONPATH=$(git rev-parse --show-toplevel):$PWD ./run_all.sh full   # needs the services
```

`quint run`'s default Rust backend downloads its evaluator through the GitHub
API on first use. Where that is blocked, fetch it from the release page into
`~/.quint/rust-evaluator-v0.7.1/`. `quint verify` downloads Apalache 0.62.4,
and TLC is the one inside that jar.

| Tier | What | Time |
|---|---|---|
| fast | the catalog and the checklist; typecheck; `type_errors.py`; one simulator run per listed instance (at most 24) with the properties and `steps <= D` as invariants, the expected violations as witnesses, and the 300 traces read back to count actions and outcomes; `--all` for every instance | about 70 s; 4 min with `--all` |
| full | TLC: every reachable state of every instance, D, every property, the quiet-stop check, and a shortest counterexample per expected violation. Apalache on the result-state instances: `steps <= D` at depth D + 1, then each property at depth D | about 70 min |
| long | Apalache on the chord and transport instances (an hour's timeout each), a 20,000 sample simulator run | not run |

The fast tier keeps, for every spec, one instance that passes every
property, so that a model that goes vacuous (a guard that is never true)
shows up as a witness that is no longer reached.

TLC is the complete check. Apalache at depth D is complete too once
`steps <= D` holds at depth D + 1 (no run has a step D + 1), and it is a
second engine with a different encoding; it is affordable where D is 11 and
not for the chords: `redis_wlost_late.quietDone` took it 35 minutes at 16
steps, and that instance's D is 21.

Every action and every outcome is reachable: the sampled traces of the fast
tier with `--all` contain 8 of the 8 actions and outcomes of the result spec, 71
of the 72 of the chord spec and 20 of the 20 of the transport spec. The one
never sampled, `CheckKeys.ResultLost`, is cd8aa46's failure, which TLC
reaches in `oncev1_dup` (15 states) and the scenario `onceReadOnce` runs. Each instance also has
witnesses (`neverDispatched`, `neverFailed`, `neverQuiet`, `neverDuplicated`,
`neverPutBack`, `noSuccessEver`) that TLC must find violated where the tables
below say so.

## How to read a spec

Every spec file has the same parts in the same order: a header (what it
models, at which SHAs, what is left out); a types module; the state; the
actions, each with the code it models; the faults; `step`; then the
properties.

Every state, outcome and action is a sum type, every definition, parameter
and state variable is annotated, and a state the code cannot be in has no
value.

| Type | In place of |
|---|---|
| `Status`, `Result`, `Report`, `Store`, `Proto`, `Design` | strings such as `"SUCCESS"` and `"redis"` |
| `Stored = Absent \| Present(Meta)`, `Observed = NotObserved \| Saw(Meta)` | a `PENDING` record with run 0, a `has` flag |
| `Pc = Idle \| Begun({ writing, read })` | a pc string and two maps that kept stale values |
| `Claim = NoClaim \| Claimed(Token) \| Done` | `ckey: int` with 0 and -1 as sentinels |
| `Exec = Free \| HeaderTask(HeaderRun) \| UnlockTask(UnlockRun)`, with `HeaderPc` and `UnlockPc` | one record with a `busy` flag, a `kind` string and empty fields |
| `HeaderPc` variants that carry their data (`Stored(Report)`, `RedisJoined(Args)`, `OnceKeys(Set[HeaderId])`) | `res`, `args` and `nargs` fields filled in later |
| `Action`, one variant per action with its fields, and one outcome type per action | `{ kind: str, w: int, h: int, outcome: str }` |
| `Config`, `Faults` | eight loose parameters per instance |
| `HeaderId`, `ExecId`, `Token`, `RunId`, `MsgId`, `ConsumerId`, `Score` | `int` |

The ids are one-field records with different field names (`{ h: int }`,
`{ w: int }`, `{ tok: int }`), built with `H(1)`, `W(2)`, `Tok(3)`. Quint's
records are structural, so two of them unify only if the field name is the
same: `queue.get(r.tok)` is rejected with "Trying to unify { h: int } and
{ tok: int }". A one-variant sum type (`type HeaderId = H(int)`) would read
the same and be nominal, but Quint 0.33.1 gives a one-variant sum type an open
row type, and Apalache and TLC then reject the spec ("Operator FORALL3 cannot
be applied to arguments of types (W(Int), Set(W(Int) | s), Bool)"). For the
same reason a `match` never uses `_` as a catch-all: every match lists every
variant, which also means a new variant has to be placed in each of them.

Other rules the specs follow:

* Types live in their own module (`resulttypes`, `chordtypes`,
  `transporttypes`), so that an instance can pass a constructor
  (`import chord(CFG = lateAck(Redis)).*`).
* Reading a variant's data inside an action goes through a pure reader
  (`headerOf(e)`, `joinedOf(pc)`) under a guard (`isHeader(e)`), because
  every branch of a `match` in an action must update the same variables.
* Instances are in their own file: quint flattens every module of every file
  it loads, about a second per instance module, whichever is the main one.
* Each nondeterministic choice is made next to the actions that use it. With
  all choices at the top of `step`, the simulator and TLC try every
  combination for every action (500 instead of 3 for most of them).

`python3 tools/type_errors.py --old` checks 17 mistakes: each is rejected now, and
the 12 that can be written against the untyped specs at 355c8b7 typechecked
there. Among them: a misspelled state (`"SUCESS"`), a claim token used as a
header id, `deliver` with its arguments swapped, a `chord_unlock` run with a
claim token or at a header-only program counter, the claim key compared with
an int, a part-return outcome on a count action, a misspelled protocol (every
guard false, so every property held), and a store named `"redis"`, which the
untyped spec checked silently as its last branch, `"es"`.

## Liveness in three lemmas

Quint's simulator does not check temporal properties, so liveness is three
checks that a model checker can do completely, under the same names in
every spec:

1. `progress`: every run is finite. Each spec counts its steps. Where a
   spec has a closed-form measure (`chord.qnt`, `resultstate.qnt`: what the
   queued messages, the busy processes, the claim and the unused budgets
   can still do, in steps), `steps + measure <= MEASURE0` is an invariant.
   Where it has none (`redis_transport.qnt`, `worker.qnt`,
   `publisher.qnt`), runs are finite by exhaustive search with the step
   counter in the state: the search ends. In both cases the exact longest
   run D of each instance comes from the search, and `steps <= D` is
   checked as an invariant.
2. `stepOrQuiet`: a run stops only in a quiet state. The search runs
   `stepOrQuiet`, which lets a quiet state stutter, with deadlock detection
   on: a state without a successor is a run that stopped without being
   quiet.
3. `quietDone`: a quiet state has ended: the body was dispatched or the
   chord failed; every message is acked; every task finished.

Together: every run ends within D steps, in a state that has ended. An
instance that breaks 3 has a run that hangs.

The fairness assumption is in the fault budgets: a fault (a lost worker, a
duplicate delivery, a timeout) happens a bounded number of times, and a
recovery step (a restore, a reconnect, a retry) is never refused forever,
because a state where one is enabled is not quiet. These are exhaustive
checks of small instances, not proofs for all sizes.

## How to add a model

1. Put the spec in its family directory with its instances in
   `<spec>_instances.qnt`, following the anatomy above. Cite the code for
   every action (`file.py:N-M` at a pinned SHA).
2. Add the spec and one entry per property to `properties.json`, before
   running anything: statement, kind, pattern, assumptions, source.
3. Give every property a witness (a state that shows it is not vacuous) and
   a spec mutant in `tools/mutate_spec.py` that makes it fail.
4. Run `python3 tools/check.py --tier full --only <spec> --update-expected`
   and give every violation a reason in the catalog.
5. List at most a few instances for the fast tier, one of which passes
   every property; the budget is 24 instances over all specs.
6. For every counterexample: a confirm script against the real code, in a
   folder under `findings/`.

`python3 tools/catalog.py` (part of the fast tier) fails when an entry, a
reason, a witness, a mutant or the budget is missing.

## Findings

<!-- generated: findings -->
| Finding | Instance and property | Outcome | Status | Thread | On the real code |
|---|---|---|---|---|---|
| [ack-after-loss](ack-after-loss/): An ack that fails with its connection is forgotten, and one through a collected channel stops the worker | `pr_late_cut.noAvoidableDup`, `pr_late_cut.workerSurvives`, `main_late_partition.workerSurvives` | duplicate | open: tracker eo-redis-ack-after-connection-loss | the reply posted on celery issue 11054 | `confirm_ack_after_loss.py`. Main: delivered again when the channel closes. Resend prototype: not delivered again. The worker exit needs a worker; those runs are in the reply. |
| [broker-loss-silent](broker-loss-silent/): A Redis broker that loses a write it acknowledged loses the task silently: it never runs and stays PENDING | `redis_late_loss.noSilentLoss`, `redis_late_loss.quietDone` | silent loss | documented: docs/getting-started/backends-and-brokers/redis.rst:268-273 (celery 768e476): RDB snapshots lose tasks published since the last one, AOF everysec up to a second | none needed: documented | `confirm_broker_loss.py`. 768e476 with kombu 445027e, Redis 7.4 broker with --save "" --appendonly no restarted (docker restart, injected) after apply_async() returned with the task queued, then a real worker: 3 of 3 runs the task never ran, state PENDING, nothing about it in the worker log. The classic-queue case (an unconfirmed publish lost by a kill) is the exactly-once report's measurement (3,659 of 5,100 lost without confirms, 0 with) and the Discussion draft on the confirm_publish default. |
| [chain-store-error](chain-store-error/): A chain's link and next step run although the parent's result could not be stored, beside the errback, and the next step's FAILURE turns into SUCCESS | `late_unencodable.notBoth`, `late_unencodable.finalStable`, `late_storeerr.notBoth`, `late_storeerr.finalStable` | visible failure | open: related to celery issue 10954 (the same window, reached by a lost pool child); not yet in the tracker | a reply on celery issue 10954 (drafted by the lead) | `confirm_chain_store_error.py`. 768e476 with kombu 445027e, a real prefork worker on Redis: a returns a value the serializer cannot encode: 5 of 5 runs ran the link and the errback, and b was read PENDING, FAILURE, then SUCCESS (lead: 3 of 3 on main 9c2f238). Mode operational, a backend that raises kombu OperationalError on a's SUCCESS write: 3 of 3, the same, nothing redelivered. |
| [dedup-read-error](dedup-read-error/): worker_deduplicate_successful_tasks: a failed read of the result backend ends a redelivered task without running it; it is acked and stays PENDING when the backend is down | `redis_dedup_readerr.noFailWithoutError`, `redis_dedup_readerr.noSilentLoss`, `redis_dedup_readerr.quietDone` | hang | open: not in the tracker yet; no upstream thread found (searched worker_deduplicate_successful_tasks, BackendGetMetaError, dedup backend unavailable; closest celery PR 10960 and 10159, other paths). The check has caught only BackendGetMetaError since it came in (celery PR 6722, 2021), and that is raised only under result_backend_always_retry | none yet (a new issue candidate) | `confirm_dedup_read_error.py`. 768e476 with kombu 445027e, real prefork workers -c 1 on Redis (broker 6379, backend in its own container on 6391), acks_late, dedup on, visibility_timeout 2 s. Worker A is SIGKILLed 1 s into a 3 s run (injected); the message is restored and worker B takes it. down (the backend container stopped while B takes it, started 8 s later): 2 of 2 runs the task never ran again, the message was acked, the state stayed PENDING; B logged "Exception raised outside body: network:ConnectionError" and "Pool callback raised exception". readonce (the first read in B raises redis ConnectionError, the backend is up): 3 of 3 FAILURE stored, never ran. With result_backend_always_retry: the task ran and ended SUCCESS (downretry 2 of 2, readonceretry 3 of 3). Without dedup the same redelivery simply runs the task. |
| [dedup-republish](dedup-republish/): worker_deduplicate_successful_tasks does not see a copy the publisher sent again after a lost reply: the task runs again after its SUCCESS | `redis_dedup_republish.noRerunAfterSuccess` | duplicate | documented: docs/userguide/configuration.rst:3653-3655 (celery 768e476): only messages redelivered by the broker are checked | none needed: documented; worth a sentence in the idempotency Discussion draft | `confirm_republish.py`. 768e476 with kombu 445027e, redis-py 8.1.0, a real prefork worker on Redis with dedup on; the publisher's replies dropped for 1.5 s by Toxiproxy (injected) with a 1 s socket timeout: 2 of 3 runs the task ran twice, SUCCESS stored before the second run, nothing skipped (the first run connected inside the drop and published once). |
| [early-ack-amqp-cut](early-ack-amqp-cut/): Early ack is not at most once across a cut AMQP connection: the ack is written into the cut, and RabbitMQ returns the task when the channel closes | `classic_early_cut.earlyAtMostOnce` | duplicate | open: tracker eo-early-ack-duplicates; celery issue 4426 (comments from 2019 and August 2026) | celery issue 4426 (the exactly-once report leans to a docs reply there) | `confirm_early_ack_cut.py`. 768e476 with kombu 445027e, a real prefork worker -c 1 (prefetch 1, heartbeat 2 s) on RabbitMQ 3.13 through Toxiproxy; the cut (a timeout toxic with timeout 0 both ways, 8 s) is injected while the task ahead runs, so that the next task's early ack falls inside it: 3 of 3 runs ran it twice, no error logged for the ack; without the cut 0 of 2. Natural rate from the exactly-once report (real blackholes of all workers, steady load): 17 to 37 tasks ran twice per run on RabbitMQ. With confirm_publish set and worker heartbeats on, the first attempt did not reproduce: the main process blocked on a heartbeat event publish waiting for its confirm, so the next task never started inside the cut. |
| [flush-ack-modes](flush-ack-modes/): A warm shutdown or a reconnect runs a task twice: flush() drops a job the child already has | `pr_late_shutdown.exactlyOnce`, `pr_early_shutdown.exactlyOnce`, `ackfull_late_cut.noAvoidableDup` | duplicate | open: tracker eo-warm-shutdown-flush-duplicate; the variant DropUnwritten removes it | the reply posted on celery issue 3802 | `confirm_flush_ack_modes.py`. Main: the child runs the task and the message is never acked, in both ack modes. With the variant: acked in both. |
| [interrupt-cleanup](interrupt-cleanup/): The first fix for celery issue 11039 failed a chord after its body was sent | `redisfix1_interrupt.noErrorAfterDispatch`, `redisfix3_interrupt_dup.atMostOnce` | duplicate | fixed: 5446659 and ad00b4a on the fix branch for celery issue 11039 (5f844ea and ee48301 had them) | the thread of celery issue 11039 | `confirm_interrupt_cleanup.py`. On Redis at 5f844ea: the body published once and FAILURE stored. Main publishes twice after a later copy, ad00b4a once. |
| [interrupt-then-copy](interrupt-then-copy/): The second fix for celery issue 11039 sent the body after the chord was failed | `redisfix2_interrupt_dup.noDispatchAfterError`, `redisfix2_interrupt_dup.atMostOnce` | duplicate | fixed: ee48301 on the fix branch for celery issue 11039 (5446659 had it) | the thread of celery issue 11039 | `confirm_interrupt_then_copy.py`. On Redis at 5446659: "published 0, errors 1", then "published 1" after the copy. |
| [mixed-fleet](mixed-fleet/): A rolling upgrade to the fix for celery issue 11036 can leave a chord pending | `mixed_dup.quietDone`, `mixed_dup.atMostOnce`, `mixed_lose_result.quietDone` | hang | by design: documented in the fix since 2057a3c | the thread of celery issue 11036 | `confirm_mixed_fleet.py`. Both results SUCCESS, the counter at 3, the body never published. |
| [once-reread](once-reread/): The first fix for celery issue 11036 failed a chord whose header results were all SUCCESS | `oncev1_dup.noSpuriousError` | visible failure | fixed: 1fa71d4 on the fix branch for celery issue 11036 (cd8aa46 had it) | the thread of celery issue 11036; reported in the reply on celery discussion 11041 | `confirm_once_reread.py`. On memcached at cd8aa46: results SUCCESS and SUCCESS, the body published 0 times. At 1fa71d4: published once. |
| [putback-renewal](putback-renewal/): A blind renewal next to kombu PR 2831 strands a message in unacked | `renewblind_held_credit.noStranded` | hang | open: tracker reply-kombu-337-renew-scope; compare and set (RenewOwn) or the pop token (ByToken) removes it | kombu issue 337, the follow-up to the renewal proposal | `confirm_putback_renewal.py`. Control: the message is back on the queue. After a renewal: on no queue, in unacked. |
| [quorum-delivery-limit](quorum-delivery-limit/): A quorum queue drops a task that keeps killing its worker at the delivery limit; the state stays PENDING and nothing logs the drop | `quorum_late_kill.noSilentLoss`, `quorum_late_kill.quietDone` | silent loss | documented: docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257 (celery 768e476) | none needed: documented; a dead letter exchange makes it visible (quorum_dlx_kill passes) | `../rabbitmq-consumer-timeout-loop/confirm_consumer_timeout_loop.py`. Case kill of that script: RabbitMQ 4.1 quorum queue with x-delivery-limit 1, a task that SIGKILLs its worker's main process and child, a supervisor restarting the worker: 2 runs, 3 worker starts, then nothing on the queue and the state PENDING, 60 s watched. |
| [rabbitmq-consumer-timeout-loop](rabbitmq-consumer-timeout-loop/): RabbitMQ: an acks_late task longer than consumer_timeout reruns for ever, whether the worker exits or reconnects | `classic_late_long.noLoop`, `classic_retry_long.noLoop` | duplicate | open: tracker eo-consumer-timeout-acks-late-loop (a candidate, no public draft); calling.rst:318-334 documents the channel close for countdowns only | none yet; closest celery issues 6760 and 6954 (named in the tracker entry) | `confirm_consumer_timeout_loop.py`. 768e476 with kombu 445027e, a real prefork worker -c 1 (prefetch 1, acks_late), consumer_timeout 5 s checked every second, a 9 s task, a supervisor loop restarting the worker, 60 s watched, nothing injected: RabbitMQ 3.13 classic queue: 6 runs, one every 11 s, 6 worker starts, still queued at the end; with broker_channel_error_retry: 7 runs, one every 9 s, no exit, 7 "Couldn't ack" logged, still queued; with worker_deduplicate_successful_tasks: 1 run and 1 skip, done. RabbitMQ 4.1 quorum queue with x-delivery-limit 2: 3 runs, then dropped with SUCCESS stored. |
| [redis-visibility-loop](redis-visibility-loop/): Redis: an acks_late task longer than the visibility timeout reruns for ever with one process and prefetch 1, or with worker_disable_prefetch | `redis_late_long_w1.noLoop`, `redis_nopf_long_w1p2.noLoop` | duplicate | documented: redis.rst:318-320 says "again, and again in a loop" for ETA, countdown and retry tasks, not for long tasks or the condition; celery issue 5935 (a 2023 comment there says "redelivered again and again in a loop"); kombu issue 337; not in the tracker yet | celery issue 5935, or the renewal reply on kombu issue 337 (the lead decides) | `confirm_redis_visibility_loop.py`. 768e476 with kombu 445027e, a real prefork worker -c 1 on Redis 7, visibility_timeout 2 s, sweep every second, a 5 s acks_late task, nothing injected, 40 s watched: prefetch multiplier 1: 8 runs, one every 5 s, still queued at the end; worker_disable_prefetch (multiplier 4): 8 runs, the same; multiplier 2: 3 runs (ceil(5 / 2)), then done; multiplier 1 with worker_deduplicate_successful_tasks: 1 run and 1 skip; a 1 s task: 1 run. |
| [renewal-hung](renewal-hung/): Renewing every unacked message turns the recovery of a hung task into a hang | `renewall_late_hang.quietDone`, `renewidle_late_hang.quietDone` | hang | by design: the reason the renewal proposed for kombu issue 337 is narrowed to messages waiting for an ETA (tracker reply-kombu-337-renew-scope) | kombu issue 337 | `confirm_renewal_hung.py`. Main: the other consumer gets both messages after the visibility timeout. Renew-everything prototype: neither. |
| [restore-by-tag](restore-by-tag/): A channel restores by delivery tag: a consumer that held a message past the timeout restores the copy another worker runs | `tagrestore_late_etashutdown.restoresOwnOnly` | duplicate | open: tracker eo-restore-by-tag-third-delivery (a candidate; where to post is with the user) | undecided: kombu issue 337 or a new kombu issue | `confirm_restore_by_tag.py`, `natural_restore_by_tag.sh`, `bytagapp.py`. Main and the PR: a third delivery while the second still runs. Three workers, no injected delay: SIGKILL 12 of 12 run once; SIGTERM 11 of 12 run twice (PR branch 12 of 12). |
<!-- end generated -->

Each family's README lists its findings with the outcome on the ranking
(a duplicate is the least bad, then a visible failure, then a hang, then a
silent loss): [canvas](canvas/README.md), [transport](transport/README.md),
[worker](worker/README.md), [result](result/README.md).

## Spec to code, code to spec

Run by `./run_all.sh full` against Redis 7, MongoDB 7, PostgreSQL 16,
Elasticsearch 8.15 and memcached 1.6:

* Result state: 161 generated traces replay on the real backends with every
  state equal (Redis 41, MongoDB 40, PostgreSQL 40, Elasticsearch 40).
* Chords: 340 random traces over the 17 `redis_*` and `kv_*` instances of
  768e476, their 18 counterexamples and 5 scenario traces replay with every
  state equal, the real `mark_as_done` and `Request.on_failure` running in
  threads parked at the spec's split points.
* Recorded histories: 600 concurrent store histories (200 each for Redis,
  MongoDB, Elasticsearch) and 23 chords run by a real worker are behaviors
  the specs allow; the Redis histories checked against the `mongo` model
  fail, as they should (181 of 200 allowed).
* The fix branches and the designs: the scripts in `findings/` run a
  scenario's steps on the real backend or broker of a given checkout
  (`PYTHONPATH=<worktree>`); each prints the bad outcome on the commit that
  had it and the good one on the fixed head.

## Mutations

Spec (`tools/mutate_spec.py`): 44 mutants, 42 caught and 2 equivalent, as
expected. The 27: 3 result stores (mongo without `$ne`, sql without the
locked re-read, es without the ready check); 5 for 768e476's chords (Redis
without dedupe, the claim without NX, incr on `>=`, an unfailed retry
refusal, a lost child counted again); 3 that loop forever (a retry, a
redelivery and a commit that use no budget), caught by `progress`; 6 for the
fix of 11036 (failing on the first read, completing without the done key,
counting on `==`, completing without reading the results, a late copy that
ignores the done key, a lost result that never fails the chord); 4 for the
fix of 11039 (failing after the publish returned, leaving the size key after
a failure and after a publish, no handler); 6 for the transport (`PUT_BACK`
without the score check, a second record that keeps the script's, no wake
token after a pop that leaves messages or after a restore, a reply read
without credit, a pop that does not record). The two equivalent ones are the
owner checks in `CHORD_CLAIM_EXTEND_LUA` and `CHORD_CLEANUP_LUA`, which only
matter when a lease expires under a live holder, where `atMostOnce` fails
anyway.
Since then: 7 for the worker and the publisher, and one for each property
that had none (8), so that every property has a mutant.

Code (`adapter/mutations.py`), traces that mismatched:

| Mutant | Replay | Caught |
|---|---|---|
| `kv_no_success_check` (base.py:1369) | rtw on Redis | 20 of 41 |
| `mongo_no_filter` (mongodb.py:207) | mongo | 19 of 40 |
| `sql_no_success_check` (database/__init__.py:186) | sql on Postgres | 19 of 40 |
| `sql_no_row_lock` (database/__init__.py:180) | sql on Postgres | 7 of 40 |
| `redis_lua_no_dedupe` (redis.py:111) | redis_dup | 20 of 20 |
| `redis_lua_claim_no_nx` (redis.py:139) | redis_dup | 16 of 20 |
| `redis_lua_no_done_marker` (redis.py:104) | redis_dup | 4 of 20 |
| `redis_cleanup_any_owner` (redis.py:160) | redis_lease | 13 of 20 |
| `redis_extend_any_owner` (redis.py:176) | redis_lease | 19 of 20 |
| `kv_count_early` (base.py:1465) | kv_dup | 20 of 20 |
| `kv_keep_counter` (base.py:1495) | kv_dup | 20 of 20 |
| `worker_lost_ignores_success` (request.py:714) | redis_child_late | 2 of 20 |

## Where this stopped

* The transport spec is checked by the simulator and TLC only: no trace of it
  was replayed on a real Redis, and no recorded history checked against it.
  kombu PR 2831 has its own Hypothesis state machine against a real server.
* `KvOnce`, `KvSkip` and the interrupt are replayed on the real code only
  through the confirm scripts in `findings/` and the scenarios they
  cover; `adapter/replay_chord.py` still replays `Redis` and `Kv` traces only.
* The counter's loss is modeled as DynamoDB and Firestore behave (the next
  incr starts from 0). memcached's `incr` on a missing key returns `None`,
  and the comparison with the chord size then raises; that is not modeled.
* A write whose reply was lost and that the client sent again (the done key
  storing the run's own token) is not modeled.
* Apalache was not run on the chord and transport instances (the long tier),
  apart from one check on `kv_none`.
* The transport spec has no closed-form progress measure. Its runs are finite
  because the exhaustive search, with the step counter in the state, ends;
  D is exact.
* The chord_unlock protocol is not replayed against the database backend.
