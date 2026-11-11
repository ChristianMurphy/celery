# Quint specs: result state, chord completion, the Redis transport

Five typed Quint specs of what Celery's result backends, chords and workers
and kombu's Redis transport and publisher do, each rule citing the code it
models, checked in three tiers and
tied to the code in both directions: traces generated from the specs are
replayed against the real backends, and histories recorded from real runs
are checked against the specs.

| File | What |
|---|---|
| `result/resultstate.qnt` | Result state of one task id; four stores (`rtw`, `mongo`, `sql`, `es`) |
| `canvas/chord.qnt`, `canvas/chord_instances.qnt` | Chord completion; five protocols (Redis Lua, incr counting on main, two fixes for celery issue 11036, `chord_unlock`), the interrupted dispatch of celery issue 11039, 69 instances |
| `transport/redis_transport.qnt`, `transport/redis_transport_instances.qnt` | kombu's Redis consume path: BRPOP then record (main) and the pop script of kombu PR 2831, two consumers, 15 instances |
| `worker/worker.qnt`, `worker/worker_instances.qnt`, `tools/gen_worker_instances.py` | Two workers from the broker to a finished task: the pool hand-off, ack modes, shutdown, reconnect, the visibility timeout, and eight design choices; 94 instances. See [worker/README.md](worker/README.md) |
| `transport/publisher.qnt`, `transport/publisher_instances.qnt` | A RabbitMQ publisher with confirms off or on; 6 instances. See worker/README.md |
| `tools/design_tables.py` | The worker results as one table per design, from `expected.json` |
| `canvas/scenarios.qnt` | The behaviors behind the findings, as `quint test` runs |
| `tools/check.py`, `expected.json` | The tiers; D and the expected violations of every instance |
| `tools/tlc.py` | Exhaustive search with TLC |
| `tools/type_errors.py` | Mistakes the types reject |
| `tools/mutate_spec.py` | Spec mutants: each must make a property fail |
| `tools/baseline/` | The result matrix before and after the retyping |
| `adapter/replay_*.py` | Spec to code: replay ITF traces on Redis, MongoDB, PostgreSQL, Elasticsearch, memcached |
| `adapter/record_*.py`, `adapter/check_histories.py`, `adapter/worker_app.py` | Code to spec: record histories (threads, a real worker), check them with `quint test` |
| `adapter/confirm_*.py` | The four counterexamples about the fix branches, on the real code |
| `adapter/mutations.py` | Code mutants: each must make a replay mismatch |
| `run_all.sh` | Everything above, by tier |

## How to run

```sh
npm i -g @informalsystems/quint          # 0.33.1; Java 17+ from the full tier on
cd spec/quint
python3 tools/check.py                         # fast tier
python3 tools/check.py --tier full             # exhaustive search, Apalache
python3 tools/check.py --tier long             # Apalache on the large instances too
python3 tools/check.py --only chord --instance once_dup --tier full
quint test --main=onceReadOnce canvas/scenarios.qnt
quint run --main=redis_dup canvas/chord_instances.qnt --invariant=atMostOnce
PYTHONPATH=$(git rev-parse --show-toplevel):$PWD ./run_all.sh full   # needs the services
```

`quint run`'s default Rust backend downloads its evaluator through the GitHub
API on first use. Where that is blocked, fetch it from the release page into
`~/.quint/rust-evaluator-v0.7.1/`. `quint verify` downloads Apalache 0.62.4,
and TLC is the one inside that jar.

## Types

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

`python3 tools/type_errors.py --old` checks 15 mistakes: each is rejected now, and
the 12 that can be written against the untyped specs at 355c8b7 typechecked
there. Among them: a misspelled state (`"SUCESS"`), a claim token used as a
header id, `deliver` with its arguments swapped, a `chord_unlock` run with a
claim token or at a header-only program counter, the claim key compared with
an int, a part-return outcome on a count action, a misspelled protocol (every
guard false, so every property held), and a store named `"redis"`, which the
untyped spec checked silently as its last branch, `"es"`.

### The retyping changed no result

`tools/baseline/` holds the matrix of the untyped specs at 355c8b7 and of the
retyped ones: 187 cells (29 instances, 6 or 7 properties), each checked by the
simulator (2,000 samples of 40 steps, seed 17) and by an exhaustive search
with TLC. `python3 tools/baseline/compare.py tools/baseline/untyped-355c8b7.json
tools/baseline/typed.json`:

* verdicts: 133 hold and 54 are violated, the same cells before and after, in
  both engines;
* shortest counterexamples (TLC, breadth first): the same length in all 54;
* the simulator and TLC disagree on one cell, before and after:
  `redis_wlost_late.quietDone`, which 2,000 samples miss and TLC finds in 14
  states (it took Apalache 35 minutes at 16 steps);
* 33 cells, all on `chord_unlock` instances, have a state graph one level
  shallower. `Finish`, `Publish`, `LoseWorker` and `LoseChild` no longer carry
  a header index in `lastAction` (the untyped record had an `h` field for
  every action, 0 for `chord_unlock`), so pairs of states that differed only
  there are one state. With `h` put back in a scratch copy, the depths are
  the old ones (15 and 23 for `unlock_none` and `unlock_dup`).

The types found no bug in the specs. The time went up: TLC 1,080 s to 1,875 s
summed over the cells, the simulator 1,095 s to 1,668 s, on a machine other
jobs were using.

## Checking in layers

Liveness is three checks, each of which a model checker can do completely:

1. Every run is finite. Each spec counts its steps. A progress measure
   (`measure`: what the queued messages, the busy processes, the claim and the
   unused budgets can still do, in steps) goes down with every action, so
   `progress`, `steps + measure <= MEASURE0`, is an invariant. The exact
   longest run D of each instance comes from the exhaustive search, and
   `steps <= D` is checked as an invariant.
2. A run stops only in a quiet state. The search runs `stepOrQuiet`, which
   lets a quiet state stutter, with deadlock detection on: a state without a
   successor is a run that stopped without being quiet.
3. A quiet state has ended (`quietDone`): the body was dispatched or the chord
   failed; every message is acked.

Together: every run ends within D steps, with the chord ended or the message
acked. An instance that breaks 3 has a run that hangs.

Quiet, per protocol:

* Redis, incr backends: no message queued, no process busy, no claim left to
  expire (an expired claim lets a later copy claim).
* `chord_unlock`: no header message queued or running, and either every header
  result is stored and no `chord_unlock` is queued or running, or a result is
  missing and `chord_unlock` can only poll again. The real task retries
  without limit; the spec gives the retries a budget of 2 so that runs are
  finite. A retry changes nothing but where the `chord_unlock` message is, so
  a second one adds no behavior.
* Transport: every message published, and no consumer can pop, read a reply,
  ack, regain credit or restore a dead consumer's message.

The tiers:

| Tier | What | Time |
|---|---|---|
| fast | typecheck; `type_errors.py`; one simulator run per listed instance (28 of 200) with the properties and `steps <= D` as invariants, the expected violations as witnesses, and the 300 traces read back to count actions and outcomes; `--all` for every instance | 69 s; 4 min with `--all` |
| full | TLC: every reachable state of every instance, D, every property, the quiet-stop check, and a shortest counterexample per expected violation. Apalache on the result-state instances: `steps <= D` at depth D + 1, then each property at depth D | 32 min |
| long | Apalache on the chord and transport instances (an hour's timeout each), a 20,000 sample simulator run | not run |

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

## Models

### Result state (`resultstate.qnt`, celery 768e476)

Two duplicate runs of a task (up to two writes each: STARTED, then RETRY,
SUCCESS or FAILURE), one `forget()`, and a reader. A store is split where the
code splits it: `begin(r, s)` is the read a backend does before writing,
`commit(r)` is the write.

Properties: `successStable` (once a reader saw SUCCESS by a run, the store
keeps it until `forget()`) and `readySticky` (a ready state is never replaced
by an unready one).

### Chords (`chord.qnt`)

A chord with two header tasks and three worker processes. Protocols:

| `proto` | Code |
|---|---|
| `Redis` | `RedisBackend.on_chord_part_return` and its three Lua scripts, 768e476 redis.py:96-185, 934-1069 |
| `Kv` | main's counter with incr, 768e476 base.py:1429-1503 (memcached, DynamoDB, GCS) |
| `KvSkip` | 09e5a59, the intermediate fix for celery issue 11036: a key per header task added before the count; a run that finds its key does not count |
| `KvOnce` | 2057a3c, the fix (code as at 1fa71d4): every run counts; the counter only triggers a check that every header result is stored; one done key per chord, added if absent, decides who completes or fails it; a key per header task that returned tells a lost result from one still to come; a header with `ignore_result` fails the chord at once. `rereadMissing: false` is cd8aa46 |
| `Unlock` | the `chord_unlock` task, 768e476 builtins.py:47-130 |

Faults, each with a budget: a header message delivered again (`dups`), a whole
worker lost (`workerLost`, optionally only in a window: between a header's
result or key and its count, or between taking the dispatch and the publish),
a pool child lost (`childLost`), a refused publish (`refusals`), a chord key
expired or evicted (`evictions`: a header result, a header's key, the done
key, the counter, the group), and a hard time limit or cold shutdown that
interrupts the dispatch (`interrupts`, celery issue 11039: in the claim
script's reply, the join, the extend call, the publish before or after the
body left, the cleanup). `oldExecs` makes one process run main's `Kv` while
the others run the fix (a rolling upgrade). `onInterrupt` picks what the Redis
backend does with the interrupt: nothing (768e476), fail the chord from the
claim on (5f844ea), until the publish returned (5446659), also drop the size
key (ee48301), and drop it after the publish too (ad00b4a).

Properties: `atMostOnce`, `noPremature` (the body is not sent while a header
result is missing), `argsComplete`, `noErrorAfterDispatch` (the chord is not
failed after its body was sent), `noDispatchAfterError` (and not sent after it
was failed), `noSpuriousError` (the chord fails only for a header FAILURE, a
refused publish, a lost key, a header that ignores its result, or an
interrupted dispatch), `failsOnce`, `quietDone`, `progress`.

### Redis transport (`redis_transport.qnt`, kombu 445027e and c61b525)

One queue, two messages, two consumers with prefetch 1.

| `design` | Code |
|---|---|
| `BrpopThenRecord` | main: BRPOP, and the record in `unacked` when the reply is read, redis.py:1457-1496, 474-495 at 445027e |
| `PopScript` | kombu PR 2831: `POP_UNACKED` pops and records in one step (:223-305), wake tokens (:279-292, 1442-1445, 1977-2007), `PUT_BACK` with its score check for a reply without credit or as old as the visibility timeout (:313-326, 1689-1698), the second record under the message's own tag in one transaction (:601-624) |
| `PopScriptPaused` | the PR before eb41008: a reply without credit waits and is delivered whatever its age |

Faults: a consumer killed, a delivered message held past the visibility
timeout by a live consumer, a reply delayed past it, credit lost while a reply
is on its way, and the script reading another delivery tag than kombu.

Properties: `inSomePlace` (at every moment a published message is on the
queue, in `unacked`, or acked), `neverLost` (or a live consumer is about to
read it), `notTwice` and `noDuplicate` (not in two places and not delivered
twice, unless a consumer was killed or held a message past the timeout),
`prefetchRespected`, `noMissedWake` (no message waits on the queue while a
consumer with credit waits and no token or pop is on the way), `quietDone`,
and `exactlyOnce` (while no fault was used: never delivered twice, and when
quiet, delivered once and acked). `ops` counts round trips to Redis.

Since then it has `CFG.renewal` (the renewal of unacked messages proposed
for kombu issue 337, as a blind `ZADD XX` or as compare and set), `CFG.putBack`
(by score, or by a token stored with the record), the property `noStranded`
(no record is left with a consumer that dropped the message), and the action
`waitAfterEmpty`, which recorded command histories showed was missing. The
worker side (prefetch above 1, the pool, shutdown, partitions, the classes of
duplicates and the designs compared) is `worker.qnt`; both are described with
their results in [worker/README.md](worker/README.md). Not modeled anywhere yet: several
queues and priorities.

## Mapping to the code

| Spec action | Code |
|---|---|
| `begin` rtw/es | `_get_task_meta_for` in `BaseKeyValueStoreBackend._store_result`, base.py:1367 |
| `commit` rtw | SUCCESS check base.py:1369-1370, `_set_with_state` base.py:1373, Redis SETEX redis.py:816-825 |
| `commit` mongo | `replace_one({'_id', 'status': {'$ne': SUCCESS}}, upsert=True)`, mongodb.py:207-213 |
| `begin` / `commit` sql | unlocked `_query_task` database/__init__.py:174; `with_for_update` re-read :175-180; insert :181-185; SUCCESS check :186-190; unique task_id models.py:34 |
| `commit` es | create `op_type=create` elasticsearch.py:135-139; `_update` get, checks, `if_seq_no` :140-142, 172-221 |
| `forget` | base.py:883-885, 1352-1353; mongodb.py:297; database/__init__.py:352-353 |
| `store` (header) | `mark_as_done` base.py:290-296 |
| `partReturn` | `CHORD_PART_RETURN_LUA` redis.py:96-146, called at 968-977 |
| `redisJoin` | zrange + unpack redis.py:1007-1017; ChordError path 1052-1060 |
| `extend` | `CHORD_CLAIM_EXTEND_LUA` redis.py:171-185, called at 1018-1031 |
| `publish` | `_retry_publish(callback.delay, ...)` redis.py:1032-1040, base.py:1485-1492, builtins.py:124-129 |
| `cleanup` (redis) | `CHORD_CLEANUP_LUA` redis.py:150-168, called at 1041-1051 |
| `count` (`Kv`) | restore + `incr` base.py:1438-1466 |
| `kvJoin` / `kvClean` | `join_native` base.py:1467-1483; `deps.delete()`, `delete(key)` 1493-1495 |
| `returned` (`KvOnce`) | group gone: `_claim_chord` or return, 1fa71d4 base.py:1515-1528; `_chord_part_returns` :1266-1277; `_fail_chord_once` :1258-1264 |
| `returned` (`KvSkip`) | `_add_chord_part` before the count, 09e5a59 base.py:1461-1488 |
| `countKey` | `incr`, `val >= size`, 1fa71d4 base.py:1531-1545 |
| `readResults`, `checkKeys`, `reread` | `_complete_chord_once`, `_missing_results`, `_chord_keys_exist`, 1fa71d4 base.py:1289-1310, 1283-1287 |
| `claim` | `_claim_chord` base.py:1253-1256; memcached `add` cache.py:255-261 |
| `evict` | the keys' lifetimes, `_chord_key_expires` base.py:1246-1251 |
| `interrupt` | `dispatching`, `sent`, `except BaseException`, ad00b4a redis.py:980-986, 1043-1046, 1076-1097; the time limit in the main process, request.py:539-579 |
| `unlockPoll` | `chord_unlock` builtins.py:50-130 (retry refusal: celery PR 10959) |
| `loseChild` | `Request.on_failure` request.py:709-790 (SUCCESS stored: celery PR 10752) |
| `claimExpires` / `doneExpires` | claim TTL redis.py:963, 1018-1020; done marker redis.py:966 |
| `sendPop`, `serverPop`, `receive` (main) | `_brpop_start`, `_brpop_read`, `QoS.append`, kombu 445027e redis.py:1457-1496, 474-495 |
| `sendPop`, `serverPop`, `popEmpty`, `wakeUp`, `waitTimeout` (PR) | `_brpop_start` c61b525 redis.py:1647-1672; `POP_UNACKED` :223-305; `_brpop_read` :1674-1722 |
| `receive` (PR) | put back :1689-1698 and `PUT_BACK` :313-326; `_deliver_popped` :1748-1760; `QoS.append(replaces=...)` :601-624 |
| `ack` | `QoS.ack` :636-638 |
| `restore` | `restore_visible` :660-686, `restore_by_tag` :688-699, `_do_restore_message` :1431-1450 |

Each action in the `.qnt` files carries the same citation in a comment.

## Results

`python3 tools/check.py --tier full` on 4 shared cores, every cell as
`expected.json` says: 32 minutes for the result, chord and first 15 transport
instances (compiling 9 minutes, TLC 17, Apalache 7), and about 21 minutes for
the 94 worker instances, 14 minutes for the 27 transport ones and under a
minute for the publisher. The worker, publisher and renewal results are in
[worker/README.md](worker/README.md). D is the longest run in steps, "states" the distinct
reachable states, and a number after a property the states in a shortest
counterexample. "Violated" lists properties that fail; "reachable" lists
witnesses, where failing means the state can be reached. Every property not
listed holds on every reachable state. In every instance a run stops only in
a quiet state, and `progress` holds.

### Result state

Properties: `successStable`, `readySticky`, `progress`. Witness:
`noSuccessEver`.

| instance | D | states | violated | reachable (witnesses) | stops only when quiet | time |
|---|---|---|---|---|---|---|
| rtw | 11 | 17128 | successStable (6), readySticky (5) | noSuccessEver (3) | yes | 7.4 s |
| mongo | 11 | 12280 | readySticky (5) | noSuccessEver (3) | yes | 6.2 s |
| sql | 11 | 13280 | readySticky (5) | noSuccessEver (3) | yes | 6.1 s |
| es | 11 | 12976 | none | noSuccessEver (3) | yes | 2.6 s |

Apalache 0.62.4, bounded. `steps <= 11` holding at depth 12 makes the checks
at depth 11 complete:

| instance | check | depth | result | time |
|---|---|---|---|---|
| rtw | steps <= 11 | 12 | holds | 75 s |
| rtw | successStable | 11 | violated | 4 s |
| rtw | readySticky | 11 | violated | 3 s |
| mongo | steps <= 11 | 12 | holds | 73 s |
| mongo | successStable | 11 | holds | 22 s |
| mongo | readySticky | 11 | violated | 3 s |
| sql | steps <= 11 | 12 | holds | 75 s |
| sql | successStable | 11 | holds | 27 s |
| sql | readySticky | 11 | violated | 3 s |
| es | steps <= 11 | 12 | holds | 84 s |
| es | successStable | 11 | holds | 27 s |
| es | readySticky | 11 | holds | 26 s |

### Chords

Properties: `atMostOnce`, `noPremature`, `argsComplete`,
`noErrorAfterDispatch`, `noDispatchAfterError`, `noSpuriousError`,
`failsOnce`, `quietDone`, `progress`. Witnesses: `neverDispatched`,
`neverFailed`, `neverQuiet`.

Instance names: `redis_`, `kv_`, `unlock_` are 768e476; `skip_` is 09e5a59;
`once_` is the fix for celery issue 11036 (2057a3c), `oncev1_` is cd8aa46,
`mixed_` is the fix with one process on main's code; `redisfix1_`,
`redisfix2_`, `redisfix3_` and `redisfix_` are 5f844ea, 5446659, ee48301 and
ad00b4a. Faults: `dup` (2 duplicate deliveries), `refuse`, `wlost_late` and
`wlost_early` (a worker lost, late or early ack), `child_late`,
`child_early`, `child_reject` (a pool child lost; `reject_on_worker_lost`),
`lease` (a claim that expires under its holder), `kill_before_count` and
`kill_after_claim` (a worker lost in that window), `ignore` (a header with
`ignore_result`), `lose_*` (that key evicted), `interrupt` (a hard time limit
in the join, the extend call, the publish before the body left, or the
cleanup), `interrupt_sent`, `interrupt_reply`.

| instance | D | states | violated | reachable (witnesses) | stops only when quiet | time |
|---|---|---|---|---|---|---|
| redis_none | 13 | 602 | none | neverDispatched (10), neverQuiet (13) | yes | 8.5 s |
| redis_dup | 23 | 183359 | none | neverDispatched (10), neverQuiet (13) | yes | 109.4 s |
| redis_refuse | 13 | 777 | none | neverDispatched (10), neverFailed (10), neverQuiet (13) | yes | 11.5 s |
| redis_wlost_late | 21 | 9704 | atMostOnce (18), quietDone (14) | neverDispatched (10), neverQuiet (13) | yes | 25.7 s |
| redis_wlost_early | 13 | 1439 | quietDone (7) | neverDispatched (10), neverQuiet (7) | yes | 8.9 s |
| redis_child_late | 13 | 1995 | quietDone (8) | neverDispatched (10), neverFailed (8), neverQuiet (8) | yes | 13.9 s |
| redis_child_early | 13 | 1995 | quietDone (8) | neverDispatched (10), neverFailed (8), neverQuiet (8) | yes | 13.9 s |
| redis_child_reject | 15 | 2547 | quietDone (8) | neverDispatched (10), neverQuiet (8) | yes | 11.8 s |
| redis_lease | 25 | 163164 | atMostOnce (18) | neverDispatched (10), neverQuiet (13) | yes | 120.9 s |
| kv_none | 11 | 490 | none | neverDispatched (9), neverQuiet (12) | yes | 6.2 s |
| kv_dup | 21 | 138523 | noErrorAfterDispatch (14), noSpuriousError (9), failsOnce (13) | neverDispatched (9), neverFailed (9), neverQuiet (12) | yes | 72.4 s |
| kv_refuse | 11 | 613 | none | neverDispatched (9), neverFailed (9), neverQuiet (12) | yes | 8.5 s |
| kv_wlost_late | 15 | 5374 | noErrorAfterDispatch (14), noSpuriousError (9), failsOnce (13), quietDone (13) | neverDispatched (9), neverFailed (9), neverQuiet (12) | yes | 25.3 s |
| kv_wlost_early | 11 | 1036 | quietDone (7) | neverDispatched (9), neverQuiet (7) | yes | 9.2 s |
| kv_child_late | 11 | 1618 | quietDone (8) | neverDispatched (9), neverFailed (8), neverQuiet (8) | yes | 12.0 s |
| kv_child_early | 11 | 1618 | quietDone (8) | neverDispatched (9), neverFailed (8), neverQuiet (8) | yes | 13.0 s |
| kv_child_reject | 13 | 1852 | quietDone (8) | neverDispatched (9), neverQuiet (8) | yes | 10.8 s |
| unlock_none | 16 | 3436 | none | neverDispatched (8), neverQuiet (11) | yes | 9.5 s |
| unlock_dup | 24 | 153849 | none | neverDispatched (8), neverQuiet (11) | yes | 59.2 s |
| unlock_refuse | 16 | 5686 | none | neverDispatched (8), neverFailed (3), neverQuiet (10) | yes | 9.8 s |
| unlock_wlost_late | 20 | 29737 | atMostOnce (12) | neverDispatched (8), neverQuiet (11) | yes | 23.2 s |
| unlock_wlost_early | 16 | 10867 | quietDone (6) | neverDispatched (8), neverQuiet (6) | yes | 11.9 s |
| unlock_child_late | 16 | 13441 | quietDone (9) | neverDispatched (8), neverFailed (7), neverQuiet (9) | yes | 16.8 s |
| unlock_child_early | 16 | 13441 | quietDone (9) | neverDispatched (8), neverFailed (7), neverQuiet (9) | yes | 18.8 s |
| unlock_child_reject | 20 | 21700 | atMostOnce (12) | neverDispatched (8), neverQuiet (11) | yes | 22.4 s |
| once_none | 15 | 808 | none | neverDispatched (13), neverQuiet (16) | yes | 7.3 s |
| once_dup | 35 | 905563 | none | neverDispatched (13), neverQuiet (16) | yes | 296.4 s |
| once_refuse | 15 | 931 | none | neverDispatched (13), neverFailed (13), neverQuiet (16) | yes | 10.2 s |
| once_wlost_late | 24 | 15715 | quietDone (20) | neverDispatched (13), neverQuiet (16) | yes | 21.3 s |
| once_wlost_early | 15 | 1750 | quietDone (8) | neverDispatched (13), neverQuiet (8) | yes | 9.2 s |
| once_child_late | 15 | 2890 | quietDone (9) | neverDispatched (13), neverFailed (12), neverQuiet (9) | yes | 13.3 s |
| once_child_early | 15 | 2890 | quietDone (9) | neverDispatched (13), neverFailed (12), neverQuiet (9) | yes | 14.8 s |
| once_child_reject | 17 | 3136 | quietDone (9) | neverDispatched (13), neverQuiet (9) | yes | 11.7 s |
| kv_kill_before_count | 14 | 1567 | none | neverDispatched (9), neverQuiet (12) | yes | 7.6 s |
| skip_kill_before_count | 16 | 3418 | quietDone (14) | neverDispatched (11), neverQuiet (14) | yes | 13.9 s |
| once_kill_before_count | 19 | 4474 | none | neverDispatched (13), neverQuiet (16) | yes | 11.5 s |
| kv_kill_after_claim | 13 | 868 | quietDone (13) | neverDispatched (9), neverQuiet (12) | yes | 9.9 s |
| once_kill_after_claim | 20 | 1438 | quietDone (20) | neverDispatched (13), neverQuiet (16) | yes | 11.0 s |
| skip_none | 13 | 688 | none | neverDispatched (11), neverQuiet (14) | yes | 7.6 s |
| skip_dup | 23 | 149959 | none | neverDispatched (11), neverQuiet (14) | yes | 67.6 s |
| kv_ignore | 10 | 430 | none | neverFailed (8), neverQuiet (11) | yes | 5.6 s |
| once_ignore | 9 | 397 | none | neverFailed (4), neverQuiet (10) | yes | 5.1 s |
| kv_lose_result | 12 | 1832 | none | neverDispatched (9), neverFailed (9), neverQuiet (12) | yes | 8.3 s |
| kv_lose_group | 12 | 1426 | failsOnce (8) | neverDispatched (9), neverFailed (5), neverQuiet (10) | yes | 9.5 s |
| kv_lose_counter | 12 | 1056 | quietDone (10) | neverDispatched (9), neverQuiet (10) | yes | 8.2 s |
| once_lose_result | 16 | 3332 | none | neverDispatched (13), neverFailed (13), neverQuiet (15) | yes | 10.7 s |
| once_lose_group | 16 | 2323 | none | neverDispatched (13), neverFailed (5), neverQuiet (10) | yes | 9.7 s |
| once_lose_counter | 16 | 1662 | quietDone (12) | neverDispatched (13), neverQuiet (12) | yes | 8.5 s |
| once_lose_partkey | 16 | 2528 | none | neverDispatched (13), neverQuiet (16) | yes | 9.7 s |
| once_lose_done | 16 | 1148 | none | neverDispatched (13), neverQuiet (16) | yes | 8.0 s |
| once_dup_wlost | 34 | 1346307 | quietDone (20) | neverDispatched (13), neverQuiet (16) | yes | 530.4 s |
| once_dup_lose_result | 27 | 346535 | none | neverDispatched (13), neverFailed (13), neverQuiet (15) | yes | 120.4 s |
| once_dup_lose_done | 29 | 102324 | atMostOnce (23), noErrorAfterDispatch (19) | neverDispatched (13), neverFailed (19), neverQuiet (16) | yes | 90.8 s |
| once_lose_result_and_key | 17 | 4529 | quietDone (15) | neverDispatched (13), neverFailed (13), neverQuiet (15) | yes | 14.0 s |
| oncev1_dup | 23 | 43188 | noSpuriousError (15) | neverDispatched (13), neverFailed (15), neverQuiet (16) | yes | 36.4 s |
| mixed_none | 15 | 802 | none | neverDispatched (10), neverQuiet (12) | yes | 7.2 s |
| mixed_dup | 25 | 56698 | atMostOnce (19), noErrorAfterDispatch (15), noDispatchAfterError (18), noSpuriousError (10), failsOnce (14), quietDone (17) | neverDispatched (10), neverFailed (10), neverQuiet (12) | yes | 90.3 s |
| mixed_lose_result | 16 | 3430 | quietDone (13) | neverDispatched (10), neverFailed (10), neverQuiet (12) | yes | 15.5 s |
| redis_interrupt | 13 | 898 | quietDone (10) | neverDispatched (10), neverQuiet (10) | yes | 9.7 s |
| redisfix1_interrupt | 13 | 898 | noErrorAfterDispatch (11) | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 13.1 s |
| redisfix2_interrupt | 13 | 898 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 10.9 s |
| redisfix3_interrupt | 13 | 898 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 10.4 s |
| redisfix_interrupt | 13 | 898 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 9.7 s |
| redis_interrupt_dup | 22 | 37810 | atMostOnce (19), quietDone (10) | neverDispatched (10), neverQuiet (10) | yes | 48.2 s |
| redisfix2_interrupt_dup | 22 | 37810 | atMostOnce (19), noDispatchAfterError (16) | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 57.3 s |
| redisfix3_interrupt_dup | 22 | 35492 | atMostOnce (19) | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 41.7 s |
| redisfix_interrupt_dup | 18 | 34333 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) | yes | 27.5 s |
| redisfix_interrupt_sent | 13 | 648 | noErrorAfterDispatch (10) | neverDispatched (10), neverFailed (10), neverQuiet (11) | yes | 13.1 s |
| redisfix_interrupt_reply | 13 | 670 | quietDone (10) | neverDispatched (10), neverQuiet (10) | yes | 9.6 s |

Apalache on the smallest chord instance, for scale: `kv_none`, `steps <= 11`
at depth 12 holds, in 529 s, and `quietDone` at depth 11 holds, in 396 s (TLC:
the whole instance in under 10 s). The other chord instances are left to the
long tier.

### Redis transport

Properties: `inSomePlace`, `neverLost`, `notTwice`, `noDuplicate`,
`prefetchRespected`, `noMissedWake`, `noStranded`, `quietDone`,
`exactlyOnce`. Witnesses:
`neverQuiet`, `neverDuplicated`, `neverPutBack`. `main_` is 445027e, `pr_` is
c61b525, `paused_` is the PR before eb41008, `renewblind_` and `renewown_`
add the renewal, `token_` the pop token (see WORKER.md).

| instance | D | states | violated | reachable (witnesses) | stops only when quiet | time |
|---|---|---|---|---|---|---|
| main_none | 12 | 395 | inSomePlace (4) | neverQuiet (13) | yes | 4.2 s |
| main_crash | 16 | 1385 | inSomePlace (4), neverLost (5), quietDone (11) | neverQuiet (11), neverDuplicated (10) | yes | 9.0 s |
| main_credit | 14 | 949 | inSomePlace (4) | neverQuiet (13) | yes | 4.6 s |
| main_credit_crash | 18 | 3281 | inSomePlace (4), neverLost (5), quietDone (11) | neverQuiet (11), neverDuplicated (10) | yes | 9.8 s |
| main_held_timeout | 17 | 1939 | inSomePlace (4) | neverQuiet (13), neverDuplicated (9) | yes | 6.4 s |
| paused_credit_timeout | 36 | 239843 | notTwice (7), noDuplicate (9) | neverQuiet (15), neverDuplicated (9) | yes | 85.7 s |
| pr_none | 24 | 8993 | none | neverQuiet (15) | yes | 6.7 s |
| pr_crash | 31 | 28251 | none | neverQuiet (14), neverDuplicated (10) | yes | 12.5 s |
| pr_credit | 34 | 55777 | none | neverQuiet (15), neverPutBack (6) | yes | 23.6 s |
| pr_reply_timeout | 33 | 49101 | none | neverQuiet (15) | yes | 21.3 s |
| pr_credit_timeout | 43 | 302577 | none | neverQuiet (15), neverPutBack (6) | yes | 117.2 s |
| pr_mismatch | 24 | 20915 | none | neverQuiet (15) | yes | 12.3 s |
| pr_mismatch_crash | 31 | 67283 | none | neverQuiet (14), neverDuplicated (10) | yes | 24.0 s |
| pr_credit_crash | 41 | 163439 | none | neverQuiet (14), neverDuplicated (10), neverPutBack (6) | yes | 46.9 s |
| pr_held_timeout | 34 | 67065 | none | neverQuiet (15), neverDuplicated (9) | yes | 26.5 s |
| renewblind_held_credit | 44 | 452757 | noStranded (11) | neverQuiet (15), neverDuplicated (9), neverPutBack (6) | yes | 147.5 s |
| renewown_held_credit | 44 | 386487 | none | neverQuiet (15), neverDuplicated (9), neverPutBack (6) | yes | 129.0 s |
| token_none | 24 | 8993 | none | neverQuiet (15) | yes | 7.0 s |
| token_crash | 31 | 28251 | none | neverQuiet (14), neverDuplicated (10) | yes | 13.7 s |
| token_credit | 34 | 55777 | none | neverQuiet (15), neverPutBack (6) | yes | 25.9 s |
| token_reply_timeout | 33 | 49101 | none | neverQuiet (15) | yes | 22.0 s |
| token_credit_timeout | 43 | 302577 | none | neverQuiet (15), neverPutBack (6) | yes | 117.6 s |
| token_mismatch | 24 | 20915 | none | neverQuiet (15) | yes | 12.9 s |
| token_mismatch_crash | 31 | 67283 | none | neverQuiet (14), neverDuplicated (10) | yes | 25.6 s |
| token_credit_crash | 41 | 163439 | none | neverQuiet (14), neverDuplicated (10), neverPutBack (6) | yes | 48.2 s |
| token_held_timeout | 34 | 67065 | none | neverQuiet (15), neverDuplicated (9) | yes | 27.0 s |
| token_renewblind_held_credit | 44 | 471439 | none | neverQuiet (15), neverDuplicated (9), neverPutBack (6) | yes | 148.2 s |

On the consume path the PR makes 3 round trips per message (publish, pop,
ack) where main makes 4 (publish, BRPOP, record, ack). A consumer that finds
the queue empty costs the PR two (the empty pop, then the wait on the wake
list) where main's BRPOP is one; after a pop that emptied the queue it waits
without popping first.

### Fast tier

`python3 tools/check.py`: 90 s for 28 of the 200 instances, 300 samples each. In every simulated instance the longest sampled run is
at most the proven D, and equal to it in most.

## Findings (spec vs. code)

Outcomes are labeled with the user's ranking: a duplicate is the least bad,
then a visible failure, then a hang, then a silent loss.

### Found in the fix branches by these models, now fixed or documented there

Each was a counterexample first, then a `quint test` run in `scenarios.qnt`,
then a run of the real code against a real service (`adapter/confirm_*.py`,
only the order of round trips or the moment of the `SystemExit` controlled).

1. celery issue 11036, cd8aa46 (`oncev1_dup.noSpuriousError`, 15 states,
   `onceReadOnce`). `_complete_chord_once` read the header results, then the
   keys of the missing ones. A duplicate header brings the count to the chord
   size while header 2 still runs; header 2 stores its result and adds its key
   between the two reads, and looks lost: the chord fails with "Chord header
   results missing" although both results are SUCCESS, and the body is never
   sent. On memcached: results `['SUCCESS', 'SUCCESS']`, body published 0
   times. A visible failure with nothing wrong. Fixed in 1fa71d4 (the missing
   results are read again); `once_dup` holds, and the mutant that restores the
   single read is caught.
2. celery issue 11039, 5f844ea (`redisfix1_interrupt.noErrorAfterDispatch`,
   `interruptFirstFix`). The interrupt lands in the cleanup script call, after
   the body was published; `dispatching` was still set, so the chord was
   failed: the errbacks ran and the body ran. On Redis: body published once,
   `ChordError('Callback dispatch interrupted')`, FAILURE stored. A duplicate
   (errback and body). Main leaves that chord alone. Fixed in 5446659.
3. celery issue 11039, 5446659 (`redisfix2_interrupt_dup.noDispatchAfterError`,
   16 states, `interruptSecondFix`). The interrupt lands before the publish,
   the chord is failed, the size key and the results stay, the claim expires,
   and a copy of a header task that returns later takes the claim and sends
   the body. On Redis: "published 0, errors 1" then, after the copy,
   "published 1". A duplicate (errback, then body). Needs the interrupt in
   the join or extend window and a later copy. Fixed in ee48301 (the handler
   drops the size key), and ad00b4a drops it after an interrupted cleanup
   too; `redisfix_interrupt_dup` holds every property.

A fourth counterexample, a hang during a rolling upgrade (`mixed_dup`), is not
a bug in the new code and cannot be closed by it; the fix documents it since
2057a3c. It is listed with what remains.

### What main violates and the fixes fix

4. Incr backends, main (`kv_dup`): a copy of a header that returns after the
   chord finished fails it (`noErrorAfterDispatch`, 14 states: errback after
   success), and two runs of one header bring the count to the size while
   another header runs, so the join times out (`noSpuriousError`, 9 states: a
   visible failure with nothing wrong). Both are celery issue 11036.
   `once_dup` holds all properties.
5. The intermediate fix (`skip_kill_before_count.quietDone`): a worker killed
   between a header's key and its count leaves the part recorded and
   uncounted, and the copy the broker delivers again skips it: the chord
   hangs. Main (`kv_kill_before_count`) and the fix
   (`once_kill_before_count`) complete it.
6. Redis, main (`redis_interrupt.quietDone`): an interrupt after the claim
   leaves the chord neither sent nor failed (celery issue 11039): a hang.
   `redisfix_interrupt` holds all properties.
7. kombu, main: a message is in no key between the pop and the record
   (`main_none.inSomePlace`, 4 states, no fault needed), and a consumer killed
   there loses it for good (`main_crash.neverLost` and `quietDone`): a silent
   loss, kombu issue 305. With the PR every instance holds `inSomePlace` and
   `neverLost`.
8. kombu, the PR before eb41008 (`paused_credit_timeout.noDuplicate`): a
   reply paused without credit for longer than the visibility timeout is
   restored and delivered by another consumer, then delivered by the first: a
   duplicate with no consumer lost. `pr_credit_timeout` holds; the mutant that
   drops `PUT_BACK`'s score check is caught.

### What remains with the fixes (expected violations)

Each line names the instance and property, the precondition, and the outcome.

celery issue 11036, the fix (2057a3c):

* `once_wlost_late`, `once_kill_after_claim`, `once_dup_wlost` (`quietDone`,
  20 states): a worker killed between adding the done key and publishing the
  body, with late ack. The copy delivered again finds the chord claimed and
  returns. Hang. Main does the same (`kv_kill_after_claim`). Documented in
  the fix.
* `mixed_dup` (`quietDone`, 17 states): a rolling upgrade. A duplicate header
  on a new worker brings the count to the size and waits for the last
  header's key; the last header runs on an old worker, whose count is past
  the size, so it only logs. Hang, mixed fleet only; main alone fails this
  chord, the fix alone completes it. Found by this spec, confirmed with
  `adapter/confirm_mixed_fleet.py` (both results SUCCESS, counter 3, body
  never published), and documented in the fix since 2057a3c.
* `mixed_dup` (`atMostOnce`, 19 states): the old worker completes the chord
  without a done key while a duplicate on a new worker completes it too.
  Duplicate body, mixed fleet only, documented. The same instance shows the
  old worker's own faults: `noErrorAfterDispatch`, `noSpuriousError`,
  `failsOnce`, `noDispatchAfterError`.
* `mixed_lose_result` (`quietDone`, 13 states): a lost result of a header an
  old worker ran has no key, so the chord waits. Hang, mixed fleet only,
  documented.
* `once_lose_result_and_key` (`quietDone`, 15 states): a result and its key
  both gone before the last header returns. Hang. Needs two evictions; the
  keys live twice as long as the results to make this rare.
* `once_lose_counter` (`quietDone`, 12 states): the counter evicted. Hang, as
  on main (`kv_lose_counter`).
* `once_dup_lose_done` (`noErrorAfterDispatch`, 19 states, and `atMostOnce`,
  23 states): the done key evicted, then a late copy of a header task. The
  copy finds the group gone and no done key and fails the finished chord, or
  a second run completes the chord. Duplicate (errback after the body, or the
  body twice). The done key lives twice as long as the results.
* `once_wlost_early`, `once_child_*` (`quietDone`): a header lost with early
  ack, and a pool child lost after storing SUCCESS (celery issue 11038).
  Hang, as on main.

With one result lost and its key there (`once_lose_result`,
`once_dup_lose_result`), with the group expired (`once_lose_group`: main
fails that chord once per run, `kv_lose_group.failsOnce`), with a header that
ignores its result (`once_ignore`: the chord fails 4 states in, where main
needs the join timeout), with one key of a header or the done key evicted and
no late copy (`once_lose_partkey`, `once_lose_done`), every property holds:
the chord completes or fails once, visibly.

celery issue 11039, the fix (ad00b4a):

* `redisfix_interrupt_sent` (`noErrorAfterDispatch`, 10 states): the interrupt
  lands inside the publish call after the broker took the body. The code
  cannot tell, and fails the chord. Duplicate (errback and body).
* `redisfix_interrupt_reply` (`quietDone`, 10 states): the interrupt lands in
  the reply of the claim script, before the worker knows it holds the claim.
  Hang. Documented in the fix.
* SIGKILL before the handler finishes (billiard sends it 100 ms after
  SIGTERM) is a lost worker: `redis_wlost_late`.

`redisfix_interrupt` and `redisfix_interrupt_dup` hold every property. The
versions in between show what each commit closed: `redisfix1_interrupt`
(5f844ea) breaks `noErrorAfterDispatch`, `redisfix2_interrupt_dup` (5446659)
`noDispatchAfterError`, and `redisfix3_interrupt_dup` (ee48301) `atMostOnce`:
an interrupt in the cleanup call after the publish left the size key, and a
later copy sent the body again, as on main (`redis_interrupt_dup`,
`adapter/confirm_interrupt_cleanup.py`: main publishes twice, ad00b4a once).

kombu PR 2831:

* `pr_held_timeout`, `main_held_timeout` (witness `neverDuplicated`, 9
  states): a task that outlasts the visibility timeout is restored and
  delivered again while its consumer lives. Duplicate, with no fault. This
  is the transport's contract (at least once).
* `pr_crash` (witness `neverDuplicated`, 10 states): a consumer killed while
  it holds a delivered message. Duplicate; a task that was mid-run cannot be
  told from one that finished.

`traces/cex/` has each counterexample as an ITF trace (`<instance>.<property>.itf.json`), to compare with runs of the real code.

### Earlier findings (768e476), unchanged

9. Read-then-write stores lose an observed SUCCESS (`rtw.successStable`); the
   race is known (celery discussion 10681).
10. A pool child lost between storing SUCCESS and the chord part return leaves
    the chord pending (`*_child_*.quietDone`, celery issue 11038), on Redis,
    on the incr backends and with the fix for 11036 alike.
11. Redis: a claim held by a lost worker blocks a prompt redelivery
    (`redis_wlost_late.quietDone`, `lostClaimHangsTest`).
12. `chord_unlock`: a lost pool child loses the chord (`unlock_child_*`), and
    a worker lost after publishing sends the body twice
    (`unlock_wlost_late.atMostOnce`, celery issue 11037).
13. At-most-once is conditional on Redis: a worker lost after publishing and
    before cleanup, redelivered after the claim TTL, sends the body again
    (`redis_wlost_late.atMostOnce`); a lease that expires under a live holder
    (`redis_lease`) is the trade-off the code documents.

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
* The fix branches: `adapter/confirm_once_reread.py`,
  `confirm_interrupt_cleanup.py`, `confirm_interrupt_then_copy.py` and
  `confirm_mixed_fleet.py` run a scenario's steps on the real backend of a
  given checkout (`PYTHONPATH=<worktree>`); each prints the bad outcome on
  the commit that had it and the good one on the fixed head.

## Mutations

Spec (`tools/mutate_spec.py`): 29 mutants, 27 caught and 2 equivalent, as
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
  through the four `adapter/confirm_*.py` scripts and the scenarios they
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
