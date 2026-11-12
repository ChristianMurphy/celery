# Canvas: chords and chains

`chord.qnt`, `chord_instances.qnt` (69 instances), `scenarios.qnt` (the
behaviors behind the findings, as `quint test` runs). Celery 768e476 and the
fix branches for celery issues 11036 and 11039.
The actions of `chord.qnt` are in one section per protocol (every protocol:
delivery and the store; Redis; Kv; KvSkip and KvOnce; Unlock; every
protocol: the publish and the finish), then the faults; each cites the code
it models with a checked `@cite`. The rearrangement changed no result
(`../tools/baseline/README.md`).

`chain.qnt`, `chain_instances.qnt`: a chain with a link callback, on the
abstract delivery environment of `../lib/delivery.qnt`. See
[Chains](#chains) below.

`nested.qnt`, `nested_instances.qnt`: a chord with a chord nested in its
header or a task that replaces itself, and the chord's errback. See
[Nested chords, replace, groups and chains](#nested-chords-replace-groups-and-chains)
below.

## Model

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

Quiet, per protocol:

* Redis, incr backends: no message queued, no process busy, no claim left to
  expire (an expired claim lets a later copy claim).
* `chord_unlock`: no header message queued or running, and either every header
  result is stored and no `chord_unlock` is queued or running, or a result is
  missing and `chord_unlock` can only poll again. The real task retries
  without limit; the spec gives the retries a budget of 2 so that runs are
  finite. A retry changes nothing but where the `chord_unlock` message is, so
  a second one adds no behavior.

## Properties

<!-- generated: properties chord.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `atMostOnce` | safety | The chord body is dispatched at most once. Assumes: two header tasks, three processes; holds on Redis while no claim expires under a live holder. | Bounded existence (at most one dispatch), global | celery/backends/redis.py:139 (768e476): the claim is `SET NX`; celery PR 10998; docs/userguide/canvas.rst:905-906 ("a task that only executes after all of the tasks in a group have finished") |
| `noPremature` | safety | The body is not dispatched while a header result is missing. | Precedence: every header result stored precedes the dispatch | docs/userguide/canvas.rst:905-906, 965 (celery 768e476): "the callback can only be executed after all of the tasks in the header have returned" |
| `argsComplete` | safety | The body gets exactly one result per header task. | Universality after the dispatch | docs/userguide/canvas.rst:905-906; celery PR 10919 (the Redis script dedupes a part that returns twice) |
| `noErrorAfterDispatch` | safety | A chord whose body was dispatched is not failed afterwards. | Absence of a chord error, after the dispatch | celery/backends/base.py:464-520 (768e476): chord_error_from_stack stores FAILURE for the body id and calls its errbacks |
| `noDispatchAfterError` | safety | The body is not dispatched after the chord was failed. | Absence of a dispatch, after a chord error | docs/userguide/canvas.rst:980 (celery 768e476): "The chord callback result will transition to the failure state" |
| `noSpuriousError` | safety | The chord fails only for a header FAILURE, a refused publish, a lost key, a header that ignores its result, or an interrupted dispatch. | Precedence: a cause precedes the chord error | docs/userguide/canvas.rst:973-980 (celery 768e476): the chord fails when a header task raises |
| `failsOnce` | safety | The chord is failed at most once. | Bounded existence (at most one chord error), global | docs/userguide/canvas.rst:1055 (celery 768e476): the errback is called once |
| `quietDone` | liveness | A quiet chord has its body dispatched or is failed. A violation is a hang. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes; `chord_unlock` retries have a budget of 2. | Response: a published chord leads to a dispatched body or a failed chord (lemma 3 of 3) | docs/userguide/canvas.rst:905-906, 973-980 (celery 768e476) |
| `progress` | liveness | steps + measure never exceeds its initial value, and measure is never negative: every run is finite. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the spec: every action lowers `measure` |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: nothing is left that could change the chord's outcome without a new fault. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Absence of a stop outside a quiet state (lemma 2 of 3) | the spec: `quiet`, checked by TLC with deadlock detection on `stepOrQuiet` |
| `neverDispatched` | witness | Some run dispatches the body. | Existence | sanity |
| `neverFailed` | witness | Some run fails the chord. | Existence | sanity |
| `neverQuiet` | witness | Some run ends. | Existence | sanity |
<!-- end generated -->

## Mapping to the code

| Spec action | Code |
|---|---|
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

Each action in the `.qnt` file carries the same citation in a comment.

## Results

`python3 tools/check.py --tier full --only chord`. D is the longest run in
steps; a number after a property is the number of states in a shortest
counterexample; "reachable" lists witnesses, where failing means the state
can be reached. In every instance a run stops only in a quiet state, and
`progress` holds.

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

<!-- generated: results chord.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| kv_child_early | 11 | quietDone (8) | neverDispatched (9), neverFailed (8), neverQuiet (8) |
| kv_child_late | 11 | quietDone (8) | neverDispatched (9), neverFailed (8), neverQuiet (8) |
| kv_child_reject | 13 | quietDone (8) | neverDispatched (9), neverQuiet (8) |
| kv_dup | 21 | noErrorAfterDispatch (14), noSpuriousError (9), failsOnce (13) | neverDispatched (9), neverFailed (9), neverQuiet (12) |
| kv_ignore | 10 | none | neverFailed (8), neverQuiet (11) |
| kv_kill_after_claim | 13 | quietDone (13) | neverDispatched (9), neverQuiet (12) |
| kv_kill_before_count | 14 | none | neverDispatched (9), neverQuiet (12) |
| kv_lose_counter | 12 | quietDone (10) | neverDispatched (9), neverQuiet (10) |
| kv_lose_group | 12 | failsOnce (8) | neverDispatched (9), neverFailed (5), neverQuiet (10) |
| kv_lose_result | 12 | none | neverDispatched (9), neverFailed (9), neverQuiet (12) |
| kv_none | 11 | none | neverDispatched (9), neverQuiet (12) |
| kv_refuse | 11 | none | neverDispatched (9), neverFailed (9), neverQuiet (12) |
| kv_wlost_early | 11 | quietDone (7) | neverDispatched (9), neverQuiet (7) |
| kv_wlost_late | 15 | noErrorAfterDispatch (14), noSpuriousError (9), failsOnce (13), quietDone (13) | neverDispatched (9), neverFailed (9), neverQuiet (12) |
| mixed_dup | 25 | atMostOnce (19), noErrorAfterDispatch (15), noDispatchAfterError (18), noSpuriousError (10), failsOnce (14), quietDone (17) | neverDispatched (10), neverFailed (10), neverQuiet (12) |
| mixed_lose_result | 16 | quietDone (13) | neverDispatched (10), neverFailed (10), neverQuiet (12) |
| mixed_none | 15 | none | neverDispatched (10), neverQuiet (12) |
| once_child_early | 15 | quietDone (9) | neverDispatched (13), neverFailed (12), neverQuiet (9) |
| once_child_late | 15 | quietDone (9) | neverDispatched (13), neverFailed (12), neverQuiet (9) |
| once_child_reject | 17 | quietDone (9) | neverDispatched (13), neverQuiet (9) |
| once_dup | 35 | none | neverDispatched (13), neverQuiet (16) |
| once_dup_lose_done | 29 | atMostOnce (23), noErrorAfterDispatch (19) | neverDispatched (13), neverFailed (19), neverQuiet (16) |
| once_dup_lose_result | 27 | none | neverDispatched (13), neverFailed (13), neverQuiet (15) |
| once_dup_wlost | 34 | quietDone (20) | neverDispatched (13), neverQuiet (16) |
| once_ignore | 9 | none | neverFailed (4), neverQuiet (10) |
| once_kill_after_claim | 20 | quietDone (20) | neverDispatched (13), neverQuiet (16) |
| once_kill_before_count | 19 | none | neverDispatched (13), neverQuiet (16) |
| once_lose_counter | 16 | quietDone (12) | neverDispatched (13), neverQuiet (12) |
| once_lose_done | 16 | none | neverDispatched (13), neverQuiet (16) |
| once_lose_group | 16 | none | neverDispatched (13), neverFailed (5), neverQuiet (10) |
| once_lose_partkey | 16 | none | neverDispatched (13), neverQuiet (16) |
| once_lose_result | 16 | none | neverDispatched (13), neverFailed (13), neverQuiet (15) |
| once_lose_result_and_key | 17 | quietDone (15) | neverDispatched (13), neverFailed (13), neverQuiet (15) |
| once_none | 15 | none | neverDispatched (13), neverQuiet (16) |
| once_refuse | 15 | none | neverDispatched (13), neverFailed (13), neverQuiet (16) |
| once_wlost_early | 15 | quietDone (8) | neverDispatched (13), neverQuiet (8) |
| once_wlost_late | 24 | quietDone (20) | neverDispatched (13), neverQuiet (16) |
| oncev1_dup | 23 | noSpuriousError (15) | neverDispatched (13), neverFailed (15), neverQuiet (16) |
| redis_child_early | 13 | quietDone (8) | neverDispatched (10), neverFailed (8), neverQuiet (8) |
| redis_child_late | 13 | quietDone (8) | neverDispatched (10), neverFailed (8), neverQuiet (8) |
| redis_child_reject | 15 | quietDone (8) | neverDispatched (10), neverQuiet (8) |
| redis_dup | 23 | none | neverDispatched (10), neverQuiet (13) |
| redis_interrupt | 13 | quietDone (10) | neverDispatched (10), neverQuiet (10) |
| redis_interrupt_dup | 22 | atMostOnce (19), quietDone (10) | neverDispatched (10), neverQuiet (10) |
| redis_lease | 25 | atMostOnce (18) | neverDispatched (10), neverQuiet (13) |
| redis_none | 13 | none | neverDispatched (10), neverQuiet (13) |
| redis_refuse | 13 | none | neverDispatched (10), neverFailed (10), neverQuiet (13) |
| redis_wlost_early | 13 | quietDone (7) | neverDispatched (10), neverQuiet (7) |
| redis_wlost_late | 21 | atMostOnce (18), quietDone (14) | neverDispatched (10), neverQuiet (13) |
| redisfix1_interrupt | 13 | noErrorAfterDispatch (11) | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix2_interrupt | 13 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix2_interrupt_dup | 22 | atMostOnce (19), noDispatchAfterError (16) | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix3_interrupt | 13 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix3_interrupt_dup | 22 | atMostOnce (19) | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix_interrupt | 13 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix_interrupt_dup | 18 | none | neverDispatched (10), neverFailed (8), neverQuiet (10) |
| redisfix_interrupt_reply | 13 | quietDone (10) | neverDispatched (10), neverQuiet (10) |
| redisfix_interrupt_sent | 13 | noErrorAfterDispatch (10) | neverDispatched (10), neverFailed (10), neverQuiet (11) |
| skip_dup | 23 | none | neverDispatched (11), neverQuiet (14) |
| skip_kill_before_count | 16 | quietDone (14) | neverDispatched (11), neverQuiet (14) |
| skip_none | 13 | none | neverDispatched (11), neverQuiet (14) |
| unlock_child_early | 16 | quietDone (9) | neverDispatched (8), neverFailed (7), neverQuiet (9) |
| unlock_child_late | 16 | quietDone (9) | neverDispatched (8), neverFailed (7), neverQuiet (9) |
| unlock_child_reject | 20 | atMostOnce (12) | neverDispatched (8), neverQuiet (11) |
| unlock_dup | 24 | none | neverDispatched (8), neverQuiet (11) |
| unlock_none | 16 | none | neverDispatched (8), neverQuiet (11) |
| unlock_refuse | 16 | none | neverDispatched (8), neverFailed (3), neverQuiet (10) |
| unlock_wlost_early | 16 | quietDone (6) | neverDispatched (8), neverQuiet (6) |
| unlock_wlost_late | 20 | atMostOnce (12) | neverDispatched (8), neverQuiet (11) |
<!-- end generated -->

Why each violation is expected:

<!-- generated: reasons chord.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `atMostOnce` | mixed_dup (19) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `atMostOnce` | once_dup_lose_done (23) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4). needs the done key evicted and a late copy; the done key lives twice as long as the results |
| `atMostOnce` | redis_interrupt_dup (19) | open | tracker chord-time-limit-marks-done (celery issue 11039) |
| `atMostOnce` | redis_lease (18) | by design | celery/backends/redis.py:169-170 (768e476): the extend script may "reacquire an expired lease while chord data remains". a lease that expires under a live holder |
| `atMostOnce` | redis_wlost_late (18) | by design | celery/backends/redis.py:169-170 (768e476). a worker lost after the publish and before the cleanup, redelivered after the claim TTL |
| `atMostOnce` | redisfix2_interrupt_dup (19) | fixed | ee48301 on the fix branch for celery issue 11039 (ad00b4a) |
| `atMostOnce` | redisfix3_interrupt_dup (19) | fixed | ad00b4a on the fix branch for celery issue 11039 (ad00b4a) |
| `atMostOnce` | unlock_child_reject (12) | open | tracker chord-unlock-dispatch-twice (celery issue 11037) |
| `atMostOnce` | unlock_wlost_late (12) | open | tracker chord-unlock-dispatch-twice (celery issue 11037) |
| `noErrorAfterDispatch` | kv_dup (14) | open | tracker chord-incr-errback-after-success (celery issue 11036) |
| `noErrorAfterDispatch` | kv_wlost_late (14) | open | tracker chord-incr-errback-after-success (celery issue 11036) |
| `noErrorAfterDispatch` | mixed_dup (15) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `noErrorAfterDispatch` | once_dup_lose_done (19) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4). needs the done key evicted and a late copy |
| `noErrorAfterDispatch` | redisfix1_interrupt (11) | fixed | 5446659 on the fix branch for celery issue 11039 (ad00b4a) |
| `noErrorAfterDispatch` | redisfix_interrupt_sent (10) | by design | the fix branch for celery issue 11039 (ad00b4a). the interrupt lands inside the publish call after the broker took the body; the code cannot tell |
| `noDispatchAfterError` | mixed_dup (18) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `noDispatchAfterError` | redisfix2_interrupt_dup (16) | fixed | ee48301 on the fix branch for celery issue 11039 (ad00b4a) |
| `noSpuriousError` | kv_dup (9) | open | tracker chord-incr-double-count (follow-up on celery issue 11036) |
| `noSpuriousError` | kv_wlost_late (9) | open | tracker chord-incr-double-count (follow-up on celery issue 11036) |
| `noSpuriousError` | mixed_dup (10) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `noSpuriousError` | oncev1_dup (15) | fixed | 1fa71d4 on the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4) |
| `failsOnce` | kv_dup (13) | open | tracker chord-incr-errback-after-success (celery issue 11036) |
| `failsOnce` | kv_wlost_late (13) | open | tracker chord-incr-errback-after-success (celery issue 11036) |
| `failsOnce` | kv_lose_group (8) | open | tracker chord-incr-errback-after-success (celery issue 11036). main fails the chord once per run that finds the group gone; the fix fails it once |
| `failsOnce` | mixed_dup (14) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `quietDone` | unlock_child_early (9), unlock_child_late (9) | open | tracker chord-unlock-lost-child (follow-up on celery issue 11037) |
| `quietDone` | kv_child_early (8), kv_child_late (8), kv_child_reject (8), once_child_early (9), once_child_late (9), once_child_reject (9), redis_child_early (8), redis_child_late (8), redis_child_reject (8) | open | tracker chord-lost-child-after-success (celery issue 11038) |
| `quietDone` | kv_wlost_early (7), once_wlost_early (8), redis_wlost_early (7), unlock_wlost_early (6) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): early ack, "so that a task invocation that already started is never executed again". a header lost with early ack is not run again, so the chord waits |
| `quietDone` | redis_wlost_late (14) | open | tracker chord-claim-blocks-redelivery (part of celery issue 11038) |
| `quietDone` | kv_wlost_late (13) | open | tracker chord-incr-double-count (follow-up on celery issue 11036) |
| `quietDone` | kv_kill_after_claim (13) | open | tracker chord-incr-double-count (follow-up on celery issue 11036) |
| `quietDone` | kv_lose_counter (10), once_lose_counter (12) | by design | celery/backends/base.py:1438-1466 (768e476): the counter is one key. needs the counter key evicted |
| `quietDone` | once_wlost_late (20) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4). a worker killed between adding the done key and publishing the body; as on main |
| `quietDone` | once_kill_after_claim (20) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4) |
| `quietDone` | once_dup_wlost (20) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4) |
| `quietDone` | mixed_dup (17), mixed_lose_result (13) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4), which documents the rolling upgrade. mixed fleet only: one process still runs main |
| `quietDone` | once_lose_result_and_key (15) | by design | the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4). needs two evictions; the keys live twice as long as the results |
| `quietDone` | redis_interrupt (10), redis_interrupt_dup (10) | open | tracker chord-time-limit-marks-done (celery issue 11039) |
| `quietDone` | redisfix_interrupt_reply (10) | by design | the fix branch for celery issue 11039 (ad00b4a). the interrupt lands in the reply of the claim script |
| `quietDone` | skip_kill_before_count (14) | fixed | 2057a3c on the fix branch for celery issue 11036 (2057a3c, code as at 1fa71d4) (09e5a59 was the intermediate fix) |
<!-- end generated -->

Apalache on the smallest chord instance, for scale: `kv_none`, `steps <= 11`
at depth 12 holds, in 529 s, and `quietDone` at depth 11 holds, in 396 s (TLC:
the whole instance in under 10 s). The other chord instances are left to the
long tier.

## Findings

Outcomes are labeled with the user's ranking: a duplicate is the least bad,
then a visible failure, then a hang, then a silent loss. The findings that
were confirmed on the real code have a folder in [../findings/](../findings/README.md).

### Found in the fix branches by these models, now fixed or documented there

Each was a counterexample first, then a `quint test` run in `scenarios.qnt`,
then a run of the real code against a real service (`../findings/*/confirm_*.py`,
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
  `../findings/mixed-fleet/confirm_mixed_fleet.py` (both results SUCCESS, counter 3, body
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
`../findings/interrupt-cleanup/confirm_interrupt_cleanup.py`: main publishes twice, ad00b4a once).

### Earlier findings (768e476), unchanged

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

## Chains

`chain.qnt` models `a.s() | b.s()` where `a` also has a link `l` and an
errback `e`, on two workers with one pool process each. The broker is the
abstract at-least-once environment of `../lib/delivery.qnt` (publish,
deliver, ack, requeue, deliver again); `../worker/worker_projection.qnt`
checks that every step of `worker.qnt`, the detailed Redis model, is a step
of that environment, so what holds here holds on any broker whose steps
project the same way.

A successful run of `a` (celery/app/trace.py:584-613 at 768e476): the body
returns, the link is published, then the next step, each through
`AMQP._retry_publish`; then `mark_as_done` stores SUCCESS with the children;
with late ack the message is acked after that. The publish comes before the
store on purpose ("callback tasks must be applied before the result is
stored, so that result.children is populated"). Any error after the body
returned goes to `on_error`: FAILURE for `a`, FAILURE for every later step
(`_fail_chain`), the errbacks. A pool process lost while the parent lives
goes through `Request.on_failure`: ack if SUCCESS is stored, requeue with
`reject_on_worker_lost`, otherwise the same FAILURE path.

Instances: the ack mode or design (`late`, `early`, `dedup` for
`worker_deduplicate_successful_tasks`, `reject` for
`task_reject_on_worker_lost`) and a fault class: `none`, `raises` (the body
of `a` raises), `node` (a worker killed with its pool), `child` (a pool
process lost), `refuse` (a publish refused until the retry policy gives
up), `storeerr` (`mark_as_done` raises once: a result the serializer
cannot encode, or a backend that stays down past its retry policy), `dup`
(a message delivered again).

<!-- generated: properties chain.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `nextOnce` | safety | The next step of the chain is published at most once. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Bounded existence (at most one publish), global | docs/userguide/canvas.rst:524-525 (celery 768e476): "The linked task will be applied with the result of its parent task"; celery/app/trace.py:414-417 (768e476) says a redelivery may publish it again |
| `linkOnce` | safety | The link callback is published at most once. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Bounded existence (at most one publish), global | docs/userguide/canvas.rst:515-516 (celery 768e476): "the linked task is called when the task returns successfully" |
| `nextAfterReturn` | safety | The next step is published only after a run of its parent returned without an error. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Precedence (a run of a returned precedes the publish of b), global | docs/userguide/canvas.rst:515-516 (celery 768e476): "the linked task is called when the task returns successfully" |
| `notBoth` | safety | A task's link callback and its errback are not both called. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Absence (link and errback for one task), global | docs/userguide/calling.rst:197-199 (celery 768e476): callbacks are "called with the return value of the parent task"; canvas.rst:515-516 |
| `errbackOnce` | safety | The errback runs at most once. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Bounded existence (at most one errback run), global | docs/userguide/calling.rst:140-142 (celery 768e476) |
| `finalStable` | safety | Once b's stored result is ready (SUCCESS or FAILURE), it does not change. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Absence (a ready result of b that changes), global | celery/backends/base.py:1361-1366 (768e476): a stored SUCCESS is not overwritten, "a lost task overwrite the last successful result" |
| `noFailedParentAfterSend` | safety | A reader never sees FAILURE for a task whose link callback or next step was sent, and so runs. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Absence (FAILURE stored for a task whose link or next step was sent), global | docs/userguide/canvas.rst:515-516 (celery 768e476): "the linked task is called when the task returns successfully" |
| `exactlyOnce` | safety | While no fault was used, each task that was sent starts once, and a quiet chain has a final result for each. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Bounded existence (exactly one run of each task sent), while no fault was used | docs/userguide/canvas.rst:515-516 (celery 768e476) |
| `quietDone` | liveness | A quiet chain has a final result for a and b, and for l if it was sent: nothing waits for ever. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Response (lemma 3 of 3: a quiet chain has ended) | docs/userguide/canvas.rst:515-516 (celery 768e476) |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: fault budgets are finite; exhaustive searches of small instances, not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: a recovery step (a delivery, an ack, a requeue) is never refused for ever: weak fairness, built into quiet. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Existence | sanity |
| `neverDone` | witness | Some run stores SUCCESS for b. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Existence | sanity |
| `neverFailed` | witness | Some run stores FAILURE for a. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Existence | sanity |
| `neverEarlyStart` | witness | b can start while a's SUCCESS is not stored yet: the links are published before the result is stored. Assumes: a chain a | b with a link l and an errback e on a; two workers with one pool process each; the abstract delivery environment (lib/delivery.qnt). | Existence | celery/app/trace.py:591-593 (768e476): "callback tasks must be applied before the result is stored, so that result.children is populated" |
<!-- end generated -->

<!-- generated: results chain.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| dedup_dup | 26 | nextOnce (11), linkOnce (9) | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| dedup_node | 24 | nextOnce (11), linkOnce (9) | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| early_child | 13 | notBoth (6), finalStable (9), noFailedParentAfterSend (6) | neverQuiet (4), neverDone (8), neverFailed (4), neverEarlyStart (7) |
| early_node | 13 | quietDone (4) | neverQuiet (4), neverDone (8), neverEarlyStart (7) |
| early_none | 13 | none | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| early_unencodable | 12 | notBoth (7), finalStable (9), noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| encfirst_none | 13 | none | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| encfirst_storeerr | 13 | notBoth (7), finalStable (9), noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| encfirst_unencodable | 4 | none | neverQuiet (5), neverFailed (5) |
| late_child | 13 | notBoth (6), finalStable (9), noFailedParentAfterSend (6) | neverQuiet (4), neverDone (8), neverFailed (4), neverEarlyStart (7) |
| late_dup | 26 | nextOnce (11), linkOnce (9) | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| late_node | 25 | nextOnce (11), linkOnce (9) | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| late_none | 13 | none | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| late_raises | 3 | none | neverQuiet (4), neverFailed (4) |
| late_refuse | 13 | notBoth (6), noFailedParentAfterSend (6) | neverQuiet (5), neverDone (8), neverFailed (5), neverEarlyStart (7) |
| late_storeerr | 13 | notBoth (7), finalStable (9), noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| late_unencodable | 12 | notBoth (7), finalStable (9), noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| quiet_child | 13 | notBoth (6), finalStable (9), noFailedParentAfterSend (6) | neverQuiet (4), neverDone (8), neverFailed (4), neverEarlyStart (7) |
| quiet_none | 13 | none | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
| quiet_refuse | 13 | notBoth (6), noFailedParentAfterSend (6) | neverQuiet (5), neverDone (8), neverFailed (5), neverEarlyStart (7) |
| quiet_storeerr | 13 | noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| quiet_unencodable | 12 | noFailedParentAfterSend (7) | neverQuiet (13), neverDone (8), neverFailed (7), neverEarlyStart (7) |
| reject_child | 24 | nextOnce (11), linkOnce (9) | neverQuiet (14), neverDone (8), neverEarlyStart (7) |
<!-- end generated -->

What the model says:

* A worker killed after the publish and before the store or the ack
  (`late_node`, 11 states), a duplicate delivery (`late_dup`) and
  `reject_on_worker_lost` (`reject_child`) publish the next step and the
  link twice. Deduplication does not help: SUCCESS is not stored yet when
  the copy runs (`dedup_node`, `dedup_dup`). Documented in the code
  (trace.py:414-417) and for the settings.
* With early ack a worker lost in the run leaves `a` PENDING for good
  (`early_node`, `quietDone`, 4 states): documented.
* Every failure after the link was sent runs the errback beside the link,
  marks `b` FAILURE, and `b` then stores SUCCESS (`notBoth`, `finalStable`,
  `noFailedParentAfterSend`): a lost pool process (celery issue 10954), a
  refused next step, a store error, and deterministically a result the
  serializer cannot encode (`late_unencodable`, 7 states). Confirmed on main:
  [../findings/chain-store-error](../findings/chain-store-error/).
* Two fix designs. `encodeFirst` (encode the result before publishing)
  passes everything for the unencodable result and nothing else changes:
  a store error or a lost process still breaks `notBoth` and
  `finalStable`. `quietAfterSend` (once the callbacks were sent, a store
  error stores FAILURE for `a` only: no `_fail_chain`, no errbacks)
  restores `notBoth` and `finalStable` for store errors and unencodable
  results; `a` still reads FAILURE while its link and next step run
  (`noFailedParentAfterSend`), and the lost-process and refused-publish
  instances are unchanged, since the parent process does not know what the
  child sent. It moves no outcome right on the ranking: the error stays
  visible as `a`'s FAILURE, only the errback no longer runs for a task
  whose link ran.

Why each violation is expected:

<!-- generated: reasons chain.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `nextOnce` | dedup_node (11), late_node (11) | by design | celery/app/trace.py:414-417 (768e476): "a Reject + redeliver may re-dispatch the already-sent callbacks. This is acceptable under Celery's at-least-once delivery model". a worker killed after the publish and before the store or the ack; deduplication does not help before the store |
| `nextOnce` | reject_child (11) | documented | docs/userguide/configuration.rst:732-740 (celery 768e476): task_reject_on_worker_lost re-queues the message "so that the task will execute again", with a warning about message loops |
| `nextOnce` | dedup_dup (11), late_dup (11) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged in time "will be redelivered to another worker and executed"; celery/app/trace.py:414-417 (768e476): "a Reject + redeliver may re-dispatch the already-sent callbacks. This is acceptable under Celery's at-least-once delivery model". deduplication helps only once SUCCESS is stored |
| `linkOnce` | dedup_node (9), late_node (9) | by design | celery/app/trace.py:414-417 (768e476): "a Reject + redeliver may re-dispatch the already-sent callbacks. This is acceptable under Celery's at-least-once delivery model". a worker killed after the publish and before the store or the ack; deduplication does not help before the store |
| `linkOnce` | reject_child (9) | documented | docs/userguide/configuration.rst:732-740 (celery 768e476): task_reject_on_worker_lost re-queues the message "so that the task will execute again", with a warning about message loops |
| `linkOnce` | dedup_dup (9), late_dup (9) | documented | docs/getting-started/backends-and-brokers/redis.rst:315-316 (celery 768e476): a task not acknowledged in time "will be redelivered to another worker and executed"; celery/app/trace.py:414-417 (768e476): "a Reject + redeliver may re-dispatch the already-sent callbacks. This is acceptable under Celery's at-least-once delivery model". deduplication helps only once SUCCESS is stored |
| `notBoth` | early_child (6), late_child (6), quiet_child (6) | open | celery issue 10954. a pool process lost between the publish and the store |
| `notBoth` | late_refuse (6), quiet_refuse (6) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). the link was sent, then the next step was refused; calling.rst says the task then fails through the normal errback handling |
| `notBoth` | late_storeerr (7) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). a backend error at the store (kombu OperationalError is re-raised and reaches the same on_error): 3 of 3 runs on a real worker |
| `notBoth` | early_unencodable (7), late_unencodable (7) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). a result the serializer cannot encode: deterministic, 5 of 5 runs on a real worker |
| `notBoth` | encfirst_storeerr (7) | by design | the encode-first variant (CFG.encodeFirst) removes only the encode trigger; a backend error at the store is the same window |
| `finalStable` | early_child (9), late_child (9), quiet_child (9) | open | celery issue 10954. a pool process lost between the publish and the store |
| `finalStable` | late_storeerr (9) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). a backend error at the store (kombu OperationalError is re-raised and reaches the same on_error): 3 of 3 runs on a real worker |
| `finalStable` | early_unencodable (9), late_unencodable (9) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). a result the serializer cannot encode: deterministic, 5 of 5 runs on a real worker |
| `finalStable` | encfirst_storeerr (9) | by design | the encode-first variant (CFG.encodeFirst) removes only the encode trigger; a backend error at the store is the same window |
| `noFailedParentAfterSend` | quiet_child (6), quiet_refuse (6), quiet_storeerr (7), quiet_unencodable (7) | by design | the quiet-after-send variant (CFG.quietAfterSend) still records the store error as FAILURE for a; it only stops _fail_chain and the errbacks |
| `noFailedParentAfterSend` | encfirst_storeerr (7) | by design | the encode-first variant removes only the encode trigger |
| `noFailedParentAfterSend` | early_child (6), early_unencodable (7), late_child (6), late_refuse (6), late_storeerr (7), late_unencodable (7) | open | finding chain-store-error; celery issue 10954 (a pool child lost after the link was published). a FAILURE for a task whose link and next step run |
| `quietDone` | early_node (4) | documented | docs/userguide/tasks.rst:24-26 (celery 768e476): with early ack "a task invocation that already started is never executed again"; a run lost after the ack leaves its result PENDING. a hang for anyone waiting on a's result; b is not affected |
<!-- end generated -->

## Nested chords, replace, groups and chains

`nested.qnt`, `nested_instances.qnt` (9 instances; celery 9c2f238, after
celery PR 10778): chord([X, Y], body) on the Redis result backend, where X
is a task, a chord nested in the header, or a task that replaces itself;
the header tasks that fail per instance; an errback given by `on_error()`
or as `apply_async`'s `link_error`.

<!-- generated: properties nested.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `bodyOnlyOnSuccess` | safety | The chord body runs only if no header task failed. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Absence (the body runs after a header task failed), global | docs/userguide/canvas.rst:905-906 (celery 9c2f238): the body executes after all of the tasks in the header have finished |
| `errbackOnce` | safety | The chord's errback is called at most once. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Bounded existence (at most one errback call), global | docs/userguide/canvas.rst:1030-1034 (celery 9c2f238): errbacks just once if any task in the chord header or body fails |
| `quietDone` | liveness | At the end the outer chord's body ran or is stored FAILURE. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Response (lemma 3 of 3: at the end the chord body ran or is stored FAILURE) | docs/userguide/canvas.rst:905-906 and 1030-1034 (celery 9c2f238) |
| `errbackOnFailure` | liveness | At the end, if a header task failed, the chord's errback was called. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Response (a failed header task is followed by an errback call) | docs/userguide/canvas.rst:1030-1050 (celery 9c2f238): errbacks just once if any task in the chord header or body fails; link_error passed to chord.apply_async() is linked the same way chord.link_error() links it |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Existence | sanity |
| `neverErrback` | witness | The errback is called. Assumes: chord([X, Y], body) on the Redis result backend; X a task, a chord with two header tasks, or a task that replaces itself with a task; header tasks that return or raise as the instance says; the errback given by on_error() or as apply_async's link_error; task_allow_error_cb_on_chord_header False; no duplicates or lost workers. | Existence | sanity |
<!-- end generated -->

<!-- generated: results nested.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| flat_fail_option | 2 | none | neverQuiet (3), neverErrback (3) |
| flat_ok_option | 3 | none | neverQuiet (4) |
| nested_bothfail_onerror | 3 | quietDone (4) | neverQuiet (4), neverErrback (3) |
| nested_fail_onerror | 3 | quietDone (4) | neverQuiet (4), neverErrback (3) |
| nested_fail_option | 3 | quietDone (4), errbackOnFailure (4) | neverQuiet (4) |
| nested_ok_option | 5 | none | neverQuiet (6) |
| nested_yfail_option | 4 | none | neverQuiet (5), neverErrback (5) |
| replaced_fail_option | 3 | none | neverQuiet (4), neverErrback (4) |
| replaced_ok_option | 4 | none | neverQuiet (5) |
<!-- end generated -->

<!-- generated: reasons nested.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `quietDone` | nested_bothfail_onerror (4), nested_fail_onerror (4) | open | celery issue 10783 (open) says the enclosing chord does not complete when a nested chord's header fails, as designed (test_nested_chord_header_link_error), with the errback as the signal; celery PR 10767 (merged) lists the case as still pending. chord_error_from_stack fails the inner body and returns no part to the outer chord (base.py:464-503) |
| `quietDone` | nested_fail_option (4) | open | finding chord-option-errback-nested; the hang itself is celery issue 10783's as designed. the same hang, with no errback either |
| `errbackOnFailure` | nested_fail_option (4) | open | finding chord-option-errback-nested (new on main with celery PR 10778; 5.5.3 raised TypeError at the call); closest celery issue 10783 and celery PR 10784, the same symptom through chain(...).on_error(). apply_async's link_error goes to the outer body only (canvas.py:2583-2600); the nested chord's special case in chord.link_error() is skipped |
<!-- end generated -->

Finding [chord-option-errback-nested](../findings/chord-option-errback-nested/)
(a hang): `outer.apply_async(link_error=eb)` with a nested chord whose own
header fails: the outer chord stays PENDING and `eb` is never called;
`outer.on_error(eb)` calls it once. On main only: 5.5.3 and 5.4.0 raise
TypeError at the call.

What a scratchpad probe on 9c2f238 (kombu 445027e, Redis 7, -c 2, nothing
injected, 1 of 1 each unless said) did for the shapes the spec leaves out
or passes:

| Case | Result | Status |
|---|---|---|
| `Task.replace` with a task, a chain, a failing task in a chord header | the body got the replacement's value (103, 1103) or failed with ChordError | as `replaced_*` |
| `Task.replace` with a group in a chord header | the body got the group's list as that member's value: tsum([1, [102, 202]]) | the replaced task's value is the group's result |
| `Task.replace` in the middle of a chain, with a task or a group | the chain went on with the replacement's value (107; 304) | passes |
| chain(add, group(add, boom), tsum) with link_error | FAILURE (ChordError), the errback once | passes |
| `group.link(cb)` | cb ran when the first task succeeded, also with the second failing; not when the first failed | documented, canvas.rst:736-756 |
| `group.link_error(eb)` with two failing tasks | eb ran twice | documented, canvas.rst:758-772 |
| `group.apply_async(link=cb)` | TypeError: Cannot add link to group: use a chord | visible at the call |
| a chord in a chord header, all succeed | 13, as expected | passes (`nested_ok_option`) |
| `group([])`, `chord([], tsum)`, chain(add, chord([], tsum)) | an empty GroupResult, ready, get() []; tsum([]) ran, 0 | passes |
| chain with an immutable step; a link on a middle step | the immutable step ignored its parent (15); the link got the step's value (cb(12,)) | passes |
| a link_error on a middle step that fails, and one on the chain | both errbacks ran once each | passes |
