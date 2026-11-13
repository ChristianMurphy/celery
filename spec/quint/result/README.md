# Result state

`resultstate.qnt` (instances in the same file), celery 768e476.

## Model

Two duplicate runs of a task (up to two writes each: STARTED, then RETRY,
SUCCESS or FAILURE; in the `_rev` instances REVOKED too, which a discard
and the revoke command write through the same `store_result`), one
`forget()`, and a reader. `mongofinal_rev` is a design: a write that keeps
every ready state (a filter `status $nin READY_STATES`). A store is split where the
code splits it: `begin(r, s)` is the read a backend does before writing,
`commit(r)` is the write.

## Properties

<!-- generated: properties resultstate.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `successStable` | safety | Once a reader saw SUCCESS by a run, the store keeps that record until forget(). Assumes: two duplicate runs of one task id, one reader, one forget(). | Universality, after a reader saw SUCCESS until forget() | celery/backends/base.py:1362-1366 (768e476): "if the status is a success then we ignore any following update to the state"; celery PR 10920 (database), 10944 (mongodb), 10945 (cassandra) |
| `readySticky` | safety | A ready state is never replaced by an unready one, except by forget(). Assumes: two duplicate runs of one task id. | Absence of an unready write, after a ready state until forget() | celery/backends/elasticsearch.py:166-176 (768e476); only Elasticsearch promises it |
| `finalStable` | safety | A ready state (SUCCESS, FAILURE, REVOKED) is never replaced by another state, except by forget(): stored states move one way. Assumes: two duplicate runs of one task id; the _rev instances also write REVOKED (mark_as_revoked). | Absence (a ready state replaced by another state), after a ready state until forget() | celery/states.py (celery 768e476): READY_STATES are final; the item: one order of stored states including REVOKED |
| `progress` | liveness | steps + measure never exceeds its initial value, and measure is never negative: every run is finite. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the spec: every action lowers `measure` |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: every run wrote all it had to write. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Absence of a stop outside a quiet state (lemma 2 of 3) | the spec: `quiet`, checked by TLC with deadlock detection on `stepOrQuiet` |
| `settles` | liveness | A run that keeps going ends for good with every run having written all it had to. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes; weak fairness of `step` (the spec has no faults). | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress` and `stepOrQuiet`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `noSuccessEver` | witness | Some run stores SUCCESS. | Existence | sanity |
<!-- end generated -->

## Mapping to the code

| Spec action | Code |
|---|---|
| `begin` rtw/es | `_get_task_meta_for` in `BaseKeyValueStoreBackend._store_result`, base.py:1367 |
| `commit` rtw | SUCCESS check base.py:1369-1370, `_set_with_state` base.py:1373, Redis SETEX redis.py:816-825 |
| `commit` mongo | `replace_one({'_id', 'status': {'$ne': SUCCESS}}, upsert=True)`, mongodb.py:207-213 |
| `begin` / `commit` sql | unlocked `_query_task` database/__init__.py:174; `with_for_update` re-read :175-180; insert :181-185; SUCCESS check :186-190; unique task_id models.py:34 |
| `commit` es | create `op_type=create` elasticsearch.py:135-139; `_update` get, checks, `if_seq_no` :140-142, 172-221 |
| `forget` | base.py:883-885, 1352-1353; mongodb.py:297; database/__init__.py:352-353 |

Each action in the `.qnt` file carries the same citation in a comment.

## Results

`python3 tools/check.py --tier full --only results`. D is the longest run in
steps; a number after a property is the number of states in a shortest
counterexample; "reachable" lists witnesses, where failing means the state
can be reached. In every instance a run stops only in a quiet state.

<!-- generated: results resultstate.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| es | 11 | finalStable (5) | noSuccessEver (3) |
| es_rev | 11 | finalStable (5) | noSuccessEver (3) |
| mongo | 11 | readySticky (5), finalStable (5) | noSuccessEver (3) |
| mongo_rev | 11 | readySticky (5), finalStable (5) | noSuccessEver (3) |
| mongofinal_rev | 11 | none | noSuccessEver (3) |
| rtw | 11 | successStable (6), readySticky (5), finalStable (5) | noSuccessEver (3) |
| rtw_rev | 11 | successStable (6), readySticky (5), finalStable (5) | noSuccessEver (3) |
| sql | 11 | readySticky (5), finalStable (5) | noSuccessEver (3) |
| sql_rev | 11 | readySticky (5), finalStable (5) | noSuccessEver (3) |
<!-- end generated -->

Why each violation is expected:

<!-- generated: reasons resultstate.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `successStable` | rtw (6), rtw_rev (6) | open | tracker structural-first-success (reply on celery discussion 10681). read-then-write stores: the read and the write are two round trips |
| `readySticky` | rtw (5), rtw_rev (5) | open | tracker es-ready-states (merged into the reply on celery discussion 10681). a late RETRY replaces a final FAILURE |
| `readySticky` | mongo (5), mongo_rev (5) | open | tracker es-ready-states (merged into the reply on celery discussion 10681) |
| `readySticky` | sql (5), sql_rev (5) | open | tracker es-ready-states (merged into the reply on celery discussion 10681) |
| `finalStable` | es (5), es_rev (5), mongo (5), mongo_rev (5), rtw (5), rtw_rev (5), sql (5), sql_rev (5) | documented | docs/userguide/workers.rst (celery 768e476), revoke: a task marked REVOKED "then stores its result when it's done"; base.py:1361-1370 keeps SUCCESS only. a duplicate run or a late write replaces FAILURE or REVOKED with another ready state on every store; only SUCCESS is kept (and lost on rtw, successStable) |
<!-- end generated -->

Apalache 0.62.4, bounded. `steps <= 11` holding at depth 12 makes the checks
at depth 11 complete:

| instance | check | depth | result | time |
|---|---|---|---|---|
| rtw | steps <= 11 | 12 | holds | 75 s |
| rtw | successStable | 11 | violated | 4 s |
| rtw | readySticky | 11 | violated | 3 s |
| rtw | finalStable | 11 | violated | 13 s |
| mongo | steps <= 11 | 12 | holds | 73 s |
| mongo | successStable | 11 | holds | 22 s |
| mongo | readySticky | 11 | violated | 3 s |
| mongo | finalStable | 11 | violated | 12 s |
| sql | steps <= 11 | 12 | holds | 75 s |
| sql | successStable | 11 | holds | 27 s |
| sql | readySticky | 11 | violated | 3 s |
| sql | finalStable | 11 | violated | 13 s |
| es | steps <= 11 | 12 | holds | 84 s |
| es | successStable | 11 | holds | 27 s |
| es | readySticky | 11 | holds | 26 s |
| es | finalStable | 11 | violated | 10 s |

Apalache was run on the four instances without REVOKED; the `_rev`
instances are checked by TLC only, and no `_rev` trace was replayed on a
real backend.

## Readers: groups, group results and client reads

`reader.qnt`, `reader_instances.qnt`, celery 768e476 (the read path is the
same at 9c2f238). Two tasks of one group, each written STARTED and then
SUCCESS or FAILURE. Two client processes join the group: the one that
applied it (it saves the GroupResult and owns the RPC reply queue) and
another one (it restores the GroupResult by id). A reader waits in one of
three ways: `Poll` (`wait_for`, base.py:1123-1150: database, MongoDB,
memcached), `PubSub` (the Redis ResultConsumer: subscribe, read the key
once, then the messages each write publishes; after a lost connection it
reads every subscribed key with MGET and then subscribes again,
redis.py:245-271) and `Rpc` (each write is a message to the applying
client's reply queue). AsyncResult keeps the first ready state it reads
(result.py:463-475). Expiry is its own state of a key (`Expired`): it
reads as PENDING. Instances `<mode>_<fault class>`: `late` (one more write
after the final one: a RETRY, as in finding retry-over-failure, or a
duplicate run's other final state), `expire` (result_expires of a task key
or of the group key), `cut` (a reader's pub/sub connection is lost),
`error` (a save_group, restore_group or read raises in the client).
Designs: `keepready` (no store replaces a ready state), `subfirst`
(subscribe again before the MGET). `ids_` instances build AsyncResults
from the ids instead of a saved group.

<!-- generated: properties reader.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `readersAgree` | safety | Two readers that have a ready answer for a task have the same one. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Absence (two readers with different ready answers for one task), global | docs/userguide/tasks.rst (celery 768e476), States: SUCCESS and FAILURE are ready states; result.py:463-470 caches the first one read |
| `answerWasWritten` | safety | Nothing from nothing: every ready answer a reader has was stored by a write for that task. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Absence (an answer no write stored), global | celery/result.py:463-475 (celery 768e476): the answer is what get_task_meta returned |
| `restoreFinds` | safety | GroupResult.restore(id) after a GroupResult.save() that returned finds the group. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Absence (a restore that finds nothing after a save that returned), global | celery/result.py:979-987 (celery 768e476): save the group-result "for later retrieval using restore" |
| `quietDone` | liveness | In a quiet state every reader has ended: it has every answer, or its save, restore or read raised, or the restore found nothing. A join that waits there waits for ever. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Response (lemma 3 of 3: a quiet reader has ended) | celery/result.py:759-842 (celery 768e476), ResultSet.join: "Gather the results of all tasks" |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run in which the code keeps working ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only; weak fairness of `work` (every action that uses no fault budget), none for the faults. | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Existence | sanity |
| `neverAnswered` | witness | A reader gets every answer of the group. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Existence | sanity |
| `neverFailed` | witness | A task stores FAILURE. Assumes: two tasks of one group, each written STARTED then SUCCESS or FAILURE; two client processes, the one that applied the group (it saves the GroupResult, and owns the RPC reply queue) and another one (it restores the GroupResult by id); each joins the group; a reader waits by polling (base.py wait_for), by Redis pub/sub (the ResultConsumer) or by rpc://; AsyncResult keeps the first ready state it reads; a key-value store refuses to replace SUCCESS only. | Existence | sanity |
<!-- end generated -->

<!-- generated: results reader.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| ids_pubsub_cut | 25 | quietDone (17) | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| keepready_late | 15 | none | neverQuiet (11), neverAnswered (8), neverFailed (3) |
| keepready_pubsub_late | 23 | none | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| poll_error | 14 | none | neverQuiet (7), neverAnswered (8), neverFailed (3) |
| poll_expire | 15 | restoreFinds (4), quietDone (10) | neverQuiet (10), neverAnswered (8), neverFailed (3) |
| poll_late | 15 | readersAgree (8), quietDone (12) | neverQuiet (11), neverAnswered (8), neverFailed (3) |
| poll_none | 14 | none | neverQuiet (11), neverAnswered (8), neverFailed (3) |
| pubsub_cut | 25 | quietDone (17) | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| pubsub_error | 22 | none | neverQuiet (7), neverAnswered (10), neverFailed (3) |
| pubsub_expire | 23 | restoreFinds (4), quietDone (16) | neverQuiet (12), neverAnswered (10), neverFailed (3) |
| pubsub_late | 23 | readersAgree (10), quietDone (16) | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| pubsub_none | 22 | none | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| rpc_late | 12 | quietDone (11) | neverQuiet (11), neverAnswered (10), neverFailed (3) |
| rpc_none | 10 | quietDone (11) | neverQuiet (11), neverAnswered (10), neverFailed (3) |
| subfirst_cut | 25 | none | neverQuiet (15), neverAnswered (10), neverFailed (3) |
| subfirst_cut2 | 28 | none | neverQuiet (15), neverAnswered (10), neverFailed (3) |
<!-- end generated -->

What the model says:

* After a lost pub/sub connection the reconnect reads the key and only then
  subscribes on a new connection. A final write between the two publishes
  a message that reaches nobody, and the read saw the old state: the
  reader waits for ever (`pubsub_cut`, `ids_pubsub_cut`). On the real code
  4 of 4 with a slow link and the release timed to the MGET; 0 of 200 with
  random kills on localhost
  ([pubsub-reconnect-gap](../findings/pubsub-reconnect-gap/)).
  Subscribing first (`subfirst_cut`, `subfirst_cut2`) passes every
  property.
* A late write that replaces FAILURE makes two readers disagree: the one
  that read first keeps its answer (`poll_late`, `pubsub_late`). A late
  RETRY leaves a later reader waiting for ever. A store that keeps every
  ready state (`keepready_*`) passes.
* An expired key reads PENDING, so a reader that comes after the expiry
  waits for ever, and a restore of an expired group returns None
  (documented, result_expires). With `rpc://` the second process never gets
  an answer (documented, tasks.rst:1293-1296).
* A backend error in save_group, restore_group or a read raises to the
  caller: visible, and no property fails (`*_error`).

Why each violation is expected:

<!-- generated: reasons reader.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `readersAgree` | poll_late (8) | open | the same class as finding retry-over-failure and tracker es-ready-states: a key-value store replaces a ready state other than SUCCESS (base.py:1369, celery 768e476), and AsyncResult keeps the first ready state it read (result.py:463-470). a late write replaces FAILURE with SUCCESS (a duplicate run): the reader that read first keeps FAILURE, the other reads SUCCESS |
| `readersAgree` | pubsub_late (10) | open | the same class as finding retry-over-failure and tracker es-ready-states: a key-value store replaces a ready state other than SUCCESS (base.py:1369, celery 768e476), and AsyncResult keeps the first ready state it read (result.py:463-470). the same with pub/sub: the first message is cached, a later reader reads the key |
| `restoreFinds` | poll_expire (4), pubsub_expire (4) | documented | docs/userguide/configuration.rst:1070-1086 (celery 768e476), result_expires: stored results are deleted after this time; the group key is set with it (base.py:1379-1382). the group key expired before the restore: restore_group returns None |
| `quietDone` | poll_expire (10), pubsub_expire (16) | documented | docs/userguide/configuration.rst:1070-1086 (celery 768e476), result_expires; docs/userguide/tasks.rst:1335-1336: any task id that is not known is PENDING. the task key expired before the reader read it: it reads PENDING for ever |
| `quietDone` | rpc_late (11), rpc_none (11) | documented | docs/userguide/tasks.rst:1293-1296 (celery 768e476): with rpc:// a result can only be retrieved by the client that initiated the task; two processes cannot wait for the same result. the second reader never gets a message |
| `quietDone` | poll_late (12), pubsub_late (16) | open | finding retry-over-failure (a late RETRY replaces a final state; tracker es-ready-states). the late write is RETRY: a reader that reads after it waits for ever |
| `quietDone` | ids_pubsub_cut (17), pubsub_cut (17) | open | finding pubsub-reconnect-gap (not in the tracker yet; the MGET before the SUBSCRIBE came with celery PR 5921, 2020; closest symptom celery issue 4039). the reader's pub/sub connection is lost; the reconnect reads the key (MGET), the final write and its message come next, and only then the new connection subscribes: the message reached nobody and get() waits for ever |
<!-- end generated -->

## Housekeeping: forget, ignored results, saved groups

`housekeeping.qnt`, `housekeeping_instances.qnt` (10 instances; celery
768e476, the same at 9c2f238): one task id with at most one retry;
ignore_result and store_errors_even_if_ignored; a client that forgets the
task at any time, or saves a group of it, forgets or deletes the group and
restores it. Instances `<settings>_<ending or client op>`.

<!-- generated: properties housekeeping.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `finalNotUnready` | safety | Once the task finished, what is stored is a final state or nothing, never RETRY. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Absence (RETRY stored after the task finished), after the last run | docs/userguide/tasks.rst:1162-1165 and configuration.rst:536-550 (celery 768e476): with ignore_result the return value is not stored; store_errors_even_if_ignored stores the errors as well |
| `forgetSticks` | safety | A forgotten result stays forgotten. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Absence (a state stored after forget), after forget | celery/result.py:136-143 (celery 768e476): AsyncResult.forget, "Forget the result of this task and its parents" |
| `restoredReady` | safety | A group restored after its tasks finished is ready: every task has a final state stored. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Absence (a restored group that is not ready after its tasks finished), global | celery/result.py:979-987 and 1036-1042 (celery 768e476): save a group result for later retrieval with restore |
| `quietDone` | liveness | At the end a task whose result is kept, and that no client removed, reads SUCCESS or FAILURE. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Response (lemma 3 of 3: at the end a kept result reads its final state) | docs/userguide/tasks.rst (celery 768e476), Result backends |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run that keeps going ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it; weak fairness of `step` (the spec has no fault budgets). | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Existence | sanity |
| `neverStoredRetry` | witness | RETRY is stored. Assumes: one task id with at most one retry (Task.retry, countdown 0); ignore_result and store_errors_even_if_ignored as the instance says; one client that may forget the task at any time, or save a group of that task once it finished, then forget or delete the group, then restore it. | Existence | sanity |
<!-- end generated -->

<!-- generated: results housekeeping.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| forget_running | 3 | forgetSticks (4) | neverQuiet (4) |
| forget_running_ignore | 3 | none | neverQuiet (4) |
| group_delete | 6 | none | neverQuiet (6) |
| group_forget | 5 | restoredReady (6) | neverQuiet (6) |
| ignore_retry | 4 | none | neverQuiet (5) |
| ignore_storeerr_raises | 2 | none | neverQuiet (3) |
| ignore_storeerr_retry | 4 | finalNotUnready (5) | neverQuiet (5), neverStoredRetry (3) |
| plain_raises | 2 | none | neverQuiet (3) |
| plain_retry | 4 | none | neverQuiet (5), neverStoredRetry (3) |
| plain_returns | 2 | none | neverQuiet (3) |
<!-- end generated -->

<!-- generated: reasons housekeeping.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `finalNotUnready` | ignore_storeerr_retry (5) | open | finding ignored-retry-stays-retry (closest celery issue 11008, closed by celery PR 11009, which keeps RETRY stored under store_errors_even_if_ignored for a retry after a lost worker). RETRY is stored as an error, the successful retry stores nothing: RETRY for ever |
| `forgetSticks` | forget_running (4) | by design | celery/result.py:136-143 and backends/base.py:883-885 (celery 768e476): forget deletes what is stored now and records nothing, so a task still running stores its final state afterwards (3 of 3 on 9c2f238, findings/ignored-retry-stays-retry/confirm_ignored_retry.py forget) |
| `restoredReady` | group_forget (6) | documented | celery/result.py:712-715 (celery 768e476): GroupResult.forget forgets all the tasks, not the saved group; restore then returns a group whose tasks read PENDING, so ready() is false and get() waits (3 of 3 on 9c2f238, findings/ignored-retry-stays-retry/confirm_ignored_retry.py groupforget) |
<!-- end generated -->

What the model says, and what the real code did (9c2f238, kombu 445027e,
Redis 7, `../findings/ignored-retry-stays-retry/confirm_ignored_retry.py`,
nothing injected):

* ignore_result with store_errors_even_if_ignored: the retry stores RETRY
  as an error and the successful attempt stores nothing, so the task reads
  RETRY for ever (`ignore_storeerr_retry.finalNotUnready`, 3 of 3; without
  store_errors_even_if_ignored it reads PENDING, 3 of 3). See
  [ignored-retry-stays-retry](../findings/ignored-retry-stays-retry/).
* forget() while the task runs: the final write puts the result back
  (`forget_running.forgetSticks`, 3 of 3); forget records nothing.
* GroupResult.forget() forgets the tasks and leaves the saved group:
  restore() returns a group that is never ready (`group_forget`, 3 of 3);
  delete() and restore() give None (`group_delete` passes).
* Seen on the way: with the result kept and -c 2, one of three plain
  retries read RETRY after its SUCCESS, which is finding retry-over-failure
  happening without the injected delay.

## Findings

No store keeps a ready state other than SUCCESS (`finalStable`, on all
four, with or without REVOKED): a duplicate run's SUCCESS replaces FAILURE,
REVOKED replaces FAILURE, and SUCCESS or STARTED replaces REVOKED, which
workers.rst documents for a revoked task that was running. Elasticsearch
refuses only ready to unready. A conditional write that keeps every ready
state (`mongofinal_rev`) passes every property.


Read-then-write stores lose an observed SUCCESS (`rtw.successStable`); the
   race is known (celery discussion 10681).
