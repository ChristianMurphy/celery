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

## Findings

No store keeps a ready state other than SUCCESS (`finalStable`, on all
four, with or without REVOKED): a duplicate run's SUCCESS replaces FAILURE,
REVOKED replaces FAILURE, and SUCCESS or STARTED replaces REVOKED, which
workers.rst documents for a revoked task that was running. Elasticsearch
refuses only ready to unready. A conditional write that keeps every ready
state (`mongofinal_rev`) passes every property.


Read-then-write stores lose an observed SUCCESS (`rtw.successStable`); the
   race is known (celery discussion 10681).
