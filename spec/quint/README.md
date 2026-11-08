# Quint specs: result state and chord completion

Two Quint specs of what Celery's result backends do, tied to the code at
768e476 (celery 5.7.0) in both directions: traces generated from the spec are
replayed against the real backends, and histories recorded from real runs
(threads, and a real worker) are checked against the spec.

| File | What |
|---|---|
| `resultstate.qnt` | Result state of one task id; four store implementations (`rtw`, `mongo`, `sql`, `es`) |
| `chord.qnt` | Chord completion; three protocols (`redis`, `kv`, `unlock`) and one instance per fault class |
| `scenarios.qnt` | The behaviors behind the findings below, as `quint test` runs |
| `check.py` | Simulates every instance against every property and prints the matrix |
| `mutate_spec.py` | Spec mutants: each must make a property fail |
| `adapter/replay_results.py`, `adapter/replay_chord.py` | Spec to code: replay ITF traces on Redis, MongoDB, PostgreSQL, Elasticsearch, memcached |
| `adapter/record_results.py`, `adapter/check_histories.py` | Code to spec: record concurrent store histories, check them with `quint test` |
| `adapter/worker_app.py`, `adapter/record_chords.py` | Code to spec: record chords run by a real worker, check them with `quint test` |
| `adapter/mutations.py` | Code mutants: each must make a replay mismatch |
| `run_all.sh` | Everything above, in order |

## How to run

```sh
npm i -g @informalsystems/quint          # 0.33.1; Java 17+ for `quint verify`
cd spec/quint
python3 check.py                         # property matrix (simulator)
python3 check.py --only results --verify # plus Apalache, bounded
quint test --main=redisLost scenarios.qnt
PYTHONPATH=$(git rev-parse --show-toplevel):$PWD ./run_all.sh   # needs the services
```

`quint run`'s default Rust backend downloads its evaluator through the GitHub
API, which this sandbox refuses; the evaluator was fetched from the release
page into `~/.quint/rust-evaluator-v0.7.1/` instead (`--backend=typescript`
also works, slower). `quint verify` downloaded Apalache 0.62.4 and works.

## Model

### Result state (`resultstate.qnt`)

Two duplicate runs of a task (`RUNS = {1, 2}`, up to two writes each:
STARTED, then RETRY, SUCCESS or FAILURE), one `forget()`, and a reader. A store
is split where the code splits it: `begin(r, s)` is the read a backend does
before writing, `commit(r)` is the write.

Properties: `successStable` (once a reader saw SUCCESS v, the store keeps
SUCCESS v until `forget()`) and `readySticky` (a ready state is never replaced
by an unready one; the parameter `READY_STICKY`).

### Chords (`chord.qnt`)

A chord with two header tasks and three worker processes. Header messages can
be delivered again (`dup`), a whole worker can be lost (`loseWorker`: unacked
messages go back to the queue), a pool child can be lost with the main process
handling `WorkerLostError` (`loseChild`, `request.py:709-790`), the broker can
refuse a publish, and the Redis claim and done marker expire. With
`LEASE_SAFE` a claim expires only once its holder is gone; without it, a holder
can stall past its lease (the trade-off stated at `redis.py:953-961`).

Properties: `atMostOnce`, `noPremature` (body only after every header result is
stored), `argsComplete` (one result per header), `noErrorAfterDispatch`
(`chord_error_from_stack` not called after the body was sent),
`noSpuriousError` (the chord fails only for a header FAILURE or a refused
publish), `quietDone` (bounded liveness: once nothing can run, the body was
sent or the chord failed). Fault budgets are finite, so every run ends quiet.

## Mapping to the code (768e476)

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
| `count` | restore + `incr` base.py:1438-1466 |
| `kvJoin` / `kvClean` | `join_native` base.py:1467-1483; `deps.delete()`, `delete(key)` 1493-1495 |
| `unlockPoll` | `chord_unlock` builtins.py:50-130 (retry refusal: celery PR 10959) |
| `loseChild` | `Request.on_failure` request.py:709-790 (SUCCESS stored: celery PR 10752) |
| `claimExpires` / `doneExpires` | claim TTL redis.py:963, 1018-1020; done marker redis.py:966 |

Each action in the `.qnt` files carries the same citation in a comment.

## Results

### Properties (simulator, 2000 samples x 40 steps per cell; result state also Apalache to 12 steps)

resultstate.qnt
| instance | successStable | readySticky |
|---|---|---|
| rtw | VIOLATED | VIOLATED |
| mongo | ok | VIOLATED |
| sql | ok | VIOLATED |
| es | ok | ok |

chord.qnt
| instance | atMostOnce | noPremature | argsComplete | noErrorAfterDispatch | noSpuriousError | quietDone | neverDispatched |
|---|---|---|---|---|---|---|---|
| redis_none | ok | ok | ok | ok | ok | ok | VIOLATED |
| redis_dup | ok | ok | ok | ok | ok | ok | VIOLATED |
| redis_refuse | ok | ok | ok | ok | ok | ok | VIOLATED |
| redis_wlost_late | VIOLATED | ok | ok | ok | ok | ok | VIOLATED |
| redis_wlost_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| redis_child_late | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| redis_child_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| redis_child_reject | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| redis_lease | VIOLATED | ok | ok | ok | ok | ok | VIOLATED |
| kv_none | ok | ok | ok | ok | ok | ok | VIOLATED |
| kv_dup | ok | ok | ok | VIOLATED | VIOLATED | ok | VIOLATED |
| kv_refuse | ok | ok | ok | ok | ok | ok | VIOLATED |
| kv_wlost_late | ok | ok | ok | VIOLATED | VIOLATED | VIOLATED | VIOLATED |
| kv_wlost_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| kv_child_late | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| kv_child_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| kv_child_reject | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| unlock_none | ok | ok | ok | ok | ok | ok | VIOLATED |
| unlock_dup | ok | ok | ok | ok | ok | ok | VIOLATED |
| unlock_refuse | ok | ok | ok | ok | ok | ok | VIOLATED |
| unlock_wlost_late | VIOLATED | ok | ok | ok | ok | ok | VIOLATED |
| unlock_wlost_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| unlock_child_late | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| unlock_child_early | ok | ok | ok | ok | ok | VIOLATED | VIOLATED |
| unlock_child_reject | VIOLATED | ok | ok | ok | ok | ok | VIOLATED |

`neverDispatched` is a witness: violated means the instance does dispatch the
body. Simulation is not exhaustive: `redis_wlost_late.quietDone` shows `ok`
but `scenarios.qnt` (`lostClaimHangsTest`) is a behavior that violates it.

### Spec to code (replay)

All generated traces replay on the real backends with every state equal:
Redis (41 traces incl. the `successStable` counterexample), MongoDB 40,
PostgreSQL 40 (18 commits that the spec says raise did raise
`IntegrityError`), Elasticsearch 40; chords: 340 random traces over 17 Redis
and memcached instances, every counterexample in `traces/cex/`, and every
scenario in `traces/scenarios/`. The chord replay runs the real
`mark_as_done` and the real `Request.on_failure(WorkerLostError)` in threads,
parked at the spec's split points; a lost worker is a thread never released.

### Code to spec (recorded histories)

* `histories/{redis,mongo,es}.jsonl`: 200 concurrent histories each (two
  writer threads, a reader, sometimes a forget, natural timing; every backend
  call logged under one lock). All 600 are behaviors the spec allows. In 7 of
  200 Redis histories a reader saw SUCCESS and it was then replaced; 0 of 200
  for MongoDB and Elasticsearch. Checked against the `mongo` model instead,
  the Redis histories fail, as they should.
* `histories/chords_{redis,kv}.jsonl`: a real prefork worker (`-c 2`, Redis
  broker, `acks_late`, `visibility_timeout=2`, headers sleeping past it so the
  broker redelivers them). Redis backend: 12 chords, 8 redelivered header runs,
  12 of 12 allowed. Memcached backend: 11 chords, 23 redelivered runs, 11 of 11
  allowed; 6 of 11 end with `errorAfterDispatch` (finding 2).

### Mutations

Spec (`mutate_spec.py`): 8 of 8 mutants caught (mongo without `$ne`, sql
without the locked re-read, es without the ready check, redis without dedupe,
claim without NX, kv count on `>=`, chord_unlock retry refusal not failing the
chord, lost child counted again). Two more are equivalent, as expected: the
owner checks in `CHORD_CLAIM_EXTEND_LUA` and `CHORD_CLEANUP_LUA` only matter
when a lease can expire under a live holder, where `atMostOnce` fails anyway.

Code (`adapter/mutations.py`), traces that mismatched:

| Mutant | Replay | Caught |
|---|---|---|
| `kv_no_success_check` (base.py:1369) | rtw on Redis | 17 of 41 |
| `mongo_no_filter` (mongodb.py:207) | mongo | 22 of 40 |
| `sql_no_success_check` (database/__init__.py:186) | sql on Postgres | 15 of 40 |
| `sql_no_row_lock` (database/__init__.py:180) | sql on Postgres | 9 of 40 |
| `redis_lua_no_dedupe` (redis.py:111) | redis_dup | 20 of 20 |
| `redis_lua_claim_no_nx` (redis.py:139) | redis_dup | 15 of 20 |
| `redis_lua_no_done_marker` (redis.py:104) | redis_dup | 1 of 20 |
| `redis_cleanup_any_owner` (redis.py:160) | redis_lease | 19 of 20 |
| `redis_extend_any_owner` (redis.py:176) | redis_lease | 20 of 20 |
| `kv_count_early` (base.py:1465) | kv_dup | 20 of 20 |
| `kv_keep_counter` (base.py:1495) | kv_dup | 20 of 20 |
| `worker_lost_ignores_success` (request.py:714) | redis_child_late | 7 of 20 |

`val >= size` at base.py:1465 is an equivalent mutant (`val > size` is tested
first) and was replaced by `kv_count_early`.

## Findings (spec vs. code)

Every finding below is a behavior the spec allows, replayed on the real
backend with the real code (no code changed; only the order of steps and
SIGKILL-like stops are controlled). Natural frequencies are given where
measured; the rest are questions until reproduced with a natural trigger.

1. Read-then-write stores lose an observed SUCCESS (`rtw.successStable`).
   Known (harness `RACE`, draft "Keeping the first SUCCESS isn't atomic").
   The spec agrees with the harness: MongoDB, PostgreSQL and Elasticsearch
   hold; the code comment at base.py:1360-1366 promises more than
   `_store_result` does.
2. Incr backends (memcached, DynamoDB, GCS) fail a finished chord when a
   header runs again (`kv_*.noErrorAfterDispatch`). A redelivered header that
   runs after the chord finished finds the GroupResult deleted
   (base.py:1446-1455, deleted at 1494) and calls `chord_error_from_stack`,
   which stores FAILURE for the body id and calls the body's errbacks after the
   body was sent. Redis has a done marker for this (celery PR 10998); the incr
   path has none. Real worker on unmodified main (no backend hooks,
   `QUINT_NOHOOK=1 python -m adapter.record_chords --proto kv --chords 20
   --seed 3`): prefork -c 2, Redis broker with `visibility_timeout=2`,
   acks_late, headers sleeping past it. Memcached backend: the body ran for 20
   of 20 chords and its errback ran for 16 of 20 (every chord with a
   redelivered header), about 2.5 s after the body. Redis backend, same run:
   errback 0 of 20 (16 with a redelivered header).
3. Incr backends count a header twice (`kv_dup.noSpuriousError`): two runs of
   one header reach the chord size while another header still runs; the join
   then waits `result_chord_join_timeout` and fails the chord. This is the
   incr-backend form of celery issue 10910 (fixed for Redis by celery PR
   10919). Shown by replay; not seen naturally in 11 chords (the redelivered
   runs finished after the other header).
4. A pool child lost between storing SUCCESS and the chord part return leaves
   the chord pending forever (`redis_child_*.quietDone`, `kv_child_*`). Since
   celery PR 10752 the main process sees SUCCESS stored and acks without a part
   return; before it, `mark_as_failure` ran the part return and the chord
   finished (with an error). The same holds when the child dies after taking
   the Redis dispatch claim. Question: natural frequency not measured (OOM or
   SIGKILL in a window of one round trip).
5. Redis: a claim held by a lost worker blocks a prompt redelivery
   (`lostClaimHangsTest`). The worker that took the dispatch claim is lost
   before publishing; with RabbitMQ the header is redelivered at once, its
   part return finds the claim still held (TTL at least 10 s) and returns; when
   the claim expires nothing runs again. The recovery the code describes
   ("Keep the initial claim tied to the join timeout for crash recovery",
   redis.py:952) needs the redelivery to come after the TTL. Question.
6. chord_unlock: a lost pool child loses the chord (`unlock_child_*`).
   chord_unlock has `ignore_result`, so the main process acks the message and
   marks it failed; no retry is published and nothing polls the chord again.
   `reject_on_worker_lost=True` avoids it. Question.
7. At-most-once is conditional (expected): a worker lost after publishing
   and before cleanup, redelivered after the claim TTL (Redis broker:
   visibility timeout 1 h) sends the body again (`redis_wlost_late`,
   `lostAfterPublishDispatchesTwiceTest`); chord_unlock with acks_late does the
   same, and with `reject_on_worker_lost` a child lost after `self.retry()`
   published the next chord_unlock but before the ack requeues the current
   one, so two pollers run from then on and both send the body
   (`unlock_child_reject.atMostOnce`); a lease that expires under a live holder (`redis_lease`) is the
   trade-off the code documents.

Side observation, outside the spec: with `database_table_names`, the
`date_done` index keeps the default name `ix_celery_taskmeta_date_done`, so two
apps with different table names in one PostgreSQL schema can't both create
their tables (`DuplicateTable`).

## Where this stopped

Result state and chords are covered. Acks and redelivery are modeled only as
far as chords need them (early vs. late ack, requeue on worker loss,
`reject_on_worker_lost`, broker duplicates); there is no separate spec of
kombu's ack/redelivery. The chord_unlock protocol is checked by the simulator
and by the scenarios, but not replayed against the database backend.
