# Stress reproductions of the chord and result-state findings

Drivers that reproduce findings from the Quint track (`verify/quint-result-chords`)
with an unmodified worker on main (768e476), natural faults and production-like
conditions only. Nothing in Celery is patched, wrapped or delayed; tasks write
their own records to a Redis list (DB 29) the way a user's task would, and
everything else is read from outside: Redis keyspace notifications on our own
server, RabbitMQ, the worker log, the stored states.

| Script | Finding (tracker id) |
| --- | --- |
| `incr_double_count.py` | chord-incr-double-count |
| `lost_child.py` | chord-lost-child-after-success |
| `claim_redelivery.py` | chord-claim-blocks-redelivery |
| `unlock_lost.py` | chord-unlock-lost-child |
| `retry_latency.py` | es-ready-states (stale RETRY replacing FAILURE) |
| `quint_check.py` | checks 2-header incr histories against `spec/chord.qnt` |
| `repro/` | the minimal scripts used in the drafts |

## Services (our own containers)

```sh
docker run -d --name stress-redis -p 16579:6379 mirror.gcr.io/library/redis:7 \
    redis-server --databases 32 --save "" --appendonly no
docker run -d --name stress-memcached -p 11311:11211 mirror.gcr.io/library/memcached:1.6
docker run -d --name stress-rabbit -p 5682:5672 mirror.gcr.io/library/rabbitmq:3.13-management
docker run -d --name stress-pg -p 5442:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=stress \
    mirror.gcr.io/library/postgres:16
```

Retry latency runs also use the shared Toxiproxy (API 8474) with a proxy of
their own, `stress-backend` on 26579.

Each experiment has its own Redis broker DB. That matters: the Redis
transport keeps unacked messages in one hash per DB, and every worker's
restore pass restores any entry older than *its own* `visibility_timeout`.
A worker with `visibility_timeout=2` on the same DB redelivered another run's
messages after 2 s; the first lost-child run was discarded for that reason.

## Run

```sh
cd t/conformance/stress
export PYTHONPATH=/path/to/celery:.      # prints celery.__file__ at start
python incr_double_count.py shape --backend memcached --chords 10
python lost_child.py targeted --at meta --chords 20
python claim_redelivery.py targeted --at claim --chords 20
python unlock_lost.py random --chords 40 --headers 200 --base 0.05 --kill-every 1
python retry_latency.py --latency 0 --tasks 3000
python quint_check.py results/<incr run>.json
```

Each run writes `results/<run>.json` (summary, per-chord outcome, the task
records) and the worker log. Rates are N of M with a 95% Wilson interval;
the load average is recorded before and after (the machine is shared).

## Conditions used, and what they stand for

- `visibility_timeout=2` s on the Redis broker stands for the default 1 h,
  with header tasks of 3 s and 12 s standing for 1.5 h and 6 h tasks.
  `result_chord_join_timeout` stays at its default 3 s.
- SIGKILL of a pool child: OOM killer, segfault. SIGKILL of a whole worker
  process group: node loss, deploy. A container memory limit (`docker run
  --memory`) stands for a Kubernetes pod limit.
- Remote result backend RTT: a Toxiproxy latency toxic on the backend
  connection only, broker local.
- Targeted kills are timed from outside: the Redis keyspace notification for
  the header's `celery-task-meta-<id>` key (its SUCCESS write) or the
  chord's `<gid>.c` dispatch claim, or the worker's own `TaskPool: Apply`
  debug line for chord_unlock. The kill lands 0.01 to 0.05 ms after the
  notification is read.

## Results on main 768e476 (2026-11-08)

`summaries.jsonl` has the summary of every run kept (rates with Wilson
intervals, windows, load). Load average was 3 to 10 on 4 shared cores.

| Finding | Targeted (natural fault, timed from outside) | Random / natural | Controls | Window |
| --- | --- | --- | --- | --- |
| incr double count (memcached) | headers 3 s and 12 s, vt 2 s: 20 of 20 chords fail with the join timeout; minimal script 3 of 3 | 3 headers of 0.1 to 12 s: 6 of 30 | Redis backend 0 of 10; vt 60 s 0 of 10 | deterministic once a header outlives vt while another runs 3 s past the double count |
| lost child after SUCCESS (Redis) | kill at SUCCESS write 20 of 20 pending (early ack 10 of 10); at claim 19 of 20; hard time limit at 0.99 s of 1 s: 24 of 30 | child killed about every 2 s, 50 x 20 KB headers: 4 of 300 + 6 of 300 (minimal); OOM in 400 MiB: 5 of 5 | kill while running 0 of 20; 2 GiB 0 of 5 | SUCCESS write to cleanup p50 5.4 ms (4 headers), 41 ms (50 x 20 KB); predicted 1.1%, seen 1.7% |
| claim held by a lost worker (Redis + RabbitMQ) | kill worker at claim: 20 of 20 pending; at SUCCESS write: 6 of 20 (the 6 had the claim) | worker killed every 5 s on average: 0 of 200 | Redis broker, vt 10 s: 0 of 10 | claim held p50 22 ms, p90 45 ms (50 x 20 KB); redelivery 5 to 27 ms after the claim |
| chord_unlock lost (PostgreSQL) | kill on `TaskPool: Apply`: early ack 10 of 10, acks_late 5 of 10, + reject_on_worker_lost 0 of 10 (1 body twice) | child killed every 1 s, 200 headers: early ack 7 of 40 + 6 of 40 (minimal), acks_late 6 of 40, + reject 0 of 40 | no kill 0 of 15 | poll p50 54 to 137 ms, p90 300 ms (200 headers) |
| stale RETRY over FAILURE (Redis) | n/a | retry(countdown=0) 8 of 5,500; autoretry + retry_backoff 10 of 6,000 | Elasticsearch 0 of 3,000; 2/5/20 ms RTT: 1 of 500, 11 of 3,500, 8 of 3,500 | publish to RETRY write p50 9.4 ms, p99 36 ms; next attempt's FAILURE p1 35 ms, p50 126 ms |

Corrections to the plan:

- Stale RETRY: backend latency doesn't open the window. Both the RETRY and
  the FAILURE write pay the same round trips; the window is the retrying
  child's own time from publish to write, stretched by CPU contention, and
  the race happens with no added latency (8 of 5,500).
- Hard time limits don't go through the lost-child path (`Request.on_timeout`
  runs `mark_as_failure` in the main process), but they lose the chord
  through the Redis claim when the limit lands after the claim.
- Random whole-worker kills can't show the claim finding in a few hundred
  chords at a realistic kill rate (expected about 0.2% per chord); the
  targeted runs and the measured window carry it.
- Short visibility timeouts on one Redis DB affect every worker on it (see
  above), so each experiment needs its own DB.

Side observation, not investigated: with the memcached backend and pool
children SIGKILLed about every 2 s, replacement children raised
`MemcacheUnknownError(b'TOUCHED')` and `(b'STORED')`, which looks like a
pymemcache connection shared across fork; 65 of 300 chords failed that way.
