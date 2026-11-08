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
