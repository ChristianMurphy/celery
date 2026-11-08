======================================
 Chord harness: actual workers, faults
======================================

``driver.py`` starts actual workers, sends many chords, lets one fault
happen while they run, and then judges each chord from an audit log that
Celery doesn't control. It checks the chord guarantees under at least once
delivery:

* the body is dispatched once, even when a header task runs twice;
* the body starts only after every header result is stored, and gets each
  header result once, in order;
* the body runs, or the chord fails with an error, once every header task
  has a stored result.

It is not part of the unit or integration suites. It needs Docker for the
services it restarts.

Services
========

The driver uses fixed local ports. The services it restarts run in
containers that only this harness uses::

    docker run -d --name chords-broker-redis -p 16379:6379 redis:7 \
        redis-server --appendonly yes --appendfsync always
    docker run -d --name chords-backend-redis -p 16380:6379 redis:7 \
        redis-server --appendonly yes --appendfsync always
    docker run -d --name chords-audit -p 16381:6379 redis:7 \
        redis-server --appendonly yes --appendfsync always
    docker run -d --name chords-rabbit -p 5673:5672 rabbitmq:3.13-management
    docker run -d --name chords-pg -p 5434:5432 -e POSTGRES_PASSWORD=pw \
        -e POSTGRES_DB=celery postgres:16
    docker run -d --name chords-mongo -p 27018:27017 mongo:7

Memcached, Cassandra (keyspace ``celery``), CouchDB, ArangoDB and
Elasticsearch are reached on their usual ports (see
``t/conformance/backends/docker-compose.yml``); each run uses its own table,
database, collection or index, and the driver never restarts them.

The broker and the Redis containers use ``appendfsync always``, so a restart
doesn't lose messages and a lost chord is Celery's, not the service's.

Running
=======

One run::

    PYTHONPATH=. python -m t.conformance.chords.driver \
        --broker redis --backend postgresql --pool prefork \
        --fault kill_random --chords 60

Many runs, one configuration per line::

    OUT=/tmp/chords PY=python t/conformance/chords/matrix.sh < configs.txt

Options that matter most:

``--broker``
    ``redis``, ``rabbitmq`` (classic queue) or ``quorum`` (RabbitMQ quorum
    queue).
``--backend``
    ``redis``, ``postgresql``, ``sqlite``, ``mongodb``, ``memcached``,
    ``cassandra``, ``couchdb``, ``arangodb``, ``elasticsearch``.
``--pool``
    ``prefork``, ``solo`` or ``threads``. Two workers (``--workers``) with
    ``--concurrency`` 2 by default.
``--acks-late`` / ``--reject-on-lost``
    ``task_acks_late`` and ``task_reject_on_worker_lost``; both 1 by default,
    so a lost header task runs again instead of failing the chord.
``--visibility``
    The Redis broker's ``visibility_timeout``. The app also sets
    ``unacked_restore_interval=1`` and ``unacked_restore_throttle=1``, as
    kombu's docs suggest for a low visibility timeout.
``--sleep LOW HIGH``
    Each header task sleeps a random time in this range.
``--fast-redelivery 1``
    A header task sleeps only on its first delivery. With a short visibility
    timeout this gives one duplicate of each slow header task instead of an
    endless redelivery loop.

Faults
======

Only natural triggers. Nothing in Celery is patched and no delay is injected.

``none``
    No fault. With ``--broker redis`` and a ``--visibility`` shorter than
    some header tasks, the broker delivers those tasks a second time while
    the first run is still going (``acks_late``).
``kill_random``
    SIGKILL a whole worker (main process and pool) every ``--fault-every``
    seconds, then start a new one. Unacked messages are delivered again.
``kill_child``
    SIGKILL one prefork pool process. The worker stays up.
``kill_after_store``
    SIGKILL the worker as soon as the audit log shows a header task's
    result stored, aiming at the window between the store and the ack. The
    driver reads the audit log, so it lands a few milliseconds late at best.
``broker_restart`` / ``backend_restart``
    ``docker restart`` (or ``docker kill`` and ``start`` with
    ``--hard-restart 1``) the track's broker or result backend container.

Outcomes
========

Each chord gets one outcome:

``ok``
    The body started once, after every header result was stored.
``duplicate_body``
    The body started more than once from fresh messages (not broker
    redeliveries of one body message), or more than once with more than one
    publish.
``early_body``
    The body started while a header result wasn't ``SUCCESS`` in the backend,
    or got results other than ``[0, 1, ..., n-1]``.
``body_and_error``
    The body started, and the chord's error callback also ran.
``spurious_error``
    The chord failed although every header task ended as ``SUCCESS``.
``stuck``
    Every header task ended as ``SUCCESS``, the body was never published, and
    the chord never failed.
``body_not_run``
    The body was published but never started (a broker problem, or an
    overloaded run). Not counted as a chord violation.
``publish_error``
    The body couldn't be published within ``task_publish_retry_policy``
    while the broker was down, and the chord failed with ``Callback error``.
    Expected.
``chord_error``
    The chord failed and a header task ended as ``FAILURE``. Expected, for
    example after ``WorkerLostError`` without ``reject_on_worker_lost``.
``header_lost``
    A header task never stored a result, so the body can't run. Expected
    without ``acks_late``, or when the broker loses the message.

``duplicate_body``, ``early_body``, ``body_and_error``, ``spurious_error`` and
``stuck`` count as violations.

History format
==============

Each run writes ``<run>.jsonl`` with one JSON object per chord, and appends
one line to ``summary.jsonl``.

A chord record::

    {
      "run": "redis-memcached-prefork-none-1a2b3c",
      "chord": "<chord id, also passed to every task>",
      "group_id": "<header group id>",
      "header_ids": ["<task id of header 0>", ...],
      "body_id": "<task id of the body>",
      "events": [<event>, ...],            # sorted by ts
      "final": {"header_states": ["SUCCESS", ...], "body_state": "SUCCESS"},
      "verdict": {"outcome": "ok", "body_runs": 1, "body_fresh_runs": 1,
                  "body_publishes": 1, "errbacks": 0, "early": false,
                  "header_runs": {"0": 1, "1": 2, ...},
                  "duplicate_headers": 1}
    }

Every event has ``ev``, ``ts`` (Unix time, seconds), ``pid``, ``pgid`` (the
worker's process group, so the worker that ran it) and ``host``. Events by
``ev``:

``submit``
    Logged by the driver right after ``apply_async()``, so a header task
    can start before it. ``group``, ``header_ids``, ``body_id``.
``header_start``
    ``task_prerun`` of a header task: ``idx``, ``tid``, ``group``,
    ``redelivered`` (the broker's flag).
``header_return``
    The header task's body finished; its result isn't stored yet.
``header_stored``
    ``task_postrun``: the result was stored and ``on_chord_part_return()``
    ran. ``state`` is the state the worker stored.
``unlock_publish`` / ``unlock_start`` / ``unlock_done``
    ``celery.chord_unlock`` published (``retries``, ``eta``), started
    (``retries``, ``redelivered``) and finished (``state``). Only on backends
    that join chords with ``chord_unlock``.
``body_publish``
    ``before_task_publish`` for the body, from whichever process sends it.
``body_start``
    The body ran: ``results`` it got, ``header_states`` it read from the
    backend for each header id, ``redelivered``.
``errback``
    The body's error callback ran: ``exc``.

``summary.jsonl`` has one line per run: ``config`` (every driver option and
the broker and backend URLs), ``counts`` per outcome, ``violations``,
``fault_log`` (each kill or restart with its time) and ``logs`` (the worker
log directory).
