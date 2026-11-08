=======================================
 Duplicate runs with actual workers
=======================================

``../backends/test_store_concurrent.py`` shows that a late write can replace
a stored ``SUCCESS`` when Toxiproxy delays one writer. These scripts check how
often that happens without delays or patches: actual workers, an actual
broker, and the ways a task id runs twice in production.

``natural_app.py``
    The app the workers run, configured from ``NATURAL_*`` variables. Its
    ``natural.work(duration)`` task sleeps and returns a token unique to the
    run. Each run logs ``start``, ``end`` and ``post`` events to a Redis list;
    the ``post`` event includes what the run read back from the backend right
    after storing its result.

``run.py``
    Starts the workers, publishes the tasks, watches the backend, waits until
    everything is done, and prints one JSON line with the counts.

Scenarios
=========

``--scenario visibility``
    Redis broker, ``task_acks_late=True``, and a ``visibility_timeout``
    (``--visibility``, seconds) shorter than some runtimes (``--min`` to
    ``--max``). The broker restores the message while the first run is still
    going, and another worker process runs it too.

``--scenario eta``
    Redis broker, the default early ack, and a ``--countdown`` longer than the
    ``visibility_timeout``. A worker holds an ETA message unacked until the
    ETA, so the broker restores it, and every worker process that got a copy
    runs it at the same ETA.

``--scenario same-id``
    Each task id is published twice, back to back. A client sends this when a
    publish is retried after its reply was lost (kombu's ``ensure()`` and
    redis-py both retry), or when it reuses task ids.

``--scenario time-limit``
    ``task_time_limit`` (``--time-limit``) inside the runtime range. The
    worker's main process stores ``FAILURE`` when the limit fires, while the
    pool process may be storing ``SUCCESS``.

``--track-started`` sets ``task_track_started=True``. ``--claim`` makes the
run that loses a Redis ``SET NX`` on the task id raise, like a task whose
duplicate hits a unique key.

What it counts
==============

``duplicated``
    Task ids that ran more than once.
``success_changed_pubsub`` (Redis only)
    The backend published ``SUCCESS`` and then another state or value for the
    same task. This is exact: it is the order in which Redis applied the writes.
``success_changed_read``
    A read (the poller, a run's own read-back, or the final read) saw
    ``SUCCESS``, and a read that started after it finished saw something else.
``replaced_by_later_run``
    The final ``SUCCESS`` came from a run that stored it more than 50 ms after
    another run stored its ``SUCCESS``.
``final_not_success``, ``stuck``
    The final state isn't ``SUCCESS``, or isn't a ready state.
``near_misses``
    For duplicated ids, how many had one run's ``SUCCESS`` write within 1 ms,
    5 ms, ... of another run's write. It shows how often runs come close
    enough to race.
``time_limit_fired``, ``failure_handlers_after_success``
    The hard time limit fired, and of those, how many fired after a run had
    already stored (and read back) ``SUCCESS``.

Running
=======

The defaults expect a Redis broker on port 16479 and, for ``--backend redis``,
a Redis result backend on port 16480; the other backends use the services in
``../backends/docker-compose.yml``::

    docker run -d --name natural-broker -p 16479:6379 redis:7
    docker run -d --name natural-backend-redis -p 16480:6379 redis:7

    python run.py --backend redis --scenario same-id --tasks 200 --min 0.5 --max 1.5
    python run.py --backend couchdb --scenario visibility --tasks 1000 \
        --track-started --prefetch 1 --concurrency 16

``--celery`` and ``--kombu`` pick the trees the workers import (the
default is this checkout and the installed kombu). ``--out`` appends the JSON
line to a file, and ``--dump`` writes every event of the task ids that went
wrong.
