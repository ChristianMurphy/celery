"""Record result-backend histories from real concurrent runs (code -> spec).

Two writer threads store the same task id the way duplicate runs of a task
do (STARTED, then SUCCESS, FAILURE or RETRY), with natural timing, while a
reader polls for SUCCESS and one forget() may happen. Every backend call
that touches the stored meta runs under one lock and is logged in the order
it ran, with the meta read back right after it. So the log is a
linearization of what really happened, at the same split points as the
spec: the read before a write (``begin``), the write (``commit``).

Writes JSON lines, one history per line:
    {"backend": "redis", "events": [{"kind": "begin", "rn": 1, "status": "STARTED", "stored": {...}}, ...]}

    python -m adapter.record_results --backend redis --n 200 > histories/redis.jsonl
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
import uuid

from .backends import make_app, observe, value_for

LOCK = threading.Lock()
_local = threading.local()


def instrument(name: str, backend, reader, log: list) -> None:
    """Log ``begin`` at the read and ``commit`` at the write, under LOCK."""

    def snap():
        return observe(reader, _local.task_id)

    if name in ('redis', 'es'):
        orig_get, orig_set = backend._get_task_meta_for, backend._set_with_state

        def _get_task_meta_for(task_id):
            run = getattr(_local, 'run', None)
            if run is None:
                return orig_get(task_id)
            with LOCK:
                meta = orig_get(task_id)
                log.append({'kind': 'begin', 'rn': run[0], 'status': run[1],
                            'stored': snap()})
            return meta

        def _set_with_state(key, value, state):
            run = getattr(_local, 'run', None)
            with LOCK:
                r = orig_set(key, value, state)
                log.append({'kind': 'commit', 'rn': run[0], 'stored': snap(),
                            'wrote': True})
                _local.committed = True
            return r
        backend._get_task_meta_for = _get_task_meta_for
        backend._set_with_state = _set_with_state
    elif name == 'mongo':
        orig = backend._store_result

        def _store_result(*a, **kw):
            run = _local.run
            with LOCK:
                log.append({'kind': 'begin', 'rn': run[0], 'status': run[1],
                            'stored': snap()})
            with LOCK:
                r = orig(*a, **kw)
                log.append({'kind': 'commit', 'rn': run[0], 'stored': snap()})
                _local.committed = True
            return r
        backend._store_result = _store_result
    else:
        raise SystemExit(f'recording is not wired for {name}')


def one_history(name: str, backend, reader, rng: random.Random) -> dict:
    log: list = []
    task_id = f'quint-rec-{uuid.uuid4().hex}'
    _local.task_id = task_id
    instrument(name, backend, reader, log)
    stop = threading.Event()

    def writer(rn: int) -> None:
        _local.task_id = task_id
        final = rng.choice(['SUCCESS', 'FAILURE', 'RETRY', 'SUCCESS'])
        for status in ('STARTED', final):
            time.sleep(rng.uniform(0, 0.004))
            _local.run, _local.committed = (rn, status), False
            backend.store_result(task_id, value_for(status, rn), status)
            if not _local.committed:     # returned without writing
                with LOCK:
                    log.append({'kind': 'commit', 'rn': rn,
                                'stored': observe(reader, task_id)})
            _local.run = None

    def watcher() -> None:
        _local.task_id = task_id
        seen = False
        while not stop.is_set():
            with LOCK:
                got = observe(reader, task_id)
                if got['status'] == 'SUCCESS' and not seen:
                    seen = True
                    log.append({'kind': 'observe', 'stored': got})
                if got['status'] != 'SUCCESS':
                    seen = False if got['status'] == 'PENDING' else seen
            time.sleep(0.0005)

    def forgetter() -> None:
        _local.task_id = task_id
        time.sleep(rng.uniform(0, 0.01))
        with LOCK:
            backend.forget(task_id)
            log.append({'kind': 'forget', 'stored': observe(reader, task_id)})

    threads = [threading.Thread(target=writer, args=(1,)),
               threading.Thread(target=writer, args=(2,))]
    if rng.random() < 0.3:
        threads.append(threading.Thread(target=forgetter))
    w = threading.Thread(target=watcher)
    w.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    w.join()
    # un-instrument (instance attributes)
    for attr in ('_get_task_meta_for', '_set_with_state', '_store_result'):
        backend.__dict__.pop(attr, None)
    return {'backend': name, 'task_id': task_id, 'events': log}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backend', required=True, choices=['redis', 'mongo', 'es'])
    ap.add_argument('--n', type=int, default=100)
    ap.add_argument('--seed', type=int, default=0)
    args = ap.parse_args(argv)
    rng = random.Random(args.seed)
    backend = make_app(args.backend).backend
    reader = make_app(args.backend).backend
    for _ in range(args.n):
        print(json.dumps(one_history(args.backend, backend, reader, rng)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
