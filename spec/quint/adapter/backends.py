"""Backends the adapter drives, and the hooks that split a store in two.

The spec splits ``store_result`` at the point where each backend reads what
is stored before it writes (see resultstate.qnt, ``begin``/``commit``). The
adapter runs the real ``store_result`` in a thread and parks that thread at
the same point, so the code under test is the unmodified backend method:

* rtw / es: right after ``_get_task_meta_for`` returns inside
  ``BaseKeyValueStoreBackend._store_result`` (base.py:1367).
* sql: right after the unlocked ``_query_task`` (database/__init__.py:174).
* mongo: at the entry of ``MongoBackend._store_result`` (it has no read).
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from celery import Celery

RUN = uuid.uuid4().hex[:8]

URLS = {
    'redis': 'redis://127.0.0.1:6379/11',
    'mongo': f'mongodb://127.0.0.1:27017/quint_replay_{RUN}',
    'sql': 'db+postgresql+psycopg2://postgres:pw@127.0.0.1:5432/celery',
    'es': f'elasticsearch://127.0.0.1:9200/quint_replay_{RUN}',
}
# Which spec instance (resultstate.qnt) models each backend.
SPEC_STORE = {'redis': 'rtw', 'mongo': 'mongo', 'sql': 'sql', 'es': 'es'}


def make_app(name: str) -> Celery:
    app = Celery(f'quint-{name}', set_as_current=False)
    app.conf.update(
        result_backend=URLS[name],
        result_backend_always_retry=False,
        result_extended=False,
    )
    return app


_local = threading.local()


class Gate:
    """Parks a run's thread at the split point until ``go`` is set."""

    def __init__(self) -> None:
        self.reached = threading.Event()
        self.go = threading.Event()
        self.used = False

    def park(self) -> None:
        if self.used:
            return
        self.used = True
        self.reached.set()
        if not self.go.wait(30):
            raise RuntimeError('gate never released')


def _gate() -> Gate | None:
    return getattr(_local, 'gate', None)


def install_hooks(name: str, backend: Any) -> None:
    """Wrap the backend instance's split-point method (instance attribute,
    so class-level mutations still take effect underneath)."""
    if name in ('redis', 'es'):
        orig = backend._get_task_meta_for

        def _get_task_meta_for(task_id):
            meta = orig(task_id)
            gate = _gate()
            if gate is not None:
                gate.park()
            return meta
        backend._get_task_meta_for = _get_task_meta_for
    elif name == 'sql':
        orig_q = backend._query_task

        def _query_task(session, task_id, for_update=False):
            task = orig_q(session, task_id, for_update=for_update)
            gate = _gate()
            if gate is not None and not for_update:
                gate.park()
            return task
        backend._query_task = _query_task
    elif name == 'mongo':
        orig_s = backend._store_result

        def _store_result(*args, **kwargs):
            gate = _gate()
            if gate is not None:
                gate.park()
            return orig_s(*args, **kwargs)
        backend._store_result = _store_result
    else:  # pragma: no cover
        raise ValueError(name)


@dataclass
class Run:
    thread: threading.Thread
    gate: Gate
    error: list


def start_store(backend: Any, task_id: str, result: Any, state: str) -> Run:
    """Start ``store_result`` in a thread and wait until it reaches the
    split point (or finishes, if it returned before it)."""
    gate = Gate()
    error: list = []

    def body() -> None:
        _local.gate = gate
        try:
            backend.store_result(task_id, result, state)
        except Exception as exc:  # noqa: BLE001 - recorded, compared to spec
            error.append(exc)
        finally:
            _local.gate = None
            gate.reached.set()

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    if not gate.reached.wait(30):
        raise RuntimeError('store_result never reached the split point')
    return Run(thread, gate, error)


def finish_store(run: Run) -> Exception | None:
    run.gate.go.set()
    run.thread.join(30)
    if run.thread.is_alive():
        raise RuntimeError('store_result did not finish')
    return run.error[0] if run.error else None


def value_for(status: str, v: int) -> Any:
    """The result a run with tag ``v`` stores for ``status``."""
    if status in ('FAILURE', 'RETRY'):
        return ValueError(f'run{v}')
    return {'run': v}


def tag_of(result: Any) -> int:
    if result is None:
        return 0
    if isinstance(result, dict):
        if 'run' in result:
            return int(result['run'])
        if result.get('exc_message'):
            msg = result['exc_message']
            msg = msg[0] if isinstance(msg, (list, tuple)) else msg
            return int(str(msg)[3:])
    if isinstance(result, BaseException) and result.args:
        return int(str(result.args[0])[3:])
    raise ValueError(f'cannot read run tag from {result!r}')


def observe(reader: Any, task_id: str) -> dict:
    meta = reader._get_task_meta_for(task_id)
    return {'status': meta['status'], 'v': tag_of(meta.get('result'))}


Hook = Callable[[str, Any], None]
