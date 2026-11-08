"""A reference model of what a result backend stores for one task id.

The rules are the ones Celery's backends say they follow:

* A task id with nothing stored reads as ``PENDING`` with result ``None``.
* Each ``store_result()`` replaces what is stored, except that
* once ``SUCCESS`` is stored, later writes for that id are ignored
  ("keep the first SUCCESS"; ``BaseKeyValueStoreBackend._store_result``,
  and the database, MongoDB and Cassandra backends).
* ``forget()`` removes what is stored, so the id reads as ``PENDING`` again.

``READY_STICKY`` is a stricter rule that only the Elasticsearch backend
follows today: a ready state (SUCCESS, FAILURE, REVOKED) is never replaced by
an unready one (STARTED, RETRY, a custom state). The suite reports where each
backend stands on it rather than requiring either.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from celery import states

PENDING = (states.PENDING, None)


def observe(meta: dict) -> tuple[str, Any]:
    """Reduce stored meta to what a client can compare across backends."""
    status = meta.get('status')
    result = meta.get('result')
    if isinstance(result, BaseException):
        result = (type(result).__name__, tuple(str(a) for a in result.args))
    elif isinstance(result, dict):
        result = tuple(sorted(result.items()))
    return status, result


@dataclass
class TaskModel:
    ready_sticky: bool = False
    stored: tuple[str, Any] = PENDING

    def store(self, state: str, observed_result: Any) -> None:
        current = self.stored[0]
        if current == states.SUCCESS:
            return
        if (self.ready_sticky and current in states.READY_STATES
                and state in states.UNREADY_STATES):
            return
        self.stored = (state, observed_result)

    def forget(self) -> None:
        self.stored = PENDING
