"""Two clients writing the same task id at once: does the first SUCCESS hold?

A task can run twice: the broker redelivers it after a lost worker or an
expired visibility timeout, or it's published twice. Both runs write the same
result key. Backends promise to keep the first SUCCESS, but most check what is
stored and then write in a separate step, so another run can store SUCCESS
between the two.

Each trial puts one writer behind Toxiproxy, which delays the server's
replies; that writer reads what is stored, and its write arrives late. The
other writer, connected directly, stores SUCCESS at a varied moment in
between. Afterwards the stored state must still be that SUCCESS.
"""
from __future__ import annotations

import os
import random
import threading
import time
import uuid

import pytest

from celery import states

from . import toxiproxy
from .model import observe

TRIALS = int(os.environ.get('CONFORMANCE_RACE_TRIALS', '20'))
LATENCY_MS = int(os.environ.get('CONFORMANCE_LATENCY_MS', '150'))

LATE_WRITES = {
    'late FAILURE': (states.FAILURE, lambda: ValueError('late run failed')),
    'late STARTED': (states.STARTED, lambda: {'pid': 2}),
    'late SUCCESS': (states.SUCCESS, lambda: 'second'),
}


@pytest.fixture
def slow_and_fast(target, app_factory):
    if target.port is None:
        pytest.skip(f'{target.name} has no network connection to delay')
    if not toxiproxy.available():
        pytest.skip('toxiproxy is not running')
    with toxiproxy.delayed(target.port, LATENCY_MS, f'conformance-{target.name}') as port:
        slow = app_factory(port=port).backend
        fast = app_factory().backend
        # Connect (and create tables) before timing anything.
        for backend in (slow, fast):
            backend.store_result(uuid.uuid4().hex, 0, states.SUCCESS)
        yield slow, fast


@pytest.mark.parametrize('late', sorted(LATE_WRITES))
def test_first_success_survives_a_late_writer(target, slow_and_fast, late):
    slow, fast = slow_and_fast
    state, make = LATE_WRITES[late]
    rng = random.Random(0)
    lost = []
    for trial in range(TRIALS):
        task_id = uuid.uuid4().hex
        errors = []

        def late_writer():
            try:
                slow.store_result(task_id, make(), state)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        thread = threading.Thread(target=late_writer)
        thread.start()
        # Store SUCCESS somewhere in the late writer's round trips.
        time.sleep(rng.uniform(0, 3 * LATENCY_MS) / 1000)
        fast.store_result(task_id, 'first', states.SUCCESS)
        seen = observe(fast.get_task_meta(task_id, cache=False))
        thread.join()
        assert not errors, errors
        final = observe(fast.get_task_meta(task_id, cache=False))
        if seen == (states.SUCCESS, 'first') and final != seen:
            lost.append((trial, final))
    assert not lost, (
        f'{target.name}: in {len(lost)} of {TRIALS} trials a {late} replaced a '
        f'SUCCESS a reader had already seen; e.g. trial {lost[0][0]} ended as '
        f'{lost[0][1]}')
