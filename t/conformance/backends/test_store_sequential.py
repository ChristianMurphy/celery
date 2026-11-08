"""One client, one operation at a time: does each backend match the model?

Hypothesis generates sequences of ``store_result()`` / ``forget()`` calls over
a few task ids and, after every step, reads each id back with a fresh lookup
and compares it with :class:`~.model.TaskModel`. A failure shrinks to the
shortest sequence that shows the backend storing something the model doesn't.
"""
from __future__ import annotations

import os
import uuid

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from celery import states
from celery.exceptions import TaskRevokedError

from .model import TaskModel, observe

EXAMPLES = int(os.environ.get('CONFORMANCE_EXAMPLES', '40'))

WRITES = {
    states.STARTED: lambda n: {'pid': n},
    'PROGRESS': lambda n: {'done': n},
    # mark_as_retry() stores the exception that caused the retry.
    states.RETRY: lambda n: KeyError(f'retry {n}'),
    states.SUCCESS: lambda n: n,
    states.FAILURE: lambda n: ValueError(f'boom {n}'),
    states.REVOKED: lambda n: TaskRevokedError(f'revoked {n}'),
}

task_slots = st.integers(min_value=0, max_value=2)


def run_machine(app_factory, *, ready_sticky):
    class StoreMachine(RuleBasedStateMachine):
        def __init__(self):
            super().__init__()
            self.backend = app_factory().backend
            prefix = uuid.uuid4().hex
            self.ids = [f'{prefix}-{i}' for i in range(3)]
            self.models = [TaskModel(ready_sticky=ready_sticky) for _ in self.ids]

        @rule(slot=task_slots, state=st.sampled_from(sorted(WRITES)),
              n=st.integers(0, 1000))
        def store(self, slot, state, n):
            value = WRITES[state](n)
            self.backend.store_result(self.ids[slot], value, state)
            self.models[slot].store(state, observe({'status': state, 'result': value})[1])

        @rule(slot=task_slots)
        def forget(self, slot):
            self.backend.forget(self.ids[slot])
            self.models[slot].forget()

        @invariant()
        def matches_model(self):
            for task_id, model in zip(self.ids, self.models):
                got = observe(self.backend.get_task_meta(task_id, cache=False))
                assert got == model.stored, (
                    f'{task_id[-1]}: backend has {got}, model has {model.stored}')

    StoreMachine.TestCase.settings = settings(
        max_examples=EXAMPLES, stateful_step_count=12, deadline=None,
        suppress_health_check=list(HealthCheck), derandomize=bool(os.environ.get('CI')))
    StoreMachine.TestCase().runTest()


def test_keeps_first_success(target, app_factory):
    """The rule every backend claims: SUCCESS is never replaced."""
    run_machine(app_factory, ready_sticky=target.ready_sticky)


def test_ready_state_is_sticky(target, app_factory):
    """The stricter rule (Elasticsearch only today); reported, not required."""
    if target.ready_sticky:
        return run_machine(app_factory, ready_sticky=True)
    try:
        run_machine(app_factory, ready_sticky=True)
    except AssertionError as exc:
        pytest.xfail(f'{target.name} lets an unready state replace a ready one: {exc}')
