"""Checker tests on literal histories: every anomaly must fire, and only then."""
from itertools import count

from t.correctness.checker import check


class H:
    """Tiny history builder; times are assigned in call order."""

    def __init__(self):
        self.ops, self._t = [], count()

    def op(self, **kw):
        kw.setdefault('time', float(next(self._t)))
        self.ops.append(kw)
        return self

    def enqueue(self, op, result='ok', tid=None, process='c0', extra=None):
        self.op(process=process, type='invoke', f='enqueue', value=op)
        return self.op(process=process, type=result, f='enqueue', value=op,
                       tid=tid or f't{op}', extra=extra or {})

    def start(self, op, token, worker='w0', f='execute', **value):
        return self.op(process=f'{worker}.{token}', type='invoke', f=f, worker=worker,
                       value=dict(value, op=op, token=token, attempt=0))

    def finish(self, op, token, worker='w0', f='execute', **value):
        return self.op(process=f'{worker}.{token}', type='ok', f=f, worker=worker,
                       value=dict(value, op=op, token=token, attempt=0))

    def kill(self, worker, f='kill-worker'):
        return self.op(process='nemesis', type='info', f=f, value={'worker': worker})

    def read(self, tid, state, result=None, final=False, process='r0'):
        self.op(process=process, type='invoke', f='read', value={'tid': tid, 'final': final})
        return self.op(process=process, type='ok', f='read', value={'tid': tid, 'state': state, 'result': result})


def run(h, **config):
    return check(h.ops, dict({'workload': 'single'}, **config))


def test_clean_history_is_valid():
    h = H().enqueue(1).start(1, 11).finish(1, 11).read('t1', 'SUCCESS', 11, final=True)
    r = run(h)
    assert r['valid'] and r['counts'] == {}


def test_lost_never_started_is_a_violation_even_with_early_ack():
    h = H().enqueue(1).read('t1', 'PENDING', final=True)
    r = run(h)
    assert r['violations'] == ['lost-never-started']


def test_killed_running_task_is_documented_loss_under_early_ack_only():
    h = H().enqueue(1).start(1, 11).kill('w0').read('t1', 'PENDING', final=True)
    assert run(h)['valid']
    assert run(h, acks_late=True)['violations'] == ['lost-killed']


def test_broker_data_loss_explains_lost_tasks_only_without_durable_persistence():
    h = H().enqueue(1).op(process='nemesis', type='info', f='kill-broker', value={})
    assert run(h, broker='redis', broker_persistence='none')['valid']
    assert not run(h, broker='redis', broker_persistence='aof-always')['valid']


def test_duplicates_are_allowed_only_under_acks_late():
    h = H().enqueue(1).start(1, 11).finish(1, 11).start(1, 12, 'w1').finish(1, 12, 'w1')
    assert run(h, acks_late=True)['valid']
    assert run(h)['violations'] == ['duplicate-complete', 'duplicate-start']


def test_killed_execution_is_bounded_by_the_kill_not_infinite():
    # w0 is killed before w1 re-runs the task: not concurrent.
    h = H().enqueue(1).start(1, 11).kill('w0').start(1, 12, 'w1').finish(1, 12, 'w1')
    r = run(h, acks_late=True)
    assert 'concurrent' not in r['counts']
    # Without a kill the first execution is an orphan, which is itself reported.
    h = H().enqueue(1).start(1, 11).start(1, 12, 'w1').finish(1, 12, 'w1')
    assert 'orphan-execution' in run(h, acks_late=True)['violations']


def test_concurrent_executions_are_reported_but_allowed():
    h = H().enqueue(1).start(1, 11).start(1, 12, 'w1').finish(1, 11).finish(1, 12, 'w1')
    r = run(h, acks_late=True)
    assert r['counts']['concurrent'] == 1 and r['valid']


def test_unexpected_and_g1a():
    assert run(H().start(9, 1).finish(9, 1))['violations'] == ['unexpected']
    h = H().enqueue(1, result='fail').start(1, 11).finish(1, 11)
    assert run(h)['violations'] == ['g1a']
    h = H().enqueue(1, result='info').start(1, 11).finish(1, 11)
    assert run(h)['valid'] and run(h)['counts'] == {'recovered': 1}


def test_impossible_result():
    h = H().enqueue(1).start(1, 11).finish(1, 11).read('t1', 'SUCCESS', 99, final=True)
    assert run(h)['violations'] == ['impossible-result']


def test_result_changed_and_state_regression_need_a_completed_earlier_read():
    base = H().enqueue(1).start(1, 11).finish(1, 11).start(1, 12, 'w1').finish(1, 12, 'w1')
    h = base.read('t1', 'SUCCESS', 11).read('t1', 'SUCCESS', 12, final=True)
    assert run(h, acks_late=True)['violations'] == ['result-changed']
    h = H().enqueue(1).start(1, 11).finish(1, 11).read('t1', 'SUCCESS', 11).read('t1', 'STARTED') \
        .read('t1', 'SUCCESS', 11, final=True)
    assert run(h)['violations'] == ['state-regression']
    # Overlapping reads may legitimately disagree.
    h = H().enqueue(1).start(1, 11).finish(1, 11)
    h.op(process='r0', type='invoke', f='read', value={'tid': 't1'})
    h.op(process='r1', type='invoke', f='read', value={'tid': 't1'})
    h.op(process='r0', type='ok', f='read', value={'tid': 't1', 'state': 'SUCCESS', 'result': 11})
    h.op(process='r1', type='ok', f='read', value={'tid': 't1', 'state': 'STARTED', 'result': None})
    assert 'state-regression' not in run(h)['counts']


def test_result_missing_when_task_completed_but_never_visible():
    h = H().enqueue(1).start(1, 11).finish(1, 11).read('t1', 'PENDING', final=True)
    assert run(h)['violations'] == ['result-missing']


def chord_history(body_input, body_after_headers=True, extra_header=False):
    tids = ['h0', 'h1', 'h2']
    h = H().enqueue(1, tid='b', extra={'header_tids': tids, 'body_tid': 'b'})
    for part, token in enumerate((10, 20, 30)):
        h.start(1, token, f='execute-header', part=part)
        if part == 2 and not body_after_headers:
            h.start(1, 99, f='execute-body').finish(1, 99, f='execute-body', input=body_input)
        h.finish(1, token, f='execute-header', part=part)
    if extra_header:
        h.start(1, 11, 'w1', f='execute-header', part=0).finish(1, 11, 'w1', f='execute-header', part=0)
    if body_after_headers:
        h.start(1, 99, f='execute-body').finish(1, 99, f='execute-body', input=body_input)
    for tid, token in zip(tids, (10, 20, 30)):
        h.read(tid, 'SUCCESS', token, final=True)
    return h


def test_chord_correct_body_is_valid():
    r = check(chord_history([10, 20, 30]).ops, {'workload': 'chord', 'acks_late': True})
    assert r['valid'], r


def test_chord_wrong_input_and_early_fire():
    h = chord_history([10, 11, 20], body_after_headers=False, extra_header=True)
    r = check(h.ops, {'workload': 'chord', 'acks_late': True})
    assert {'chord-wrong-input', 'chord-early'} <= set(r['violations'])


def test_chord_unordered_mode_compares_as_multiset():
    h = chord_history([30, 10, 20])
    assert check(h.ops, {'workload': 'chord', 'result_chord_ordered': False})['valid']
    assert not check(h.ops, {'workload': 'chord'})['valid']


def test_chord_stuck():
    h = H().enqueue(1, tid='b', extra={'header_tids': ['h0'], 'body_tid': 'b'})
    h.start(1, 10, f='execute-header', part=0).finish(1, 10, f='execute-header', part=0)
    h.read('h0', 'SUCCESS', 10, final=True)
    assert check(h.ops, {'workload': 'chord'})['violations'] == ['chord-stuck']
