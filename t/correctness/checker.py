"""Checkers over a canonical harness history.

A history is a list of Jepsen-style operations (dicts) in real-time order::

    {"index", "time", "process", "type": invoke|ok|fail|info, "f", "value", ...}

``f`` is one of ``enqueue`` (client publishes op), ``execute`` /
``execute-header`` / ``execute-body`` (a task execution, one process per
execution, killed executions have no completion), ``read`` (an uncached
result read) or a nemesis operation.

Every anomaly is reported.  Whether it invalidates the run depends on what
the configuration actually promises: e.g. losing a running task is the
documented behaviour of early acknowledgement, and concurrent executions are
allowed under late acknowledgement.
"""
from collections import defaultdict

INF = float('inf')

# Anomalies that are wrong under every configuration Celery documents.
ALWAYS_INVALID = (
    'unexpected', 'g1a', 'impossible-result', 'result-changed', 'state-regression',
    'result-missing', 'chord-wrong-input', 'chord-early', 'chord-stuck',
    'chord-body-without-headers', 'orphan-execution', 'running-at-end',
)
# Faults that can legitimately destroy broker or backend data.
BROKER_DATA_LOSS = ('kill-broker',)
BACKEND_DATA_LOSS = ('kill-backend',)
# Nemesis operations that are bookkeeping rather than faults.
NOT_FAULTS = ('start-worker', 'teardown', 'recovery-start', 'recovery-end', 'resume-worker')


def _completions(history):
    """Pair each invoke with its completion (by process)."""
    pending, pairs = {}, []
    for op in history:
        p = op['process']
        if op['type'] == 'invoke':
            pending[p] = op
        elif p in pending and op['type'] in ('ok', 'fail', 'info'):
            pairs.append((pending.pop(p), op))
    pairs.extend((inv, None) for inv in pending.values())
    return pairs


def _kills(history):
    """(time, kind) at which each worker lost its process tree, pool child, or was torn down."""
    kills = defaultdict(list)
    for op in history:
        if op['process'] != 'nemesis':
            continue
        v = op.get('value') or {}
        if op['f'] in ('kill-worker', 'crash-hook', 'teardown') and isinstance(v, dict) and v.get('worker'):
            kills[v['worker']].append((op['time'], op['f']))
    return kills


def executions(history, f):
    """Executions of task kind ``f`` grouped by logical key."""
    kills = _kills(history)
    by_key = defaultdict(list)
    for inv, done in _completions([o for o in history if o['f'] == f]):
        v = inv['value']
        key = (v['op'], v['part']) if 'part' in v else v['op']
        worker = inv.get('worker')
        killed_by = None
        if done is not None:
            end, completed = done['time'], True
        else:
            # A killed execution ran until, at the latest, the next kill of its worker.
            end, killed_by = next(((t, kind) for t, kind in sorted(kills.get(worker, ()))
                                   if t >= inv['time']), (INF, None))
            completed = False
        by_key[key].append({
            'start': inv['time'], 'end': end, 'completed': completed, 'killed_by': killed_by,
            'token': v['token'], 'worker': worker, 'tid': inv.get('tid'),
            'redelivered': inv.get('redelivered'), 'value': (done or inv)['value'],
        })
    return by_key


def enqueues(history):
    out = {}
    for inv, done in _completions([o for o in history if o['f'] == 'enqueue']):
        out[inv['value']] = {
            'time': inv['time'], 'type': done['type'] if done else None,
            'tid': (done or inv).get('tid'), 'extra': (done or {}).get('extra', {}),
        }
    return out


def reads(history):
    by_tid = defaultdict(list)
    for inv, done in _completions([o for o in history if o['f'] == 'read']):
        if done is None or done['type'] != 'ok':
            continue
        by_tid[inv['value']['tid']].append({
            'invoke': inv['time'], 'complete': done['time'],
            'state': done['value']['state'], 'result': done['value'].get('result'),
            'final': bool(inv['value'].get('final')),
        })
    return by_tid


def read_anomalies(rs):
    """Regressions and changed results: compare each read with earlier *completed* reads."""
    out = []
    rs = sorted(rs, key=lambda r: r['invoke'])
    for i, later in enumerate(rs):
        for earlier in rs[:i]:
            if earlier['complete'] >= later['invoke'] or earlier['state'] != 'SUCCESS':
                continue
            if later['state'] != 'SUCCESS':
                out.append(('state-regression', earlier, later))
                break
            if later['result'] != earlier['result']:
                out.append(('result-changed', earlier, later))
                break
    return out


def final_read(rs):
    finals = [r for r in rs if r['final']]
    return finals[-1] if finals else None


def concurrent(execs):
    iv = sorted((e['start'], e['end']) for e in execs)
    return any(iv[i + 1][0] < iv[i][1] for i in range(len(iv) - 1))


def check(history, config):
    nemesis = [o for o in history if o['process'] == 'nemesis']
    fault_kinds = {o['f'] for o in nemesis if o['f'] not in NOT_FAULTS and not o['f'].startswith('heal-')}
    hooks = {(o.get('value') or {}).get('point') for o in nemesis if o['f'] == 'crash-hook'}
    persistent_broker = config.get('broker_persistence', 'aof-everysec') in ('aof-always',) \
        or config.get('broker') == 'rabbitmq'
    persistent_backend = config.get('backend') != 'redis' \
        or config.get('backend_persistence', 'aof-everysec') == 'aof-always'
    broker_loss_possible = bool(fault_kinds & set(BROKER_DATA_LOSS)) and not persistent_broker
    backend_loss_possible = bool(fault_kinds & set(BACKEND_DATA_LOSS)) and not persistent_backend

    enq = enqueues(history)
    rd = reads(history)
    anomalies = defaultdict(list)
    workload = config.get('workload', 'single')

    def check_task(key, execs, enq_type, tid, label):
        done = [e for e in execs if e['completed']]
        if enq_type is None and execs:
            anomalies['unexpected'].append(label)
        if enq_type == 'fail' and execs:
            anomalies['g1a'].append(label)
        if enq_type == 'info' and execs:
            anomalies['recovered'].append(label)
        if enq_type == 'ok' and not done:
            anomalies['lost-never-started' if not execs else 'lost-killed'].append(label)
        for e in execs:
            if not e['completed'] and e['killed_by'] == 'teardown':
                anomalies['running-at-end'].append(label)
            elif not e['completed'] and e['killed_by'] is None:
                anomalies['orphan-execution'].append(label)
        if len(execs) > 1:
            anomalies['duplicate-start'].append(label)
        if len(done) > 1:
            anomalies['duplicate-complete'].append(label)
        if len(execs) > 1 and concurrent(execs):
            anomalies['concurrent'].append(label)
        rs = rd.get(tid, [])
        tokens = {e['token'] for e in done}
        for r in rs:
            if r['state'] == 'SUCCESS' and r['result'] not in tokens:
                anomalies['impossible-result'].append(label)
                break
        for kind in sorted({kind for kind, _, _ in read_anomalies(rs)}):
            anomalies[kind].append(label)
        fr = final_read(rs)
        if enq_type == 'ok' and done and fr and fr['state'] != 'SUCCESS':
            anomalies['result-missing'].append(label)
        return fr

    if workload == 'single':
        ex = executions(history, 'execute')
        for op in sorted(set(enq) | set(ex), key=str):
            e = enq.get(op, {})
            check_task(op, ex.get(op, []), e.get('type'), e.get('tid'), op)
    elif workload == 'chord':
        hx = executions(history, 'execute-header')
        bx = executions(history, 'execute-body')
        ordered = config.get('result_chord_ordered', True)
        for op in sorted(set(enq) | {k[0] for k in hx} | set(bx), key=str):
            e = enq.get(op, {})
            extra = e.get('extra', {})
            header_tids = extra.get('header_tids', [])
            stored, header_done = [], []
            for part, htid in enumerate(header_tids):
                fr = check_task((op, part), hx.get((op, part), []), e.get('type'), htid, f'{op}.h{part}')
                stored.append(fr['result'] if fr and fr['state'] == 'SUCCESS' else None)
                done = [x['end'] for x in hx.get((op, part), []) if x['completed']]
                header_done.append(min(done) if done else None)
            bodies = bx.get(op, [])
            body_label = f'{op}.body'
            if e.get('type') is None and bodies:
                anomalies['unexpected'].append(body_label)
            if len(bodies) > 1:
                anomalies['duplicate-body'].append(body_label)
            all_headers_ok = header_tids and all(s is not None for s in stored)
            if e.get('type') == 'ok' and all_headers_ok and not bodies:
                anomalies['chord-stuck'].append(body_label)
            for b in bodies:
                got = b['value'].get('input')
                if got is None:  # killed before recording its input
                    continue
                if not all_headers_ok or None in header_done:
                    anomalies['chord-body-without-headers'].append(body_label)
                    continue
                if (got != stored) if ordered else (sorted(map(str, got)) != sorted(map(str, stored))):
                    anomalies['chord-wrong-input'].append(body_label)
                if b['start'] < max(header_done):
                    anomalies['chord-early'].append(body_label)
    else:
        raise ValueError(workload)

    # Verdicts: which anomalies does this configuration's documented guarantee forbid?
    acks_late = config.get('acks_late', False)
    explained = {}
    for kind in anomalies:
        if kind in ALWAYS_INVALID:
            explained[kind] = None
        elif kind in ('concurrent', 'recovered', 'duplicate-body'):
            explained[kind] = 'allowed: at-least-once delivery has no mutual exclusion'
        elif kind in ('duplicate-start', 'duplicate-complete'):
            explained[kind] = 'allowed under acks_late (at-least-once)' if acks_late else None
        elif kind == 'lost-killed':
            if not acks_late:
                explained[kind] = 'documented: early ack loses the task running on a killed worker'
            elif 'child_exit_after_store' in hooks and not config.get('reject_on_worker_lost'):
                explained[kind] = 'documented: pool child lost and reject_on_worker_lost is off'
            else:
                explained[kind] = None
        elif kind == 'lost-never-started':
            explained[kind] = None
        if explained.get(kind) is None and kind.startswith('lost') and broker_loss_possible:
            explained[kind] = 'broker restarted without durable persistence'
        if explained.get(kind) is None and kind == 'result-missing' and backend_loss_possible:
            explained[kind] = 'backend restarted without durable persistence'
    violations = sorted(k for k, why in explained.items() if why is None)
    return {
        'valid': not violations,
        'violations': violations,
        'counts': {k: len(v) for k, v in sorted(anomalies.items())},
        'examples': {k: v[:10] for k, v in sorted(anomalies.items())},
        'explained': {k: v for k, v in explained.items() if v},
        'ops': len(enq),
        'faults': sorted(fault_kinds),
    }
