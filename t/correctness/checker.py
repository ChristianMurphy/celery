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
    'chord-body-without-headers', 'orphan-execution', 'running-at-end', 'stalled-in-broker',
    'retry-exceeded', 'retry-out-of-order', 'eta-early', 'chain-impossible-input',
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

    residue = set()
    for o in nemesis:
        if o['f'] == 'recovery-end':
            r = (o.get('value') or {}).get('residue') or {}
            residue |= set(r.get('ready', [])) | set(r.get('unacked', []))

    enq = enqueues(history)
    rd = reads(history)
    anomalies = defaultdict(list)
    workload = config.get('workload', 'single')

    def check_task(key, execs, enq_type, tid, label, done=None, duplicates=True, upstream_lost=False):
        if done is None:
            done = [e for e in execs if e['completed']]
        if enq_type is None and execs:
            anomalies['unexpected'].append(label)
        if enq_type == 'fail' and execs:
            anomalies['g1a'].append(label)
        if enq_type == 'info' and execs:
            anomalies['recovered'].append(label)
        if enq_type == 'ok' and not done and upstream_lost:
            # A chain step whose parent never completed: a consequence, not a separate loss.
            anomalies['chain-cut'].append(label)
        elif enq_type == 'ok' and not done:
            if tid in residue:
                # Still held by the broker when recovery gave up: stalled, not (yet) lost.
                anomalies['stalled-in-broker'].append(label)
            else:
                anomalies['lost-never-started' if not execs else 'lost-killed'].append(label)
        for e in execs:
            if not e['completed'] and e['killed_by'] == 'teardown':
                anomalies['running-at-end'].append(label)
            elif not e['completed'] and e['killed_by'] is None:
                anomalies['orphan-execution'].append(label)
        if duplicates and len(execs) > 1:
            anomalies['duplicate-start'].append(label)
        if duplicates and len(done) > 1:
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
            # Header tasks in this workload never raise, so once every header
            # has run the body must run, even if a header's result was lost.
            all_headers_ran = header_tids and None not in header_done
            if e.get('type') == 'ok' and all_headers_ran and not bodies:
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
    elif workload == 'retry':
        rx = executions(history, 'execute-retry')
        fails = config.get('retry_fails', 2)
        for op in sorted(set(enq) | set(rx), key=str):
            e, execs = enq.get(op, {}), rx.get(op, [])
            success = [x for x in execs if x['completed'] and x['value'].get('outcome') == 'success']
            check_task(op, execs, e.get('type'), e.get('tid'), op, done=success, duplicates=False)
            by_attempt = defaultdict(list)
            for x in execs:
                by_attempt[x['value']['attempt']].append(x)
            if any(len(v) > 1 for v in by_attempt.values()):
                anomalies['duplicate-start'].append(op)
            if len(success) > 1:
                anomalies['duplicate-complete'].append(op)
            if len(execs) > fails + 1:
                anomalies['retry-fanout'].append(op)
            if by_attempt and max(by_attempt) > fails:
                anomalies['retry-exceeded'].append(op)
            for k in sorted(by_attempt):
                if k == 0:
                    continue
                retried = [x['end'] for x in by_attempt.get(k - 1, [])
                           if x['completed'] and x['value'].get('outcome') == 'retry']
                if not retried or min(x['start'] for x in by_attempt[k]) < min(retried):
                    anomalies['retry-out-of-order'].append(op)
                    break
    elif workload == 'eta':
        ex = executions(history, 'execute-eta')
        for op in sorted(set(enq) | set(ex), key=str):
            e, execs = enq.get(op, {}), ex.get(op, [])
            check_task(op, execs, e.get('type'), e.get('tid'), op)
            if execs:
                first, eta = min(x['start'] for x in execs), execs[0]['value']['eta']
                if first < eta - 0.5:  # same host clock; allow scheduling granularity
                    anomalies['eta-early'].append(op)
                elif first > eta + config.get('eta_tolerance', 5):
                    anomalies['eta-late'].append(op)
    elif workload == 'chain':
        cx = executions(history, 'execute-chain')
        for op in sorted(set(enq) | {k[0] for k in cx}, key=str):
            e = enq.get(op, {})
            step_tids = e.get('extra', {}).get('step_tids', [])
            for part, tid in enumerate(step_tids):
                execs = cx.get((op, part), [])
                parents = [x for x in cx.get((op, part - 1), []) if x['completed']] if part else []
                check_task((op, part), execs, e.get('type'), tid, f'{op}.s{part}',
                           upstream_lost=bool(part) and not parents)
                allowed = {x['token'] for x in parents} if part else {None}
                inputs = {x['value'].get('input') for x in execs}
                if inputs - allowed:
                    anomalies['chain-impossible-input'].append(f'{op}.s{part}')
                if len(inputs) > 1:
                    anomalies['chain-fork'].append(f'{op}.s{part}')
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
        elif kind in ('retry-fanout', 'chain-fork'):
            explained[kind] = 'consequence of a duplicate execution'
        elif kind == 'chain-cut':
            explained[kind] = 'an earlier step of the chain never completed'
        elif kind == 'eta-late':
            explained[kind] = 'reported: delayed by faults or redelivery'
        elif kind in ('duplicate-start', 'duplicate-complete'):
            if acks_late:
                explained[kind] = 'allowed under acks_late (at-least-once)'
            elif (workload == 'eta' and config.get('broker', 'redis') == 'redis'
                  and config.get('eta_max', 0) > config.get('visibility_timeout', 3600)):
                explained[kind] = 'documented: ETA beyond the Redis visibility_timeout is redelivered'
            else:
                explained[kind] = None
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
