"""Check recorded 2-header chord histories against chord.qnt (kv protocol).

The worker is unmodified, so the only events are the task bodies' own
start/end records and the body/errback records. Backend steps are inferred:
a header run's store() and count() follow its end record (mark_as_done runs
right after the body returns, base.py:290-296). Each chord that failed with
a join timeout becomes a Quint run that replays the header runs that ended
before the first errback, then expects the spec's kvJoin(w, false): a chord
error with no failed header (kvDup.duplicateCountsTwiceTest generalised).
``quint test`` then says whether the spec allows this history.

    python quint_check.py results/<run>.json
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# chord.qnt copied from verify/quint-result-chords at fecda6a (spec/quint/)
SPEC = Path(__file__).resolve().parent / 'spec'


def to_run(i, chord, events):
    gid = chord['gid']
    errs = [e for e in events if e['kind'] == 'errback' and e.get('tag') == chord['tag']]
    cutoff = min(e['t'] for e in errs)
    runs = {}   # (pid, start t) -> run
    open_by_pid = {}
    for e in sorted(events, key=lambda e: e['t']):
        if e.get('gid') != gid:
            continue
        if e['kind'] == 'start':
            r = dict(h=e['h'], start=e['t'], end=None)
            runs[(e['pid'], e['t'])] = r
            open_by_pid[e['pid']] = r
        elif e['kind'] == 'end' and e['pid'] in open_by_pid:
            open_by_pid.pop(e['pid'])['end'] = e['t']
    done = [r for r in runs.values() if r['end'] is not None and r['end'] < cutoff]
    steps, timeline = [], []
    for k, r in enumerate(done):
        timeline.append((r['start'], 'start', k))
        timeline.append((r['end'], 'end', k))
    timeline.sort()
    queued = {1: 1, 2: 1}
    free, slot, count = [1, 2, 3], {}, 0
    for _, what, k in timeline:
        h = done[k]['h']
        if what == 'start':
            if queued[h] == 0:
                steps.append(f'.then(dup({h}))')
                queued[h] += 1
            queued[h] -= 1
            w = slot[k] = free.pop(0)
            steps.append(f'.then(deliver({w}, {h}))')
        else:
            w = slot[k]
            count += 1
            out = 'complete' if count == 2 else 'over' if count > 2 else 'counted'
            steps.append(f'.then(store({w})).then(count({w}))'
                         f'.expect(lastAction.outcome == "{out}")')
            if out == 'complete':
                steps.append(f'.then(kvJoin({w}, false))'
                             '.expect(chordError and not(anyFailed) and dispatched == 0)')
                break
            steps.append(f'.then(finish({w}))')
            free.append(w)
            free.sort()
    return f'  run c{i} = init\n    ' + '\n    '.join(steps)


def main(path):
    d = json.loads(Path(path).read_text())
    failed = [c for c in d['chords'] if c['verdict'] == 'join_timeout']
    if not failed:
        print('no join_timeout chords to check')
        return 0
    runs = '\n\n'.join(to_run(i, c, d['events']) for i, c in enumerate(failed))
    mod = 'hist_incr'
    src = (f'module {mod} {{\n  import chord(PROTO = "kv", ACKS_LATE = true, '
           'REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 20, '
           'MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).* '
           f'from "./chord"\n\n{runs}\n}}\n')
    out = SPEC / (Path(path).stem + '.qnt')
    out.write_text(src)
    p = subprocess.run(['quint', 'test', '--backend=typescript', f'--main={mod}',
                        str(out), '--match=^c[0-9]+$'], capture_output=True, text=True)
    text = p.stdout + p.stderr
    ok = text.count('ok c')
    print(f'{out.name}: {ok} of {len(failed)} join-timeout histories allowed by '
          f'chord.qnt as duplicateCountsTwice (quint exit {p.returncode})')
    if p.returncode:
        print('\n'.join(text.splitlines()[-25:]))
    return p.returncode


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
