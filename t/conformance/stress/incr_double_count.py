"""Finding chord-incr-double-count: two runs of one header both incr.

Workload: chords whose headers have mixed durations; some outlive the Redis
broker's visibility timeout (scaled from 1 h down to seconds), so the broker
redelivers them while they still run and both runs count. If another header
is still running result_chord_join_timeout after the counter reaches the
chord size, the chord fails with a join timeout although no header failed.

    python incr_double_count.py shape  --backend memcached --chords 10
    python incr_double_count.py random --backend memcached --chords 40
    python incr_double_count.py shape  --backend redis     (control)
    python incr_double_count.py shape  --vt 60             (control: no redelivery)
"""
from __future__ import annotations

import argparse
import random
import time
import uuid
from collections import Counter

import common as C


def classify(gid_events, body_events, errbacks, state):
    """Classify one chord from the task bodies' own records.

    The first errback carries the reason the chord failed first; later
    errbacks come from late header runs that find the group deleted
    (finding chord-incr-errback-after-success) and overwrite the body's
    stored FAILURE, so the stored state alone can't tell.
    """
    ran = len(body_events)
    starts = Counter(e['h'] for e in gid_events if e['kind'] == 'start')
    redelivered = any(v > 1 for v in starts.values())
    # errbacks are task messages and can run out of order behind a backlog;
    # only the join raises TimeoutError, so look at all of them.
    first = next((e['exc'] for e in errbacks if 'TimeoutError' in e['exc']),
                 errbacks[0]['exc'] if errbacks else '')
    if ran == 0 and 'TimeoutError' in first:
        verdict = 'join_timeout'          # the double count
    elif ran == 0 and errbacks:
        verdict = 'failed_other'
    elif ran == 0:
        verdict = 'pending'
    elif errbacks:
        verdict = 'errback_after_body'    # chord-incr-errback-after-success
    else:
        verdict = 'ok'
    return dict(verdict=verdict, body_ran=ran, errbacks=len(errbacks),
                first_error=first[:200], redelivered=redelivered,
                starts=dict(starts), state=state)


def drain(run, gid, tag, quiet, limit):
    """One chord at a time: wait until it has an outcome, every run of its
    headers has ended and none started for ``quiet`` seconds, so copies of
    one chord's headers don't pile onto the next chord's."""
    end = time.time() + limit
    while time.time() < end:
        evs = C.events(run)
        g = [e for e in evs if e.get('gid') == gid]
        done = any(e.get('tag') == tag and e['kind'] in ('body', 'errback') for e in evs)
        starts = sum(e['kind'] == 'start' for e in g)
        ends = sum(e['kind'] == 'end' for e in g)
        last = max((e['t'] for e in g), default=time.time())
        if done and starts == ends and time.time() - last > quiet:
            return True
        time.sleep(0.5)
    return False


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=['shape', 'random'])
    ap.add_argument('--backend', choices=['memcached', 'redis'], default='memcached')
    ap.add_argument('--chords', type=int, default=10)
    ap.add_argument('--vt', type=float, default=2)
    ap.add_argument('--concurrency', type=int, default=2)
    ap.add_argument('--durations', default='3,12',
                    help='shape mode: header durations in seconds')
    ap.add_argument('--seed', type=int, default=1)
    ap.add_argument('--settle', type=float, default=20)
    a = ap.parse_args(argv)

    run = f'incr-{a.mode}-{a.backend}-vt{a.vt:g}-{uuid.uuid4().hex[:6]}'
    backend = C.MEMCACHED if a.backend == 'memcached' else f'{C.REDIS}/21'
    env = C.base_env(broker=f'{C.REDIS}/20', backend=backend, queue=run, vt=a.vt,
                     acks_late=1, ev=run, join_timeout=3)
    C.apply_env(env)
    from celery import chord

    import stress_app as S
    print(C.proof())
    log = C.RESULTS / f'{run}.worker.log'
    C.RESULTS.mkdir(exist_ok=True)
    load0 = C.load()
    w = C.Worker(env, log, concurrency=a.concurrency)
    w.wait_ready()
    rng = random.Random(a.seed)
    chords = []
    try:
        for i in range(a.chords):
            if a.mode == 'shape':
                # H1 outlives the visibility timeout, H2 runs much longer.
                durations = [float(x) for x in a.durations.split(',')]
            else:
                # Heterogeneous durations: most short, some past the timeout.
                durations = [rng.choice([0.1, 0.5, 1, 3, 6, 12])
                             for _ in range(3)]
            tag = f'{run}-{i}'
            res = chord([S.header.s(h + 1, d) for h, d in enumerate(durations)])(
                S.body.s(tag).on_error(S.errback.s(tag=tag)))
            chords.append(dict(tag=tag, body=res.id, durations=durations,
                               gid=res.parent.id))
            drain(run, res.parent.id, tag, quiet=a.vt + 4,
                  limit=max(durations) * 6 + 60)
        time.sleep(a.settle)     # late redeliveries
    finally:
        w.stop()
    load1 = C.load()

    evs = C.events(run)
    out = []
    for c in chords:
        gev = [e for e in evs if e.get('gid') == c['gid']]
        bev = [e for e in evs if e['kind'] == 'body' and e.get('tag') == c['tag']]
        erb = [e for e in evs if e['kind'] == 'errback' and e.get('tag') == c['tag']]
        c.update(classify(gev, bev, erb, S.app.AsyncResult(c['body']).state))
        out.append(c)
    verdicts = Counter(c['verdict'] for c in out)
    n_fail = verdicts['join_timeout']
    summary = dict(run=run, mode=a.mode, backend=a.backend, vt=a.vt,
                   concurrency=a.concurrency,
                   chords=len(out), verdicts=dict(verdicts),
                   redelivered=sum(c['redelivered'] for c in out),
                   join_timeout=C.fmt_rate(n_fail, len(out)),
                   load_before=load0, load_after=load1, proof=C.proof())
    C.save(run, dict(summary=summary, chords=out, events=evs))
    for k, v in summary.items():
        print(f'{k}: {v}')


if __name__ == '__main__':
    main()
