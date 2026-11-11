"""Replay chord.qnt traces against the real Redis and memcached backends.

Every exec of the spec (one delivery of a header task) is a thread running
the real code: ``backend.mark_as_done(tid, result, request)`` for a header
run, and the real ``Request.on_failure(WorkerLostError)`` for a lost pool
child. The thread is parked at the points where the spec splits
``on_chord_part_return`` into steps, by wrapping the backend's own calls:

  redis  stored        store_result returned            (base.py:294)
         partReturned  CHORD_PART_RETURN_LUA returned   (redis.py:968)
         joined        before CHORD_CLAIM_EXTEND_LUA    (redis.py:1021)
         extended      CHORD_CLAIM_EXTEND_LUA returned
         published     before CHORD_CLEANUP_LUA         (redis.py:1042)
         cleaned       CHORD_CLEANUP_LUA returned
  kv     stored        store_result returned
         counted       incr returned                    (base.py:1456)
         joined        before _retry_publish            (base.py:1486)
         kclean        before the first delete          (base.py:1494)

A spec step releases one thread and waits for it to park again (or end);
where it parks must match the spec's next pc, and the backend state read
back must match the spec state. A lost worker is a thread that is never
released again (no ``finally`` runs, as with SIGKILL). The body publish is
``app.amqp._retry_publish``, replaced by a recorder that accepts or refuses
as the spec step says.

    python -m adapter.replay_chord --instance redis_wlost_late traces/chord/redis_wlost_late/*.json
"""
from __future__ import annotations

import argparse
import sys
import threading
import uuid
from pathlib import Path
from typing import Any

from billiard.einfo import ExceptionInfo

from celery import Celery, states
from celery.contrib.testing.mocks import TaskMessage
from celery.exceptions import WorkerLostError
from celery.worker.request import Request

from . import itf
from .backends import tag_of

URLS = {'redis': 'redis://127.0.0.1:6379/11',
        'kv': 'cache+memcached://127.0.0.1:11211/'}

# chord.qnt instances: (PROTO, ACKS_LATE, REJECT_ON_WORKER_LOST)
FAULTS = {'none': (True, False), 'dup': (True, False), 'refuse': (True, False),
          'wlost_late': (True, False), 'wlost_early': (False, False),
          'child_late': (True, False), 'child_early': (False, False),
          'child_reject': (True, True), 'lease': (True, False)}

# spec pc (HeaderPc) -> where the real thread must be parked
# ('END' = returned)
PARK = {
    'redis': {'Stored': 'stored', 'RedisClaimed': 'partReturned',
              'RedisJoined': 'joined', 'RedisExtended': 'extended',
              'RedisPublished': 'published'},
    'kv': {'Stored': 'stored', 'KvComplete': 'counted', 'KvJoined': 'joined',
           'KvClean': 'kclean'},
}
# the step's own hook point when the spec's run is at HeaderDone, before
# the code returns
PARK_DONE = {'PartReturn': 'partReturned', 'Extend': 'extended',
             'Cleanup': 'cleaned', 'Count': 'counted'}
RESULT = {'Pending': 'PENDING', 'Success': 'SUCCESS', 'Failure': 'FAILURE',
          'Evicted': 'PENDING'}
REPORT = {'Succeeded': 'SUCCESS', 'Failed': 'FAILURE'}


def action(act) -> tuple[str, int | None, str]:
    """``(kind, exec slot, outcome)`` of the spec's lastAction (Action)."""
    kind, value = itf.split(act)
    if isinstance(value, dict):
        return kind, value.get('w'), value.get('outcome', '')
    return kind, value if kind in ('Finish', 'DeliverUnlock') else None, ''


def header_run(e) -> dict | None:
    """The HeaderRun of an Exec, with ``pc`` as its tag; None otherwise."""
    kind, run = itf.split(e)
    if kind != 'HeaderTask':
        return None
    return {**run, 'pc': itf.tag(run['pc'])}

_local = threading.local()


class Mismatch(AssertionError):
    pass


class ExecThread:
    """One spec exec: a thread running real code, parked at hook points."""

    def __init__(self, target, name: str) -> None:
        self.cond = threading.Condition()
        self.at: str | None = None
        self.release = False
        self.dead = False
        self.info: dict[str, Any] = {}
        self.plan: str | None = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, args=(target,),
                                       name=name, daemon=True)

    def _run(self, target) -> None:
        _local.exec = self
        try:
            target()
        except BaseException as exc:  # noqa: BLE001 - reported as mismatch
            self.error = exc
        finally:
            with self.cond:
                self.at = 'END'
                self.cond.notify_all()

    def park(self, name: str, **info) -> None:
        with self.cond:
            self.at = name
            self.info.update(info)
            self.release = False
            self.cond.notify_all()
            while not self.release:
                self.cond.wait()   # a dead exec waits forever, like SIGKILL
            self.at = None

    def _wait_parked(self) -> str:
        with self.cond:
            if not self.cond.wait_for(lambda: self.at is not None, 20):
                raise Mismatch(f'{self.thread.name} did not park or end')
            return self.at

    def start(self) -> str:
        self.thread.start()
        return self._wait_parked()

    def advance(self) -> str:
        with self.cond:
            if self.at == 'END':
                return 'END'
            self.release = True
            self.at = None
            self.cond.notify_all()
        return self._wait_parked()


def _exec() -> ExecThread | None:
    return getattr(_local, 'exec', None)


def _park(name: str, **info) -> None:
    ex = _exec()
    if ex is not None:
        ex.park(name, **info)


class Harness:
    def __init__(self, proto: str, acks_late: bool, reject: bool) -> None:
        self.proto = proto
        app = Celery(f'quint-chord-{proto}', set_as_current=False,
                     broker='memory://')
        app.conf.update(result_backend=URLS[proto],
                        result_backend_always_retry=False,
                        result_chord_join_timeout=0.3,
                        task_acks_late=acks_late,
                        task_reject_on_worker_lost=reject)
        self.app = app

        @app.task(name='quint.header', shared=False)
        def header():  # pragma: no cover - never executed, only described
            return None

        @app.task(name='quint.body', shared=False)
        def body(results):  # pragma: no cover
            return results

        self.header = header
        self.published: list[list] = []
        app.amqp._retry_publish = self._retry_publish
        self.backend = app.backend
        self._install_hooks()

    # ---- hooks on the backend instance ----------------------------------
    def _retry_publish(self, publish, *args, **kwargs):
        ex = _exec()
        if self.proto == 'kv':
            _park('joined')
        plan = ex.plan if ex is not None else 'ok'
        if plan == 'refused':
            raise ConnectionError('quint: broker refused the publish')
        self.published.append(list(args[0]))
        return None

    def _install_hooks(self) -> None:
        b = self.backend
        orig_store = b.store_result

        def store_result(*a, **kw):
            r = orig_store(*a, **kw)
            if a and a[0] in self.tids.values():   # not the body's FAILURE
                _park('stored')
            return r
        b.store_result = store_result

        if self.proto == 'redis':
            for attr, before, after in (
                    ('_chord_part_return_script', None, 'partReturned'),
                    ('_chord_claim_extend_script', 'joined', 'extended'),
                    ('_chord_cleanup_script', 'published', 'cleaned')):
                script = getattr(b, attr)  # the registered redis Script

                def wrapper(*a, _s=script, _b=before, _a=after, **kw):
                    if _b:
                        _park(_b)
                    r = _s(*a, **kw)
                    _park(_a, ret=r)
                    return r
                b.__dict__[attr] = wrapper
        else:
            orig_incr, orig_delete = b.incr, b.delete

            def incr(key):
                r = orig_incr(key)
                _park('counted', ret=r)
                return r

            def delete(key):
                ex = _exec()
                if ex is not None and not ex.info.get('deleting'):
                    ex.info['deleting'] = True
                    _park('kclean')
                return orig_delete(key)
            b.incr, b.delete = incr, delete

    # ---- chord setup ----------------------------------------------------
    def new_chord(self) -> None:
        self.gid = f'quint-{uuid.uuid4().hex}'
        self.tids = {h: f'{self.gid}-h{h}' for h in (1, 2)}
        self.body_id = f'{self.gid}-body'
        self.callback = self.app.signature('quint.body',
                                           options={'task_id': self.body_id})
        self.published.clear()
        self.tok2h: dict[int, int] = {}
        self.realtok: dict[int, str] = {}
        results = [self.app.AsyncResult(self.tids[h]) for h in (1, 2)]
        if self.proto == 'redis':
            self.backend.set_chord_size(self.gid, 2)        # canvas.py:2109
            self.backend.apply_chord((self.gid, results), self.callback)
        else:
            self.backend.apply_chord((self.gid, results), self.callback)

    def message(self, h: int):
        return TaskMessage('quint.header', id=self.tids[h], group=self.gid,
                           group_index=h - 1, chord=dict(self.callback))

    def request(self, h: int) -> Request:
        return Request(self.message(h), app=self.app, task=self.header)

    def in_worker(self, fun):
        """Run ``fun`` with this backend as the thread's app.backend (the
        app keeps a backend per thread unless it is thread safe)."""
        def target():
            if not self.backend.thread_safe:
                self.app._local.backend = self.backend
            return fun()
        return target

    # ---- reading the real state -----------------------------------------
    def keys(self) -> dict[str, bytes]:
        k = self.backend.get_key_for_group
        return {s: k(self.gid, '.' + s) for s in 'jdsc'}

    def read(self) -> dict[str, Any]:
        b = self.backend
        st: dict[str, Any] = {}
        st['results'] = {h: b._get_task_meta_for(t)['status']
                         for h, t in self.tids.items()}
        body = b._get_task_meta_for(self.body_id)['status']
        st['chordError'] = body == states.FAILURE
        st['dispatched'] = len(self.published)
        if self.published:
            args = self.published[-1]
            st['bodyArgs'] = frozenset(self.tok2h[tag_of(r)] for r in args)
            st['bodyArgsSize'] = len(args)
        if self.proto == 'redis':
            c = b.client
            ks = self.keys()
            jset = set()
            for raw in c.zrange(ks['j'], 0, -1):
                _, tid, state, result = b.decode(raw)
                h = int(tid.rsplit('-h', 1)[1])
                jset.add((h, state, tag_of(result)))
            st['jset'] = jset
            st['dset'] = {int(m.decode().rsplit(':', 1)[0].rsplit('-h', 1)[1])
                          for m in c.smembers(ks['d'])}
            st['skey'] = bool(c.exists(ks['s']))
            st['ckey'] = c.get(ks['c'])
        else:
            key = b.get_key_for_chord(self.gid)
            st['counter'] = int(b.client.get(key) or 0)
            st['groupSaved'] = b.get(b.get_key_for_group(self.gid)) is not None
        return st

    def compare(self, spec: dict, where: str) -> None:
        real = self.read()
        want: dict[str, Any] = {
            'results': {h: RESULT[r] for h, r in spec['results'].items()},
            'chordError': spec['chordError'],
            'dispatched': spec['dispatched'],
        }
        if spec['dispatched']:
            want['bodyArgs'] = spec['bodyArgs']
            want['bodyArgsSize'] = spec['bodyArgsSize']
        if self.proto == 'redis':
            want['jset'] = {(dict(e)['h'], REPORT[dict(e)['res']],
                             dict(e)['tok']) for e in spec['jset']}
            want['dset'] = set(spec['dset'])
            want['skey'] = spec['skey']
            claim, tok = itf.split(spec['ckey'])
            want['ckey'] = {'NoClaim': None, 'Done': b'done'}.get(
                claim, self.realtok.get(tok, f'<token of {tok}>'))
            if isinstance(real['ckey'], bytes) and real['ckey'] != b'done':
                real['ckey'] = real['ckey'].decode()
        else:
            want['counter'] = spec['counter']
            want['groupSaved'] = spec['groupSaved']
        diff = {k: (real.get(k), v) for k, v in want.items()
                if real.get(k) != v}
        if diff:
            raise Mismatch(f'{where}: real != spec: '
                           + '; '.join(f'{k}: {a!r} != {b!r}'
                                       for k, (a, b) in diff.items()))


def replay(h: Harness, path: Path) -> int:
    states_ = itf.load(path)
    h.new_chord()
    threads: dict[int, ExecThread] = {}
    prev = states_[0]
    for i, st in enumerate(states_):
        kind, w, outcome = action(st['lastAction'])
        where = f'{path.name} step {i} ({kind} w={w} {outcome})'
        e_prev = header_run(prev['execs'][w]) if w else None
        e_new = header_run(st['execs'][w]) if w else None
        parked = None
        if kind in ('Init', 'Dup', 'Deliver', 'DeliverUnlock'):
            if kind == 'Deliver':
                h.tok2h[e_new['tok']] = e_new['h']
        elif kind == 'Store':
            tok, hh = e_new['tok'], e_new['h']
            req = h.request(hh)
            ex = ExecThread(h.in_worker(lambda r=req, t=tok: h.backend.mark_as_done(
                r.id, {'run': t}, request=r._context)), f'exec{w}')
            threads[w] = ex
            parked = ex.start()
        elif kind == 'Finish' and w not in threads:
            parked = 'END'   # the code already returned at the last step
        elif kind in ('PartReturn', 'Join', 'Extend', 'Cleanup', 'Count',
                      'Finish'):
            parked = threads[w].advance()
        elif kind == 'Publish':
            threads[w].plan = 'refused' if outcome == 'Refused' else 'ok'
            parked = threads[w].advance()
        elif kind == 'LoseWorker':
            if w in threads:
                threads.pop(w).dead = True   # never released again
        elif kind == 'LoseChild':
            old = threads.pop(w, None)
            if old is not None:
                old.dead = True
            hh, tok = e_prev['h'], e_prev['tok']
            req = h.request(hh)
            try:
                raise WorkerLostError(f'run{tok}')
            except WorkerLostError:
                einfo = ExceptionInfo()
            ex = ExecThread(h.in_worker(lambda r=req, ei=einfo: r.on_failure(
                ei, send_failed_event=False, return_ok=False)), f'main{w}')
            parked = ex.start()
            if parked != 'END':
                threads[w] = ex
        elif kind in ('ClaimExpires', 'DoneExpires'):
            h.backend.client.delete(h.keys()['c'])
        else:
            raise Mismatch(f'{where}: no replay for action {kind}')

        # the real thread must be where the spec's exec is
        if parked is not None:
            ex = threads.get(w)
            if ex is not None and ex.error is not None:
                raise Mismatch(f'{where}: thread raised {ex.error!r}')
            if e_new is None or kind == 'Finish':
                want = 'END'
            elif e_new['pc'] == 'HeaderDone':
                if (kind == 'Cleanup' and h.proto != 'redis'
                        or kind == 'Count' and outcome == 'NoGroup'):
                    want = 'END'
                else:
                    want = PARK_DONE.get(kind, 'END')
            else:
                want = PARK[h.proto][e_new['pc']]
            if parked != want:
                raise Mismatch(f'{where}: real thread at {parked}, spec pc '
                               f'{e_new["pc"] if e_new else "Free"} '
                               f'(expects {want})')
            if parked == 'END':
                threads.pop(w, None)
        # learn the real claim token when the spec's exec takes the claim
        claim, tok = itf.split(st['ckey'])
        if h.proto == 'redis' and claim == 'Claimed' and tok not in h.realtok:
            raw = h.backend.client.get(h.keys()['c'])
            if raw:
                h.realtok[tok] = raw.decode()
        h.compare(st, where)
        prev = st
    for ex in threads.values():   # leave parked execs parked (dead)
        ex.dead = True
    return len(states_)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--instance', required=True,
                    help='chord.qnt instance the traces come from, e.g. redis_dup')
    ap.add_argument('--mutate', action='append', default=[])
    ap.add_argument('traces', nargs='+', type=Path)
    args = ap.parse_args(argv)
    proto, fault = args.instance.split('_', 1)
    if proto not in URLS:
        print(f'{proto}: no replay target (unlock is checked by unit tests only)')
        return 2
    if args.mutate:
        from . import mutations
        for m in args.mutate:
            mutations.apply(m)
    acks_late, reject = FAULTS[fault]
    h = Harness(proto, acks_late, reject)
    failed = steps = 0
    for path in args.traces:
        try:
            steps += replay(h, path)
        except Mismatch as exc:
            failed += 1
            print(f'MISMATCH {exc}')
    print(f'{args.instance}: {len(args.traces) - failed}/{len(args.traces)} '
          f'traces matched ({steps} states)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
