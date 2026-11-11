#!/usr/bin/env python3
"""Plant bugs in the spec and check that a property catches each one.

Each mutant is the spec with one rule changed back to what the code did
before a fix (or a plausible slip). The property named must be violated on
an instance where the unmutated spec satisfies it.

    python3 mutate_spec.py
    python3 mutate_spec.py transport      # only mutants that mention a word
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import tlc

HERE = Path(__file__).resolve().parent

# (name, spec, instance, property, old, new)
MUTANTS = [
    ('mongo filter without $ne SUCCESS', 'resultstate.qnt', 'mongo', 'successStable',
     'if (curStatus == Success) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>',
     'if (false) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>'),
    ('sql without the locked re-read', 'resultstate.qnt', 'sql', 'successStable',
     'else if (curStatus == Success) (cur, Skipped) // :186-190',
     'else if (read == Success) (cur, Skipped) // :186-190'),
    ('es without the ready check', 'resultstate.qnt', 'es', 'readySticky',
     'else if (isReady(curStatus) and isUnready(w.status))\n          (cur, Skipped)',
     'else if (false)\n          (cur, Skipped)'),
    ('redis: no dedupe (before celery PR 10919)', 'chord.qnt', 'redis_dup', 'argsComplete',
     'val fresh = not(dset.contains(r.h))', 'val fresh = true'),
    ('redis: claim without NX (before celery PR 10998)', 'chord.qnt', 'redis_dup', 'atMostOnce',
     'val claim = complete and ckey == NoClaim', 'val claim = complete'),
    # The two owner checks below only matter when a claim can expire while
    # its holder still runs (leaseSafe: false), and then atMostOnce fails
    # without the mutation too: expected to be equivalent mutants.
    ('EQUIV redis: extend ignores the owner', 'chord.qnt', 'redis_wlost_late', 'atMostOnce',
     'val mine = ckey == Claimed(r.tok)\n    val reacquire = ckey == NoClaim and skey',
     'val mine = true\n    val reacquire = ckey == NoClaim and skey'),
    ('EQUIV redis: cleanup ignores the owner (celery issue 10997)', 'chord.qnt', 'redis_dup', 'atMostOnce',
     'val mine = ckey == Claimed(r.tok)\n    all {\n      CFG.proto == Redis, isHeader(e), at(r.pc) == AtRedisPublished',
     'val mine = true\n    all {\n      CFG.proto == Redis, isHeader(e), at(r.pc) == AtRedisPublished'),
    ('kv: count on >= size', 'chord.qnt', 'kv_dup', 'atMostOnce',
     '...r, pc: if (counter + 1 == N) KvComplete else HeaderDone',
     '...r, pc: if (counter + 1 >= N) KvComplete else HeaderDone'),
    ('unlock: retry refusal not failing the chord (before celery PR 10959)', 'chord.qnt', 'unlock_refuse', 'quietDone',
     "markError, uq' = uq,\n        used' = { ...used, refusals: used.refusals + 1 },",
     "keepObs, uq' = uq,\n        used' = { ...used, refusals: used.refusals + 1 },"),
    ('lost child counted again (before celery PR 10752)', 'chord.qnt', 'kv_child_late', 'noSpuriousError',
     'if (isHeader(e) and results.get(r.h) == Success) all {',
     'if (false) all {'),
    # A loop: the layered liveness checks must see it.
    ('unlock: a retry that uses no budget (re-polls forever)', 'chord.qnt', 'unlock_none', 'progress',
     "keepObs, uq' = uq + 1,\n        used' = { ...used, idlePolls: used.idlePolls + 1 },",
     "keepObs, uq' = uq + 1,\n        used' = used,"),
    ('a redelivery that uses no budget (redelivers forever)', 'chord.qnt', 'redis_dup', 'progress',
     "used' = { ...used, dups: used.dups + 1 },", "used' = used,"),
    ('results: a commit that leaves the run begun (commits forever)', 'resultstate.qnt', 'rtw', 'progress',
     "pc' = pc.set(r, Idle),", "pc' = pc,"),
    # kombu PR 2831
    ('transport: PUT_BACK without the score check', 'redis_transport.qnt', 'pr_credit_timeout', 'noDuplicate',
     'e.tag == p.tag\n                                   and e.score == p.score and not(e.moved))',
     'e.tag == p.tag)'),
    ('transport: the second record keeps the script\'s record (before kombu 848904a)', 'redis_transport.qnt', 'pr_mismatch', 'notTwice',
     'unacked.exclude(mine).union(Set(own))', 'unacked.union(Set(own))'),
    ('transport: a pop that leaves messages puts no wake token back', 'redis_transport.qnt', 'pr_none', 'noMissedWake',
     "wake' = (wake or (scripted(CFG.design) and rest != Set())),", "wake' = wake,"),
    ('transport: a restore pushes no wake token', 'redis_transport.qnt', 'pr_reply_timeout', 'noMissedWake',
     "queue' = queue.union(Set(e.m)),\n      wake' = (wake or scripted(CFG.design)),",
     "queue' = queue.union(Set(e.m)),\n      wake' = wake,"),
    ('transport: a reply is read without credit', 'redis_transport.qnt', 'main_credit', 'prefetchRespected',
     'alive(c), isReply(s.poll), putsBack or credit(c),', 'alive(c), isReply(s.poll),'),
    ('transport: the script pops without recording (main\'s BRPOP)', 'redis_transport.qnt', 'pr_crash', 'neverLost',
     'unacked\' = if (scripted(CFG.design))\n                   unacked.union(Set({ m: m, tag: tag, score: score, by: c,\n                                       moved: false }))\n                 else unacked,',
     "unacked' = unacked,"),
    ('transport: a renewal that compares the score moves another\'s record anyway', 'redis_transport.qnt', 'renewown_held_credit', 'noStranded',
     'CFG.renewal == RenewBlind, alive(c), live(c).held.contains(e.m),',
     'CFG.renewal != NoRenewal, alive(c), live(c).held.contains(e.m),'),
    # celery issue 11036 and 11039
    ('once: fail on the first read (cd8aa46)', 'chord.qnt', 'once_dup', 'noSpuriousError',
     'else if (CFG.rereadMissing) KeysThere', 'else if (false) KeysThere'),
    ('once: complete without the done key', 'chord.qnt', 'once_dup', 'atMostOnce',
     'pc: if (doneKey) HeaderDone else KvComplete', 'pc: if (false) HeaderDone else KvComplete'),
    ('once: count on == size, as main', 'chord.qnt', 'once_dup', 'quietDone',
     'pc: if (p == KvOnce) (if (v >= N) OnceCheck else HeaderDone)',
     'pc: if (p == KvOnce) (if (v == N) OnceCheck else HeaderDone)'),
    ('once: complete without reading the results', 'chord.qnt', 'once_dup', 'noSpuriousError',
     'pc: if (missing == Set()) OnceClaim else OnceKeys(missing) })),',
     'pc: OnceClaim })),'),
    ('once: a late copy fails the chord without the done key', 'chord.qnt', 'once_dup', 'noErrorAfterDispatch',
     'else if (not(groupSaved)) (if (doneKey) ChordDone else GroupGone)',
     'else if (not(groupSaved)) GroupGone'),
    ('once: a lost result never fails the chord', 'chord.qnt', 'once_lose_result', 'quietDone',
     'else if (doneKey) StillLostButClaimed else StillLost', 'else StillLostButClaimed'),
    ('interrupt: fail the chord after the publish returned (5f844ea)', 'chord.qnt', 'redisfix_interrupt', 'noErrorAfterDispatch',
     'or (point == InCleanup and r.pc == RedisPublished(Refused))', 'or point == InCleanup'),
    ('interrupt: leave the size key (5446659)', 'chord.qnt', 'redisfix_interrupt_dup', 'noDispatchAfterError',
     '      | CloseAlways => point != InClaimReply', '      | CloseAlways => false'),
    ('interrupt: leave the size key once the body is sent (ee48301)', 'chord.qnt', 'redisfix_interrupt_dup', 'atMostOnce',
     '      | CloseAlways => point != InClaimReply', '      | CloseAlways => handled'),
    ('interrupt: not handled (768e476)', 'chord.qnt', 'redisfix_interrupt', 'quietDone',
     '      | CloseAlways => untilSent\n', '      | CloseAlways => false\n'),
]


def violated(spec_text: str, instance: str, prop: str, samples: int) -> bool:
    with tempfile.NamedTemporaryFile('w', suffix='.qnt', dir=HERE,
                                     prefix='.mutant_', delete=False) as f:
        f.write(spec_text)
        path = Path(f.name)
    try:
        p = subprocess.run(
            ['quint', 'run', f'--main={instance}', str(path),
             f'--invariant={prop}', '--max-steps=60',
             f'--max-samples={samples}', '--seed=23'],
            cwd=HERE, capture_output=True, text=True)
        return '[violation]' in p.stdout + p.stderr
    finally:
        path.unlink()


def main() -> None:
    caught = 0
    chosen = [m for m in MUTANTS
              if all(word in m[0] + m[1] for word in sys.argv[1:])]
    for name, spec, inst, prop, old, new in chosen:
        text = (HERE / spec).read_text()
        instances = tlc.instances_file(HERE / spec).read_text()
        assert text.count(old) == 1, f'{name}: pattern found {text.count(old)}x'
        base = violated(tlc.single_instance(text, instances, inst), inst,
                        prop, 3000)
        mut = violated(tlc.single_instance(text.replace(old, new), instances,
                                           inst), inst, prop, 3000)
        ok = mut and not base
        if name.startswith('EQUIV'):
            ok = not ok
            label = 'equiv  ' if ok else 'NOT EQUIV'
        else:
            label = 'caught ' if ok else 'MISSED '
        caught += ok
        print(f'{label} {name}: {inst}.{prop} '
              f'(original {"violated" if base else "holds"}, mutant '
              f'{"violated" if mut else "holds"})')
    print(f'{caught}/{len(chosen)} spec mutants behaved as expected')


if __name__ == '__main__':
    main()
