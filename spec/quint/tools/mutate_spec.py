#!/usr/bin/env python3
"""Plant bugs in the spec and check that a property catches each one.

Each mutant is the spec with one rule changed back to what the code did
before a fix (or a plausible slip). The property named must be violated on
an instance where the unmutated spec satisfies it.

    python3 tools/mutate_spec.py
    python3 tools/mutate_spec.py transport      # only mutants that mention a word
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import tlc

HERE = tlc.ROOT

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
     'e.m == p.m and e.tag == p.tag and e.score == p.score\n',
     'e.m == p.m and e.tag == p.tag\n'),
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
    # the worker and the publisher
    ('worker: flush() drops a written job in every design', 'worker.qnt', 'keepwritten_late_shutdown', 'exactlyOnce',
     '| Written => if (d.flush == DropUnaccepted) Dropped else Written', '| Written => Dropped'),
    ('worker: renewing ETA messages also renews prefetched ones', 'worker.qnt', 'reneweta_late_hang', 'quietDone',
     '| RenewScheduled => l.jobs.get(m) == Scheduled',
     '| RenewScheduled => l.jobs.get(m) == Scheduled or l.jobs.get(m) == Received'),
    ('worker: a restore at reconnect of idle tags restores running ones too', 'worker.qnt', 'restoreidle_late_cut', 'noUnavoidableDup',
     '| ReconnectRestoresIdle => l.oldTags.filter(m => l.jobs.get(m) == NoJob)',
     '| ReconnectRestoresIdle => l.oldTags'),
    ('worker: the pop script records nothing when its reply is lost', 'worker.qnt', 'pr_late_partition', 'neverLost',
     "unacked' = if (D.pop == PopScript) unacked.set(m, HeldBy(c)) else unacked,",
     "unacked' = unacked,"),
    ('worker: an ack through a collected channel never stops the worker', 'worker.qnt', 'ackfull_late_cut', 'workerSurvives',
     'if (D.collectedAck == WorkerExits)', 'if (D.collectedAck == StillAcks)'),
    ('publisher: with confirms, the publish returns before the confirm', 'publisher.qnt', 'on_crash', 'neverLost',
     "pub' = pub.set(m, if (CFG.confirms == ConfirmsOn) Waiting else Returned),",
     "pub' = pub.set(m, Returned),"),
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
    # one mutant for each property that had none
    ('once: every run that finds the group gone fails the chord, as main', 'chord.qnt', 'once_lose_group', 'failsOnce',
     'else if (not(groupSaved)) (if (doneKey) ChordDone else GroupGone)',
     'else if (not(groupSaved)) GroupGone'),
    ('transport: an ack that leaves the record', 'redis_transport.qnt', 'pr_none', 'exactlyOnce',
     "unacked' = unacked.filter(e => not(e.m == m and e.tag == OwnTag)),\n    acked' = acked.union(Set(m)),",
     "unacked' = unacked,\n    acked' = acked.union(Set(m)),"),
    ('worker: with the pop token a channel still restores by tag', 'worker.qnt', 'ownrecord_late_etashutdown', 'restoresOwnOnly',
     'tags.filter(m => if (D.restoreBy == ByPop) unacked.get(m) == HeldBy(c)',
     'tags.filter(m => if (false) unacked.get(m) == HeldBy(c)'),
    ('worker: the pop script costs two round trips', 'worker.qnt', 'pr_late_none', 'normalCost',
     'did(Deliver({ c: c, m: m }), POP_COST),', 'did(Deliver({ c: c, m: m }), 2),'),
    ('publisher: a resend that does not wait for the confirm to be overdue', 'publisher.qnt', 'on_none', 'publishedOnce',
     "stalled(m), brokerUp,\n    frames' = frames.union(Set(m)),",
     "pub.get(m) == Waiting, brokerUp,\n    frames' = frames.union(Set(m)),"),
    ('publisher: a write to disk that keeps the copy in memory', 'publisher.qnt', 'off_none', 'exactlyOnce',
     "memory' = memory.set(m, 0),", "memory' = memory,"),
    ('publisher: the queue accepts a frame and keeps nothing', 'publisher.qnt', 'off_none', 'quietDone',
     "memory' = memory.setBy(m, n => n + 1),", "memory' = memory,"),
    ('publisher: reading a confirm costs nothing', 'publisher.qnt', 'on_none', 'normalCost',
     'did(Confirm(m), 1),', 'did(Confirm(m), 0),'),
    # chain.qnt: one mutant per property
    ('chain: the next step is published twice', 'chain.qnt', 'late_none', 'nextOnce',
     "pubs' = pubs.setBy(MB, n => n + 1),", "pubs' = pubs.setBy(MB, n => n + 2),"),
    ('chain: a link that is published twice', 'chain.qnt', 'late_none', 'linkOnce',
     "pubs' = pubs.setBy(ML, n => n + 1),", "pubs' = pubs.setBy(ML, n => n + 2),"),
    ('chain: the next step is published before the body returns', 'chain.qnt', 'late_none', 'nextAfterReturn',
     "returnedOk' = true,", "returnedOk' = returnedOk,"),
    ('chain: the failure path also sends the link', 'chain.qnt', 'late_none', 'notBoth',
     'used.refusals < CFG.max.refusals,', 'true,'),
    ('chain: every failure runs the errback twice', 'chain.qnt', 'late_raises', 'errbackOnce',
     "errbacks' = if (r.m == MA and not(quietFail)) errbacks + 1",
     "errbacks' = if (r.m == MA and not(quietFail)) errbacks + 2"),
    ('chain: any second store of b counts as a change', 'chain.qnt', 'late_dup', 'finalStable',
     'ready(before) and before != after', 'ready(before)'),
    ('chain: a run that starts twice without a fault', 'chain.qnt', 'late_none', 'exactlyOnce',
     'if (late) Q::deliver(m) else Q::take(m),', 'if (late) Q::publish(m) else Q::take(m),'),
    ('chain: encode-first that still sends the link', 'chain.qnt', 'encfirst_unencodable', 'noFailedParentAfterSend',
     'not(CFG.unencodable and CFG.encodeFirst),', 'true,'),
    ('chain: a lost node\'s message is not requeued', 'chain.qnt', 'late_node', 'quietDone',
     '      if (r.acked) Q::dkeep else Q::requeue(r.m),\n      jobs\' = jobs.set(w, Idle), stored\' = stored, keepObs,\n      applied\' = applied, used\' = { ...used, nodes',
     '      Q::dkeep,\n      jobs\' = jobs.set(w, Idle), stored\' = stored, keepObs,\n      applied\' = applied, used\' = { ...used, nodes'),
    # redelivery.qnt: one mutant per property
    ('redelivery: an ack that leaves the record (restored again and again)', 'redelivery.qnt', 'redis_late_long', 'noLoop',
     "      rec' = if (REDIS) NoRec else rec,\n      unacked' = unackedAfter(w, h),",
     "      rec' = rec,\n      unacked' = unackedAfter(w, h),"),
    ('redelivery: dead-lettering that drops', 'redelivery.qnt', 'quorum_dlx_kill', 'noSilentLoss',
     "ended' = if (over) (if (CFG.dlx) DeadLettered else Dropped) else ended,",
     "ended' = if (over) Dropped else ended,"),
    ('redelivery: an early ack that leaves the record', 'redelivery.qnt', 'redis_early_kill', 'earlyAtMostOnce',
     "rec' = if (REDIS and not(LATE)) NoRec else rec,", "rec' = rec,"),
    ('redelivery: a restore that does not mark the copy redelivered', 'redelivery.qnt', 'redis_dedup_long_w1', 'noRerunAfterSuccess',
     "ready' = ready + 1, redeliv' = true, rec' = NoRec,", "ready' = ready + 1, redeliv' = redeliv, rec' = NoRec,"),
    ('redelivery: a delivery that leaves the copy on the queue', 'redelivery.qnt', 'redis_late_short', 'projectionOk',
     "ready' = ready - 1, redeliv' = false,", "ready' = ready, redeliv' = false,"),
    ('redelivery: restore before the visibility timeout', 'redelivery.qnt', 'redis_late_short', 'exactlyOnce',
     'REDIS, ageOf(rec) == T, hasRec(rec),', 'REDIS, hasRec(rec),'),
    ('redelivery: no restore_visible', 'redelivery.qnt', 'redis_late_kill', 'quietDone',
     'REDIS, ageOf(rec) == T, hasRec(rec),', 'false, ageOf(rec) == T, hasRec(rec),'),
]


def violated(where: Path, spec_text: str, instance: str, prop: str,
             samples: int) -> bool:
    with tempfile.NamedTemporaryFile('w', suffix='.qnt', dir=where,
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
        path = tlc.spec_path(spec)
        text = path.read_text()
        instances = tlc.instances_file(path).read_text()
        assert text.count(old) == 1, f'{name}: pattern found {text.count(old)}x'
        base = violated(path.parent, tlc.single_instance(text, instances, inst), inst,
                        prop, 3000)
        mut = violated(path.parent, tlc.single_instance(
            text.replace(old, new), instances, inst), inst, prop, 3000)
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
