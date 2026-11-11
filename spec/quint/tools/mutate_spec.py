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

# (name, spec, instance, property, old, new[, samples])
MUTANTS = [
    ('mongo filter without $ne SUCCESS', 'resultstate.qnt', 'mongo', 'successStable',
     'if (curStatus == Success) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>',
     'if (false) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>'),
    ('sql without the locked re-read', 'resultstate.qnt', 'sql', 'successStable',
     'else if (curStatus == Success) (cur, Skipped) // :186-190',
     'else if (read == Success) (cur, Skipped) // :186-190'),
    ('results: a store that keeps every ready state, broken', 'resultstate.qnt', 'mongofinal_rev', 'finalStable',
     '      or (finalSticky and isReady(curStatus) and curStatus != w.status)', '      or false'),
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
    ('redelivery: a failed read with always_retry that still fails the task', 'redelivery.qnt', 'redis_dedupretry_readerr', 'noFailWithoutError',
     "stored' = if (CFG.alwaysRetry or stored == Success or not(stores)) stored", "stored' = if (stored == Success or not(stores)) stored"),
    ('redelivery: deduplication that ignores a stored SUCCESS', 'redelivery.qnt', 'redis_dedup_kill', 'noRerunAfterDone',
     'val dedup = CFG.dedup and LATE and h.redelivered and stored == Success\n', 'val dedup = false\n'),
    ('redelivery: a killed AMQP worker whose deliveries are not returned', 'redelivery.qnt', 'classic_late_kill', 'outcomeVisible',
     "    } else returnAll(w, gen.get(w)),", "    } else all { published' = published, ready' = ready, redeliv' = redeliv, rec' = rec, unacked' = unacked, returns' = returns, ended' = ended },"),
    # retry.qnt: one mutant per property
    ('retry: a retry that publishes the same attempt again', 'retry.qnt', 'retryfirst_none', 'runsBounded',
     'Q::publish(M(job(w).att + 1)),', 'Q::publish(M(job(w).att)),'),
    ('retry: a delivery counted twice', 'retry.qnt', 'retryfirst_fail', 'runsBoundedAlways',
     "bodies' = bodies + 1,", "bodies' = bodies + 2,"),
    ('retry: an ack that requeues', 'retry.qnt', 'retryfirst_none', 'noRetryAfterSuccess',
     'Q::ack(M(job(w).att)),', 'Q::requeue(M(job(w).att)),'),
    ('retry: max retries exceeded that stores nothing', 'retry.qnt', 'retryfirst_fail', 'neverLost',
     "stored' = if (atMax) storing(r.att, Failure)", "stored' = if (atMax) stored"),
    ('retry: a lost node\'s message is not requeued', 'retry.qnt', 'retryfirst_node', 'quietDone',
     "Q::requeue(M(job(w).att)),\n    jobs' = jobs.set(w, Idle)", "Q::dkeep,\n    jobs' = jobs.set(w, Idle)"),
    ('retry: a fenced store that ignores the attempt', 'retry.qnt', 'fenced_ok_none', 'noStaleWrite',
     'if (stored.res == Success or (CFG.fenced and (att < stored.att or down)))',
     'if (stored.res == Success or (CFG.fenced and down))'),
    # revoke.qnt: one mutant per property
    ('revoke: on_accepted check missing in the design', 'revoke.qnt', 'checkaccept_revoke', 'noRunAfterRevokeKnown',
     'not(CFG.checkOnAccept and discards(w, r.m)),', 'true,'),
    ('revoke: a check that ignores expires', 'revoke.qnt', 'checkaccept_expire', 'noRunAfterExpiry',
     'm == M(1) and ((CFG.expires and expired) or lv(w).revoked)', 'm == M(1) and lv(w).revoked'),
    ('revoke: a broadcast that skips worker 2', 'revoke.qnt', 'late_revoke', 'noRunMissingRevoke',
     "    CFG.revoke, not(revokeSent), applied.contains(M(1)),\n    revokeSent' = true, applied' = applied, expired' = expired,\n    workers' = WORKERS.mapBy(w => match workers.get(w) {\n      | Down => Down\n      | Up(l) => Up({ ...l, inbox: l.inbox or\n",
     "    CFG.revoke, not(revokeSent), applied.contains(M(1)),\n    revokeSent' = true, applied' = applied, expired' = expired,\n    workers' = WORKERS.mapBy(w => match workers.get(w) {\n      | Down => Down\n      | Up(l) => Up({ ...l, inbox: l.inbox or w == W(2) and\n"),
    ('revoke: an exit that does not save the state db', 'revoke.qnt', 'statedb_halt', 'revokeSurvivesRestart',
     "saved.set(w, l.revoked) else saved,", "saved else saved,"),
    ('revoke: a connection loss that does not cancel', 'revoke.qnt', 'cancel_cut_s2', 'noParallelRuns',
     '.filter(r => not(cancel and late and r.stage == Running))', '.filter(r => true)', 30000),
    ('revoke: a store that replaces a final state', 'revoke.qnt', 'sticky_checkaccept_revoke', 'finalStable',
     'if (cur == Success or (CFG.finalSticky and final(cur))) cur else r', 'if (cur == Success) cur else r'),
    ('revoke: a command that stays in the inbox', 'revoke.qnt', 'late_revoke', 'ctlOnce',
     "workers' = setLive(w, { ...l, inbox: false, revoked: true,", "workers' = setLive(w, { ...l, revoked: true,"),
    ('revoke: an ack that requeues', 'revoke.qnt', 'late_halt', 'noRerunAfterSuccess',
     "  action settle(r: Req): bool = if (r.stale) Q::dkeep else Q::ack(r.m)", "  action settle(r: Req): bool = if (r.stale) Q::dkeep else Q::requeue(r.m)"),
    ('revoke: a cold shutdown that does not put back a late-ack run', 'revoke.qnt', 'late_halt', 'outcomeVisible',
     "    val back = if (LATE) cancelled.filter(r => not(r.stale)) else Set()", "    val back = if (LATE) Set() else Set()"),
    ('revoke: an ack that settles nothing', 'revoke.qnt', 'late_revoke', 'quietDone',
     "  action settle(r: Req): bool = if (r.stale) Q::dkeep else Q::ack(r.m)", "  action settle(r: Req): bool = Q::dkeep"),
    # reader.qnt: one mutant per property
    ('reader: KeepReady that keeps SUCCESS only', 'reader.qnt', 'keepready_late', 'readersAgree',
     '      | KeepReady => isReady(cur)', '      | KeepReady => cur == Success'),
    ('reader: an expired key read as SUCCESS', 'reader.qnt', 'poll_expire', 'answerWasWritten',
     '| Absent => Pending | Present(x) => x | Expired => Pending', '| Absent => Pending | Present(x) => x | Expired => Success'),
    ('reader: a restore that looks for the wrong key', 'reader.qnt', 'poll_none', 'restoreFinds',
     'val found = gkey == Saved', 'val found = gkey == NoGroup'),
    ('reader: the first read before the subscribe', 'reader.qnt', 'pubsub_none', 'quietDone',
     "CFG.mode == PubSub, joining(r), view(r, t).sub, not(view(r, t).primed),",
     "CFG.mode == PubSub, joining(r), not(view(r, t).primed),"),
    # beat.qnt: one mutant per property
    ('beat: a send that does not reserve the entry', 'beat.qnt', 'sync1_cron_kill', 'noDoubleSend',
     "procs' = procs.set(b, Up({ last: wall,", "procs' = procs.set(b, Up({ last: u.last,"),
    ('beat: a tick that waits one more step', 'beat.qnt', 'cron_none', 'noMissedRun',
     "      | Interval => wall - procOf(p).last >= R\n      | Cron => nextOcc(procOf(p).last) <= wall",
     "      | Interval => wall - procOf(p).last >= R\n      | Cron => nextOcc(procOf(p).last) + R <= wall"),
    ('beat: a new entry that starts two periods ahead', 'beat.qnt', 'cron_none', 'quietDone',
     "last: if (saved.get(b) < 0) wall else saved.get(b), lastSync: -1 })),",
     "last: if (saved.get(b) < 0) wall + 2 * R else saved.get(b), lastSync: -1 })),"),
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
    for name, spec, inst, prop, old, new, *more in chosen:
        # a seventh field: simulator samples, for a violation that needs
        # a rare interleaving
        samples = more[0] if more else 3000
        path = tlc.spec_path(spec)
        text = path.read_text()
        instances = tlc.instances_file(path).read_text()
        assert text.count(old) == 1, f'{name}: pattern found {text.count(old)}x'
        base = violated(path.parent, tlc.single_instance(text, instances, inst), inst,
                        prop, samples)
        mut = violated(path.parent, tlc.single_instance(
            text.replace(old, new), instances, inst), inst, prop, samples)
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
