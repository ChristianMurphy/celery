// -*- mode: Bluespec; -*-
// Named scenarios behind the findings in README.md, as `quint test` runs.
// Each run is a behavior the spec allows; the final expect states the bad
// outcome. adapter.replay_chord replays the same steps on the real backends
// (run_all.sh writes them out as ITF).
//
//   quint test scenarios.qnt --main=redisLost

module redisLost {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    max: { ...NO_FAULTS, workerLost: 1, childLost: 1 },
  }).* from "./chord"

  // Header 2 counted; header 1 stored and claimed the dispatch, then its
  // worker is lost before publishing. The broker redelivers header 1 at
  // once (RabbitMQ requeues on connection loss), before the claim's TTL
  // runs out: the claim is still held, so the redelivered run does not
  // dispatch. The claim then expires and nothing is left to run.
  run lostClaimHangsTest =
    init
      .then(deliver(W(1), H(2))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2))).then(partReturn(W(2)))
      .expect(lastAction == PartReturn({ w: W(2), h: H(1), outcome: TookClaim }))
      .then(loseWorker(W(2))).expect(queue.get(H(1)) == 1)
      .then(deliver(W(3), H(1))).then(store(W(3))).then(partReturn(W(3)))
      .expect(lastAction
              == PartReturn({ w: W(3), h: H(1), outcome: CompleteNoClaim }))
      .then(finish(W(3)))
      .then(claimExpires)
      .expect(quiet and dispatched == 0 and not(chordError))

  // The pool child of the last header stores SUCCESS and is lost before
  // CHORD_PART_RETURN_LUA runs. The main process sees SUCCESS stored and
  // acks without a part return (celery PR 10752): the chord never ends.
  run childLostAfterStoreHangsTest =
    init
      .then(deliver(W(1), H(2))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2)))
      .then(loseChild(W(2)))
      .expect(lastAction == LoseChild({ w: W(2), outcome: SuccessStored }))
      .expect(quiet and dispatched == 0 and not(chordError))

  // The worker is lost after the body was published and before cleanup;
  // the redelivery arrives after the claim expired (Redis broker:
  // visibility_timeout, 1 hour by default): the body is sent again.
  run lostAfterPublishDispatchesTwiceTest =
    init
      .then(deliver(W(1), H(2))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2))).then(publish(W(2), true))
      .then(loseWorker(W(2))).then(claimExpires)
      .then(deliver(W(3), H(1))).then(store(W(3))).then(partReturn(W(3)))
      .then(redisJoin(W(3))).then(extend(W(3))).then(publish(W(3), true))
      .expect(dispatched == 2)
}

module kvDup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"

  // Header 1 runs twice (redelivery). Both runs incr the counter, so it
  // reaches the chord size while header 2 is still running; the join waits
  // result_chord_join_timeout and fails the chord although no header failed.
  // The Redis backend fixed this in celery PR 10919; incr backends did not.
  run duplicateCountsTwiceTest =
    init
      .then(dup(H(1)))
      .then(deliver(W(1), H(1))).then(store(W(1))).then(count(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2))).then(count(W(2)))
      .expect(lastAction == Count({ w: W(2), h: H(1), outcome: CountComplete })
              and results.get(H(2)) == Pending)
      .then(kvJoin(W(2), false)).expect(chordError and not(anyFailed))

  // A redelivered header that runs after the chord finished finds the
  // GroupResult deleted and fails the chord: chord_error_from_stack stores
  // FAILURE for the body and calls its errbacks, after the body was sent.
  run lateDuplicateFailsDoneChordTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(count(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(count(W(2)))
      .then(kvJoin(W(2), true)).then(publish(W(2), true)).then(kvClean(W(2)))
      .then(finish(W(2)))
      .then(dup(H(1))).then(deliver(W(3), H(1))).then(store(W(3)))
      .then(count(W(3)))
      .expect(lastAction == Count({ w: W(3), h: H(1), outcome: NoGroup })
              and dispatched == 1 and errorAfterDispatch)
}

module unlockChild {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Unlock),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"

  // The pool child running chord_unlock is lost. chord_unlock has
  // ignore_result, so the main process acks it and marks it failed; no retry
  // is published and nothing polls the chord again.
  run unlockLostHangsTest =
    init
      .then(deliverUnlock(W(3))).then(loseChild(W(3)))
      .expect(lastAction == LoseChild({ w: W(3), outcome: ChildAcked }))
      .then(deliver(W(1), H(1))).then(store(W(1))).then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(finish(W(2)))
      .expect(quiet and dispatched == 0 and not(chordError))
}

// celery issue 11036, cd8aa46: _complete_chord_once read the header results,
// then the keys of the missing ones. Header 1 runs twice, so the count is at
// the chord size while header 2 still runs; header 2 stores its result and
// adds its key between the two reads, and looks lost. Replayed on memcached
// by adapter/confirm_once_reread.py.
module onceReadOnce {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    rereadMissing: false,
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"

  run returningDuringTheCheckLooksLostTest =
    init
      .then(dup(H(1)))
      .then(deliver(W(1), H(1))).then(store(W(1))).then(returned(W(1)))
      .then(countKey(W(1))).then(finish(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2))).then(returned(W(2)))
      .then(countKey(W(2)))
      .expect(lastAction == Count({ w: W(2), h: H(1), outcome: CountComplete }))
      .then(readResults(W(2)))
      .expect(lastAction
              == ReadResults({ w: W(2), h: H(1), missing: Set(H(2)) }))
      .then(deliver(W(3), H(2))).then(store(W(3))).then(returned(W(3)))
      .then(checkKeys(W(2)))
      .expect(lastAction == CheckKeys({ w: W(2), h: H(1), outcome: ResultLost }))
      .expect(chordError and allReady and not(anyFailed))
      .then(finish(W(2)))
      .then(countKey(W(3))).then(readResults(W(3))).then(claim(W(3)))
      .expect(lastAction == ClaimChord({ w: W(3), h: H(2), outcome: ClaimTaken }))
      .then(finish(W(3)))
      .expect(quiet and dispatched == 0)
}

// The same steps on 1fa71d4, which reads the missing results again.
module onceReadAgain {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"

  run returningDuringTheCheckCompletesTest =
    init
      .then(dup(H(1)))
      .then(deliver(W(1), H(1))).then(store(W(1))).then(returned(W(1)))
      .then(countKey(W(1))).then(finish(W(1)))
      .then(deliver(W(2), H(1))).then(store(W(2))).then(returned(W(2)))
      .then(countKey(W(2))).then(readResults(W(2)))
      .then(deliver(W(3), H(2))).then(store(W(3))).then(returned(W(3)))
      .then(checkKeys(W(2)))
      .expect(lastAction == CheckKeys({ w: W(2), h: H(1), outcome: KeysThere }))
      .then(reread(W(2)))
      .expect(lastAction == Reread({ w: W(2), h: H(1), outcome: Arrived }))
      .then(claim(W(2))).then(kvJoin(W(2), true)).then(publish(W(2), true))
      .then(kvClean(W(2))).then(finish(W(2)))
      .then(countKey(W(3))).then(finish(W(3)))
      .expect(quiet and dispatched == 1 and not(chordError))
}

// celery issue 11039. One chord, the last header takes the dispatch, and a
// hard time limit lands at a chosen point. Replayed on Redis by
// adapter/confirm_interrupt.py.
module interruptMain {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"

  // 768e476: the publish is interrupted before the body left; the finally
  // writes the done marker; the chord is neither sent nor failed.
  run interruptedPublishMarksDoneAndDropsTheBodyTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2)))
      .then(interrupt(W(2), InPublishUnsent, true))
      .expect(ckey == Done and quiet and dispatched == 0 and not(chordError))
}

module interruptFirstFix {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailWhileDispatching,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"

  // 5f844ea: the body is published, and the interrupt lands in the cleanup
  // call: the chord is failed after its body was sent.
  run interruptInCleanupFailsASentChordTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2))).then(publish(W(2), true))
      .then(interrupt(W(2), InCleanup, false))
      .expect(dispatched == 1 and chordError and errorAfterDispatch)
}

module interruptSecondFix {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailUntilSent,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"

  // 5446659: the same interrupt leaves the sent chord alone.
  run interruptInCleanupLeavesASentChordTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2))).then(publish(W(2), true))
      .then(interrupt(W(2), InCleanup, false))
      .expect(dispatched == 1 and not(chordError))

  // 5446659: the interrupt lands before the publish, the chord is failed,
  // the claim expires, and a later copy of a header task sends the body.
  run laterCopySendsTheBodyOfAFailedChordTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2)))
      .then(interrupt(W(2), InExtend, true))
      .expect(chordError and dispatched == 0 and skey)
      .then(claimExpires)
      .then(dup(H(1))).then(deliver(W(3), H(1))).then(store(W(3)))
      .then(partReturn(W(3)))
      .expect(lastAction == PartReturn({ w: W(3), h: H(1), outcome: TookClaim }))
      .then(redisJoin(W(3))).then(extend(W(3))).then(publish(W(3), true))
      .expect(dispatched == 1 and dispatchAfterError)
}

module interruptThirdFix {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailAndClose,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"

  // ee48301: the handler drops the size key, so the later copy only counts.
  run laterCopyFindsTheFailedChordClosedTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2)))
      .then(interrupt(W(2), InExtend, true))
      .expect(chordError and dispatched == 0 and not(skey))
      .then(claimExpires)
      .then(dup(H(1))).then(deliver(W(3), H(1))).then(store(W(3)))
      .then(partReturn(W(3)))
      .expect(lastAction == PartReturn({ w: W(3), h: H(1), outcome: Counted }))
      .then(finish(W(3)))
      .expect(quiet and dispatched == 0 and errors == 1)

  // ee48301, as 768e476: the body is published, the interrupt lands in the
  // cleanup call before it ran, the claim expires, and a later copy of a
  // header task sends the body again.
  run laterCopySendsTheBodyTwiceTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2))).then(publish(W(2), true))
      .then(interrupt(W(2), InCleanup, false))
      .expect(dispatched == 1 and not(chordError) and skey)
      .then(claimExpires)
      .then(dup(H(1))).then(deliver(W(3), H(1))).then(store(W(3)))
      .then(partReturn(W(3))).then(redisJoin(W(3))).then(extend(W(3)))
      .then(publish(W(3), true))
      .expect(dispatched == 2)
}

module interruptFix {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: CloseAlways,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"

  // ad00b4a: the handler drops the size key after the publish too, so the
  // later copy only counts, and the chord is not failed.
  run laterCopyFindsTheSentChordClosedTest =
    init
      .then(deliver(W(1), H(1))).then(store(W(1))).then(partReturn(W(1)))
      .then(finish(W(1)))
      .then(deliver(W(2), H(2))).then(store(W(2))).then(partReturn(W(2)))
      .then(redisJoin(W(2))).then(extend(W(2))).then(publish(W(2), true))
      .then(interrupt(W(2), InCleanup, false))
      .expect(dispatched == 1 and not(chordError) and not(skey))
      .then(claimExpires)
      .then(dup(H(1))).then(deliver(W(3), H(1))).then(store(W(3)))
      .then(partReturn(W(3)))
      .expect(lastAction == PartReturn({ w: W(3), h: H(1), outcome: Counted }))
      .then(finish(W(3)))
      .expect(quiet and dispatched == 1 and not(chordError))
}
