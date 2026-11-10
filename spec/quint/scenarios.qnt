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
