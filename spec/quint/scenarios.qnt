// -*- mode: Bluespec; -*-
// Named scenarios behind the findings in README.md, as `quint test` runs.
// Each run is a behavior the spec allows; the final expect states the bad
// outcome. adapter.replay_chord replays the same steps on the real backends
// (python -m adapter.scenarios writes them out as ITF).
//
//   quint test scenarios.qnt --main=scenarios

module redisLost {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false,
               LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1,
               MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).* from "./chord"

  // Header 2 counted; header 1 stored and claimed the dispatch, then its
  // worker is lost before publishing. The broker redelivers header 1 at
  // once (RabbitMQ requeues on connection loss), before the claim's TTL
  // runs out: the claim is still held, so the redelivered run does not
  // dispatch. The claim then expires and nothing is left to run.
  run lostClaimHangsTest =
    init
      .then(deliver(1, 2)).then(store(1)).then(partReturn(1)).then(finish(1))
      .then(deliver(2, 1)).then(store(2)).then(partReturn(2))
      .expect(lastAction.outcome == "claimed")
      .then(loseWorker(2)).expect(queue.get(1) == 1)
      .then(deliver(3, 1)).then(store(3)).then(partReturn(3))
      .expect(lastAction.outcome == "complete")
      .then(finish(3))
      .then(claimExpires)
      .expect(quiet and dispatched == 0 and not(chordError))

  // The pool child of the last header stores SUCCESS and is lost before
  // CHORD_PART_RETURN_LUA runs. The main process sees SUCCESS stored and
  // acks without a part return (celery PR 10752): the chord never ends.
  run childLostAfterStoreHangsTest =
    init
      .then(deliver(1, 2)).then(store(1)).then(partReturn(1)).then(finish(1))
      .then(deliver(2, 1)).then(store(2))
      .then(loseChild(2)).expect(lastAction.outcome == "success_stored")
      .expect(quiet and dispatched == 0 and not(chordError))

  // The worker is lost after the body was published and before cleanup;
  // the redelivery arrives after the claim expired (Redis broker:
  // visibility_timeout, 1 hour by default): the body is sent again.
  run lostAfterPublishDispatchesTwiceTest =
    init
      .then(deliver(1, 2)).then(store(1)).then(partReturn(1)).then(finish(1))
      .then(deliver(2, 1)).then(store(2)).then(partReturn(2))
      .then(redisJoin(2)).then(extend(2)).then(publish(2, true))
      .then(loseWorker(2)).then(claimExpires)
      .then(deliver(3, 1)).then(store(3)).then(partReturn(3))
      .then(redisJoin(3)).then(extend(3)).then(publish(3, true))
      .expect(dispatched == 2)
}

module kvDup {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false,
               LEASE_SAFE = true, MAX_DUPS = 1, MAX_WORKER_LOST = 0,
               MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).* from "./chord"

  // Header 1 runs twice (redelivery). Both runs incr the counter, so it
  // reaches the chord size while header 2 is still running; the join waits
  // result_chord_join_timeout and fails the chord although no header failed.
  // The Redis backend fixed this in celery PR 10919; incr backends did not.
  run duplicateCountsTwiceTest =
    init
      .then(dup(1))
      .then(deliver(1, 1)).then(store(1)).then(count(1))
      .then(deliver(2, 1)).then(store(2)).then(count(2))
      .expect(lastAction.outcome == "complete" and results.get(2) == "PENDING")
      .then(kvJoin(2, false)).expect(chordError and not(anyFailed))

  // A redelivered header that runs after the chord finished finds the
  // GroupResult deleted and fails the chord: chord_error_from_stack stores
  // FAILURE for the body and calls its errbacks, after the body was sent.
  run lateDuplicateFailsDoneChordTest =
    init
      .then(deliver(1, 1)).then(store(1)).then(count(1)).then(finish(1))
      .then(deliver(2, 2)).then(store(2)).then(count(2))
      .then(kvJoin(2, true)).then(publish(2, true)).then(kvClean(2)).then(finish(2))
      .then(dup(1)).then(deliver(3, 1)).then(store(3)).then(count(3))
      .expect(lastAction.outcome == "nogroup" and dispatched == 1 and errorAfterDispatch)
}

module unlockChild {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false,
               LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0,
               MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).* from "./chord"

  // The pool child running chord_unlock is lost. chord_unlock has
  // ignore_result, so the main process acks it and marks it failed; no retry
  // is published and nothing polls the chord again.
  run unlockLostHangsTest =
    init
      .then(deliverUnlock(3)).then(loseChild(3)).expect(lastAction.outcome == "acked")
      .then(deliver(1, 1)).then(store(1)).then(finish(1))
      .then(deliver(2, 2)).then(store(2)).then(finish(2))
      .expect(quiet and dispatched == 0 and not(chordError))
}

module scenarios {
  import redisLost as R from "./scenarios"
  import kvDup as K from "./scenarios"
  import unlockChild as U from "./scenarios"
}
