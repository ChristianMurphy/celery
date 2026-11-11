// -*- mode: Bluespec; -*-
// Chord completion: when is the body dispatched, and how often?
//
// Every rule cites celery at 768e476 (file:line) unless it names another
// commit. The protocols, picked by CFG.proto:
//
//   Redis     RedisBackend.on_chord_part_return with the Lua scripts
//             CHORD_PART_RETURN_LUA, CHORD_CLAIM_EXTEND_LUA and
//             CHORD_CLEANUP_LUA (redis.py:96-185, 934-1069), after
//             celery PR 10919 and PR 10998.
//   Kv        BaseKeyValueStoreBackend.on_chord_part_return: a counter
//             with incr (base.py:1429-1503); memcached, dynamodb, gcs.
//   KvSkip    an intermediate fix for celery issue 11036 (09e5a59,
//             base.py:1450-1488): a key per header task, added before the
//             count; a run that finds its key there does not count.
//   KvOnce    the fix for celery issue 11036 (1fa71d4, base.py:1246-1310,
//             1498-1585): every run counts; the counter only triggers a
//             check that every header result is stored; one done key per
//             chord, added if absent, decides who completes or fails it;
//             a key per header task that returned tells a lost result
//             from one still to come. CFG.rereadMissing = false is the
//             version before 1fa71d4 (cd8aa46), which read the missing
//             results once.
//   Unlock    the celery.chord_unlock task polling the header results
//             (builtins.py:47-130); database backend and every backend
//             without incr (base.py:1069-1072, 1019-1067).
//
// A chord with header tasks HEADERS. Each header task message can be
// delivered more than once (broker redelivery, max.dups), worker processes
// can be lost at any step (whole worker: max.workerLost; pool child with the
// main process handling WorkerLostError: max.childLost), and the broker can
// refuse a publish (max.refusals). Keys can expire or be evicted
// (max.evictions, of the keys in CFG.evictable), and a hard time limit or a
// cold shutdown can interrupt the dispatch (max.interrupts, at the points in
// CFG.interruptAt; celery issue 11039, fix at ad00b4a in redis.py:979-1097).
// CFG.oldExecs are processes of a worker that still runs Kv while the rest
// run CFG.proto (a rolling upgrade).
//
// Types: protocols, results, program counters, the claim key and the last
// action are sum types; header, exec slot and token ids are distinct record
// types. A state the code cannot be in (a chord_unlock run holding a claim
// token, a join outcome on a count action, a claim that is both "done" and
// owned) has no value. The types live in their own module because an
// instance passes constructors (proto: Redis), which must be in scope where
// the instance is made.
module chordtypes {
  type Proto = Redis | Kv | KvSkip | KvOnce | Unlock
  // The protocols that count part returns with incr.
  pure def counts(p: Proto): bool = match p {
    | Redis => false | Kv => true | KvSkip => true | KvOnce => true
    | Unlock => false
  }

  // Ids are one-field records with different field names, so one kind of id
  // does not typecheck where another is expected. (A one-variant sum type
  // would read the same, but Quint 0.33.1 gives it an open row type that
  // Apalache and TLC reject.)
  type HeaderId = { h: int }      // header index (1-based); group_index = h - 1
  type ExecId = { w: int }        // a process slot running one delivery
  type Token = { tok: int }       // claim token (redis), unique per delivery
  pure def H(i: int): HeaderId = { h: i }
  pure def W(i: int): ExecId = { w: i }
  pure def Tok(i: int): Token = { tok: i }

  // A header result in the result backend (keep the first SUCCESS).
  // Evicted: it was stored, then expired or was evicted. The backend reads
  // it as PENDING again; the spec keeps the two apart to tell a body sent
  // before a header finished from one sent after its result went.
  type Result = Pending | Success | Failure | Evicted
  pure def isMissing(r: Result): bool = match r {
    | Pending => true | Success => false | Failure => false | Evicted => true
  }
  // What one run reports for its header.
  type Report = Succeeded | Failed
  // <gid>.c (redis.py:948): absent, a claim token, or the "done" marker.
  type Claim = NoClaim | Claimed(Token) | Done
  // An entry of <gid>.j: the result one run added for a header.
  type Part = { h: HeaderId, res: Report, tok: Token }
  // What the body would get: the header indexes, and how many results.
  type Args = { args: Set[HeaderId], nargs: int }

  // Where a run of a header task is.
  type HeaderPc =
    | Running                 // the task body runs
    | Stored(Report)          // result stored, part return not run yet
    | RedisClaimed            // CHORD_PART_RETURN_LUA gave this run the claim
    | RedisJoined(Args)       // read <gid>.j
    | RedisExtended(Args)     // CHORD_CLAIM_EXTEND_LUA kept the claim
    | RedisPublished(PublishOutcome)  // publish returned, cleanup not run
    | KeyAdded                // KvSkip, KvOnce: the header's key is added
    | OnceCheck               // KvOnce: the count is at the chord size
    | OnceKeys(Set[HeaderId]) // KvOnce: these results were missing
    | OnceReread(Set[HeaderId]) // KvOnce: and their keys are all there
    | OnceClaim               // KvOnce: every result is stored
    | KvComplete              // this run completes the chord: join next
    | KvJoined(Args)          // join_native returned
    | KvClean                 // before deps.delete() / delete(key)
    | HeaderDone              // on_chord_part_return returned
  // Where a run of chord_unlock is.
  type UnlockPc = Polling | UnlockJoined(Args) | UnlockDone

  type HeaderRun = {
    h: HeaderId,
    tok: Token,
    acked: bool,      // message acknowledged
    main: bool,       // running in the worker's main process (WorkerLostError path)
    pc: HeaderPc,
  }
  type UnlockRun = { acked: bool, pc: UnlockPc }
  // A worker process: free, or running one delivery of a message.
  type Exec = Free | HeaderTask(HeaderRun) | UnlockTask(UnlockRun)

  // ---- reading an Exec. Matches are exhaustive on purpose: a new variant
  // must be placed in each of them. The xxxOf readers return a filler for
  // the other variants and are only used under the matching isXxx guard.
  pure def isFree(e: Exec): bool = match e {
    | Free => true | HeaderTask(_) => false | UnlockTask(_) => false
  }
  pure def isHeader(e: Exec): bool = match e {
    | Free => false | HeaderTask(_) => true | UnlockTask(_) => false
  }
  pure def isUnlock(e: Exec): bool = match e {
    | Free => false | HeaderTask(_) => false | UnlockTask(_) => true
  }
  pure val NO_HEADER: HeaderRun =
    { h: H(0), tok: Tok(0), acked: true, main: false, pc: HeaderDone }
  pure val NO_UNLOCK: UnlockRun = { acked: true, pc: UnlockDone }
  pure def headerOf(e: Exec): HeaderRun = match e {
    | Free => NO_HEADER | HeaderTask(r) => r | UnlockTask(_) => NO_HEADER
  }
  pure def unlockOf(e: Exec): UnlockRun = match e {
    | Free => NO_UNLOCK | HeaderTask(_) => NO_UNLOCK | UnlockTask(u) => u
  }
  pure def isAcked(e: Exec): bool = match e {
    | Free => true | HeaderTask(r) => r.acked | UnlockTask(u) => u.acked
  }

  pure def isStored(pc: HeaderPc): bool = match pc {
    | Stored(_) => true
    | Running => false | RedisClaimed => false | RedisJoined(_) => false
    | RedisExtended(_) => false | RedisPublished(_) => false
    | KeyAdded => false | OnceCheck => false | OnceKeys(_) => false
    | OnceReread(_) => false | OnceClaim => false | KvComplete => false
    | KvJoined(_) => false | KvClean => false | HeaderDone => false
  }
  pure def reportOf(pc: HeaderPc): Report = match pc {
    | Stored(r) => r
    | Running => Failed | RedisClaimed => Failed | RedisJoined(_) => Failed
    | RedisExtended(_) => Failed | RedisPublished(_) => Failed
    | KeyAdded => Failed | OnceCheck => Failed | OnceKeys(_) => Failed
    | OnceReread(_) => Failed | OnceClaim => Failed | KvComplete => Failed
    | KvJoined(_) => Failed | KvClean => Failed | HeaderDone => Failed
  }
  pure val NO_ARGS: Args = { args: Set(), nargs: 0 }
  // The results a run joined, where it has joined.
  pure def joinedOf(pc: HeaderPc): Args = match pc {
    | RedisJoined(a) => a | RedisExtended(a) => a | KvJoined(a) => a
    | Running => NO_ARGS | Stored(_) => NO_ARGS | RedisClaimed => NO_ARGS
    | RedisPublished(_) => NO_ARGS | KeyAdded => NO_ARGS | OnceCheck => NO_ARGS
    | OnceKeys(_) => NO_ARGS | OnceReread(_) => NO_ARGS | OnceClaim => NO_ARGS
    | KvComplete => NO_ARGS | KvClean => NO_ARGS | HeaderDone => NO_ARGS
  }
  // The header results a run found missing, where it is checking them.
  pure def missingOf(pc: HeaderPc): Set[HeaderId] = match pc {
    | OnceKeys(m) => m | OnceReread(m) => m
    | Running => Set() | Stored(_) => Set() | RedisClaimed => Set()
    | RedisJoined(_) => Set() | RedisExtended(_) => Set()
    | RedisPublished(_) => Set() | KeyAdded => Set() | OnceCheck => Set()
    | OnceClaim => Set() | KvComplete => Set() | KvJoined(_) => Set()
    | KvClean => Set() | HeaderDone => Set()
  }
  // One name per place, to guard on a place whatever it carries.
  type At = AtRunning | AtStored | AtRedisClaimed | AtRedisJoined
          | AtRedisExtended | AtRedisPublished | AtKeyAdded | AtOnceCheck
          | AtOnceKeys | AtOnceReread | AtOnceClaim | AtKvComplete
          | AtKvJoined | AtKvClean | AtHeaderDone
  pure def at(pc: HeaderPc): At = match pc {
    | Running => AtRunning | Stored(_) => AtStored
    | RedisClaimed => AtRedisClaimed | RedisJoined(_) => AtRedisJoined
    | RedisExtended(_) => AtRedisExtended
    | RedisPublished(_) => AtRedisPublished
    | KeyAdded => AtKeyAdded | OnceCheck => AtOnceCheck
    | OnceKeys(_) => AtOnceKeys | OnceReread(_) => AtOnceReread
    | OnceClaim => AtOnceClaim | KvComplete => AtKvComplete
    | KvJoined(_) => AtKvJoined | KvClean => AtKvClean
    | HeaderDone => AtHeaderDone
  }
  // A run that is about to publish the body, and what it would send.
  pure def isPublishing(e: Exec): bool = match e {
    | Free => false
    | HeaderTask(r) => at(r.pc) == AtRedisExtended or at(r.pc) == AtKvJoined
    | UnlockTask(u) => match u.pc {
        | UnlockJoined(_) => true | Polling => false | UnlockDone => false
      }
  }
  pure def publishingOf(e: Exec): Args = match e {
    | Free => NO_ARGS
    | HeaderTask(r) => joinedOf(r.pc)
    | UnlockTask(u) => match u.pc {
        | UnlockJoined(a) => a | Polling => NO_ARGS | UnlockDone => NO_ARGS
      }
  }
  pure def isFinishing(e: Exec): bool = match e {
    | Free => false
    | HeaderTask(r) => r.pc == HeaderDone
    | UnlockTask(u) => u.pc == UnlockDone
  }
  pure def isClaimed(c: Claim): bool = match c {
    | NoClaim => false | Claimed(_) => true | Done => false
  }

  // ---- outcomes, one type per action that has more than one ----
  type PartReturnOutcome = SawDone | TookClaim | CompleteNoClaim | Counted
  type JoinOutcome = JoinOk | JoinError
  type ExtendOutcome = Extended | Reacquired | ClaimLost
  type PublishOutcome = Sent | Refused
  type CleanupOutcome = Cleaned | NotOwner
  type CountOutcome = NoGroup | CountedOne | CountComplete | CountOver
  // KvSkip, KvOnce: the step that adds the header's key.
  type ReturnedOutcome =
    | KeyNew | KeyThere       // added, or already there: go on to count
    | AlreadyRecorded         // KvSkip: the key is there, so do not count
    | GroupGone               // no group and nobody claimed: fail the chord
    | ChordDone               // KvOnce: the chord is claimed, return
    | IgnoresResult           // KvOnce: a header that ignores its result
  // KvOnce: the checks once the count is at the chord size.
  type CheckKeysOutcome =
    | StillRunning            // a missing header has no key: wait for it
    | KeysThere               // every missing header returned: read again
    | ResultLost              // cd8aa46: a header returned, no result: fail
    | LostButClaimed          // the same, but the chord is claimed already
  type RereadOutcome =
    | Arrived                 // the second read finds every result
    | StillLost               // a result is still missing: fail the chord
    | StillLostButClaimed     // the same, but the chord is claimed already
  type ClaimOutcome = ClaimWon | ClaimTaken
  // A chord key that can expire or be evicted.
  type Key = ResultKey(HeaderId) | PartKey(HeaderId) | DoneKey | CounterKey
           | GroupKey
  // Where a hard time limit or a cold shutdown (SystemExit) lands in
  // RedisBackend.on_chord_part_return, for the run that holds the claim.
  type InterruptPoint =
    | InClaimReply            // the claim script ran, its reply is on the way
    | InJoin                  // reading the results (redis.py:987-1017)
    | InExtend                // before or in the extend script (:1018-1031)
    | InPublishUnsent         // in _retry_publish, the body not yet sent
    | InPublishSent           // in _retry_publish, the body already sent
    | InCleanup               // in the cleanup script, in the finally
  // What on_chord_part_return does with the interrupt.
  type InterruptHandling =
    | Unhandled               // 768e476: nothing
    | FailWhileDispatching    // 5f844ea: fail the chord from the claim on
    | FailUntilSent           // 5446659: the same, until the publish returned
    | FailAndClose            // ee48301: and drop the chord size key
    | CloseAlways             // ad00b4a: drop it after the publish too
  type PollOutcome = PollError | PollReady | PollRetry | PollRefused
  type LoseWorkerOutcome = MessageLost | MessageRequeued
  type LoseChildOutcome = SuccessStored | ChildRequeued | MarkedFailure
                        | ChildAcked

  // The action that produced a state, read by the replay adapter.
  type Action =
    | Init
    | Deliver({ w: ExecId, h: HeaderId })
    | DeliverUnlock(ExecId)
    | Store({ w: ExecId, h: HeaderId })
    | PartReturn({ w: ExecId, h: HeaderId, outcome: PartReturnOutcome })
    | Join({ w: ExecId, h: HeaderId, outcome: JoinOutcome })
    | Extend({ w: ExecId, h: HeaderId, outcome: ExtendOutcome })
    | Publish({ w: ExecId, outcome: PublishOutcome })
    | Cleanup({ w: ExecId, h: HeaderId, outcome: CleanupOutcome })
    | Count({ w: ExecId, h: HeaderId, outcome: CountOutcome })
    | Returned({ w: ExecId, h: HeaderId, outcome: ReturnedOutcome })
    | ReadResults({ w: ExecId, h: HeaderId, missing: Set[HeaderId] })
    | CheckKeys({ w: ExecId, h: HeaderId, outcome: CheckKeysOutcome })
    | Reread({ w: ExecId, h: HeaderId, outcome: RereadOutcome })
    | ClaimChord({ w: ExecId, h: HeaderId, outcome: ClaimOutcome })
    | Evict(Key)
    | Interrupt({ w: ExecId, point: InterruptPoint, handled: bool })
    | Poll({ w: ExecId, outcome: PollOutcome })
    | Finish(ExecId)
    | Dup(HeaderId)
    | LoseWorker({ w: ExecId, outcome: LoseWorkerOutcome })
    | LoseChild({ w: ExecId, outcome: LoseChildOutcome })
    | ClaimExpires
    | DoneExpires

  // ---- configuration of an instance ----
  // Budgets, and how much of each was used: the four fault classes, and the
  // polls of chord_unlock that find the header unfinished and retry. The
  // real task polls for as long as it takes; a budget makes every run
  // finite, and a poll that changes nothing adds no behavior beyond the
  // first (see "quiet" below).
  type Faults = {
    dups: int, workerLost: int, childLost: int, refusals: int, idlePolls: int,
    evictions: int, interrupts: int,
  }
  // Where a whole worker can be lost.
  type KillWindow =
    | Anywhere
    | BeforeCount   // between the header's result (or its key) and its count
    | AfterClaim    // between taking the dispatch and the publish
  type Config = {
    proto: Proto,
    acksLate: bool,
    rejectOnWorkerLost: bool,
    // The claim lease (redis) never expires while its holder is alive.
    // false = a holder can stall past the lease (GC pause, slow join with
    // result_chord_join_timeout=None, see the comment at redis.py:953-961).
    leaseSafe: bool,
    // KvOnce: read the missing results again before failing (1fa71d4).
    rereadMissing: bool,
    // Header tasks with ignore_result=True.
    ignoreResult: Set[HeaderId],
    // Processes of a worker that still runs Kv (rolling upgrade).
    oldExecs: Set[ExecId],
    killWindow: KillWindow,
    evictable: Set[Key],
    interruptAt: Set[InterruptPoint],
    onInterrupt: InterruptHandling,
    max: Faults,
  }
  pure val NO_FAULTS: Faults = {
    dups: 0, workerLost: 0, childLost: 0, refusals: 0, idlePolls: 2,
    evictions: 0, interrupts: 0,
  }
  pure val NONE_USED: Faults = {
    dups: 0, workerLost: 0, childLost: 0, refusals: 0, idlePolls: 0,
    evictions: 0, interrupts: 0,
  }

  // ---- progress measure (variant): what a state can still do, in steps.
  // Every action lowers it by at least one (checked as the invariant
  // "progress"), so no run loops, redelivery and re-polling included.
  pure def headerRank(pc: HeaderPc): int = match pc {
    | Running => 11 | Stored(_) => 10
    | RedisClaimed => 7 | RedisJoined(_) => 6 | RedisExtended(_) => 3
    | RedisPublished(_) => 2
    | KeyAdded => 9 | OnceCheck => 8 | OnceKeys(_) => 7 | OnceReread(_) => 6
    | OnceClaim => 5
    | KvComplete => 4 | KvJoined(_) => 3 | KvClean => 2
    | HeaderDone => 1
  }
  pure def unlockRank(pc: UnlockPc): int = match pc {
    | Polling => 3 | UnlockJoined(_) => 2 | UnlockDone => 1
  }
  pure def execRank(e: Exec): int = match e {
    | Free => 0
    | HeaderTask(r) => headerRank(r.pc)
    | UnlockTask(u) => unlockRank(u.pc)
  }
  // Taking the claim costs two steps of slack (it can expire, or become the
  // done marker, which expires too), so the pcs around it are 3 apart.
  pure def claimRank(c: Claim): int = match c {
    | NoClaim => 0 | Claimed(_) => 2 | Done => 1
  }
  pure val QUEUED_HEADER: int = 12    // deliver, then Running
  pure val QUEUED_UNLOCK: int = 4     // deliver, then Polling
  pure val FAULT: int = 13            // a fault can requeue one message
  pure val IDLE_POLL: int = 5         // a retry queues one chord_unlock
  // acks_late, no reject_on_worker_lost, a safe lease, no faults.
  pure def lateAck(p: Proto): Config = {
    proto: p, acksLate: true, rejectOnWorkerLost: false, leaseSafe: true,
    rereadMissing: true, ignoreResult: Set(), oldExecs: Set(),
    killWindow: Anywhere, evictable: Set(), interruptAt: Set(),
    onInterrupt: Unhandled, max: NO_FAULTS,
  }
  pure def earlyAck(p: Proto): Config = { ...lateAck(p), acksLate: false }
}

module chord {
  import chordtypes.*

  const CFG: Config

  pure val HEADERS: Set[HeaderId] = Set(H(1), H(2))
  pure val N: int = 2
  pure val EXECS: Set[ExecId] = Set(W(1), W(2), W(3))
  pure val KEYS: Set[Key] =
    HEADERS.map(h => ResultKey(h)).union(HEADERS.map(h => PartKey(h)))
      .union(Set(DoneKey, CounterKey, GroupKey))
  pure val INTERRUPT_POINTS: Set[InterruptPoint] =
    Set(InClaimReply, InJoin, InExtend, InPublishUnsent, InPublishSent,
        InCleanup)

  // ---- broker ----
  var queue: HeaderId -> int  // queued copies of each header task message
  var uq: int                 // queued chord_unlock messages
  var execs: ExecId -> Exec
  // ---- result backend: header results (keep the first SUCCESS) ----
  var results: HeaderId -> Result
  // ---- redis chord keys (redis.py:944-948) ----
  var jset: Set[Part]         // <gid>.j
  var dset: Set[HeaderId]     // <gid>.d
  var skey: bool              // <gid>.s exists
  var ckey: Claim             // <gid>.c
  var nextTok: int
  // ---- kv chord keys ----
  var counter: int            // chord-unlock-<gid> counter
  var groupSaved: bool        // the saved GroupResult
  var partKeys: Set[HeaderId] // KvSkip, KvOnce: chord-unlock-<gid>.<task id>
  var doneKey: bool           // KvOnce: chord-unlock-<gid>.done
  // ---- observations ----
  var dispatched: int
  var bodyArgs: Set[HeaderId] // header indexes given to the last dispatched body
  var bodyArgsSize: int       // number of results given to it
  var chordError: bool        // chord_error_from_stack was called
  var errors: int             // how many times
  var errorAfterDispatch: bool
  var dispatchAfterError: bool
  var premature: bool         // dispatched before a header stored a result
  // ---- fault budgets used ----
  var used: Faults
  var lastAction: Action
  // Number of actions taken: makes "every run ends within D steps" an
  // invariant (steps <= D).
  var steps: int

  def allReady: bool = HEADERS.forall(h => not(isMissing(results.get(h))))
  // The protocol a process runs.
  def protoOf(w: ExecId): Proto =
    if (CFG.oldExecs.contains(w)) Kv else CFG.proto
  def anyFailed: bool = HEADERS.exists(h => results.get(h) == Failure)
  // A header has not stored a result yet.
  def neverStored: bool = HEADERS.exists(h => results.get(h) == Pending)

  action init: bool = all {
    // the header messages are published once (canvas.py chord.run)
    queue' = HEADERS.mapBy(_ => 1),
    // fallback_chord_unlock publishes one chord_unlock (base.py:1058)
    uq' = if (CFG.proto == Unlock) 1 else 0,
    execs' = EXECS.mapBy(_ => Free),
    results' = HEADERS.mapBy(_ => Pending),
    jset' = Set(), dset' = Set(),
    // set_chord_size: drop <gid>.c, set <gid>.s (redis.py:862-868)
    skey' = CFG.proto == Redis,
    ckey' = NoClaim, nextTok' = 1,
    // _apply_chord_incr saves the group (base.py:1422-1427); cache
    // backend sets the counter to 0 (cache.py:238-242)
    counter' = 0, groupSaved' = counts(CFG.proto),
    partKeys' = Set(), doneKey' = false,
    dispatched' = 0, bodyArgs' = Set(), bodyArgsSize' = 0,
    chordError' = false, errors' = 0, errorAfterDispatch' = false,
    dispatchAfterError' = false, premature' = false,
    used' = NONE_USED,
    lastAction' = Init, steps' = 0,
  }

  // ---- frame helpers ----
  // Every action records itself and counts one step.
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepRedis: bool = all {
    jset' = jset, dset' = dset, skey' = skey, ckey' = ckey,
  }
  action keepKv: bool = all {
    counter' = counter, groupSaved' = groupSaved, partKeys' = partKeys,
    doneKey' = doneKey,
  }
  action keepDispatch: bool = all {
    dispatched' = dispatched, bodyArgs' = bodyArgs, bodyArgsSize' = bodyArgsSize,
    dispatchAfterError' = dispatchAfterError, premature' = premature,
  }
  action keepErrors: bool = all {
    chordError' = chordError, errors' = errors,
    errorAfterDispatch' = errorAfterDispatch,
  }
  action keepObs: bool = all { keepDispatch, keepErrors }
  // chord_error_from_stack: FAILURE for the body id, and its errbacks
  // (base.py:464-520).
  action markError: bool = all {
    chordError' = true, errors' = errors + 1,
    errorAfterDispatch' = (errorAfterDispatch or dispatched > 0),
    keepDispatch,
  }

  // ---- delivery ------------------------------------------------------------
  // A worker takes a header message. Without acks_late it is acked before
  // the task runs (worker/request.py, Request.execute_using_pool acks
  // early unless acks_late).
  action deliver(w: ExecId, h: HeaderId): bool = all {
    isFree(execs.get(w)), queue.get(h) > 0,
    queue' = queue.set(h, queue.get(h) - 1),
    execs' = execs.set(w, HeaderTask({ h: h, tok: Tok(nextTok),
                                   acked: not(CFG.acksLate), main: false,
                                   pc: Running })),
    nextTok' = nextTok + 1,
    uq' = uq, results' = results, keepRedis, keepKv, keepObs, used' = used,
    did(Deliver({ w: w, h: h })),
  }

  action deliverUnlock(w: ExecId): bool = all {
    CFG.proto == Unlock, isFree(execs.get(w)), uq > 0,
    uq' = uq - 1,
    execs' = execs.set(w, UnlockTask({ acked: not(CFG.acksLate), pc: Polling })),
    queue' = queue, nextTok' = nextTok, results' = results, keepRedis, keepKv,
    keepObs, used' = used,
    did(DeliverUnlock(w)),
  }

  // ---- header task stores its result ----------------------------------------
  // mark_as_done: store_result, then on_chord_part_return (base.py:290-296).
  // Stores keep the first SUCCESS (resultstate.qnt). The unlock protocol has
  // no part return (BaseBackend.on_chord_part_return is a no-op, base.py:1016).
  action store(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      isHeader(e), r.pc == Running,
      // a header with ignore_result stores nothing, and still runs the part
      // return (base.py:293-296)
      results' = if (CFG.ignoreResult.contains(r.h)) results
                 else results.set(r.h, Success),
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (CFG.proto == Unlock) HeaderDone else Stored(Succeeded) })),
      queue' = queue, uq' = uq, nextTok' = nextTok, keepRedis, keepKv,
      keepObs, used' = used,
      did(Store({ w: w, h: r.h })),
    }
  }

  // ---- redis: CHORD_PART_RETURN_LUA (redis.py:96-146), one atomic step ----
  action partReturn(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      CFG.proto == Redis, isHeader(e), isStored(r.pc),
      if (ckey == Done) all {
        // done marker: late duplicate, return -1 (redis.py:104-107, 976-977)
        execs' = execs.set(w, HeaderTask({ ...r, pc: HeaderDone })),
        jset' = jset, dset' = dset, ckey' = ckey,
        did(PartReturn({ w: w, h: r.h, outcome: SawDone })),
      } else {
        // add the result once per task id and header index (redis.py:111-119)
        val fresh = not(dset.contains(r.h))
        val j2 = if (fresh)
                   jset.union(Set({ h: r.h, res: reportOf(r.pc), tok: r.tok }))
                 else jset
        val d2 = dset.union(Set(r.h))
        // readycount == size + totaldiff, then SET ckey token NX
        // (redis.py:120-142); totaldiff is 0 here (no add_to_chord)
        val complete = skey and j2.size() == N
        val claim = complete and ckey == NoClaim
        all {
          jset' = j2, dset' = d2,
          ckey' = if (claim) Claimed(r.tok) else ckey,
          // if readycount == total and claimed (redis.py:984)
          execs' = execs.set(w, HeaderTask({
            ...r, pc: if (claim) RedisClaimed else HeaderDone })),
          did(PartReturn({
            w: w, h: r.h,
            outcome: if (claim) TookClaim
                     else if (complete) CompleteNoClaim else Counted })),
        }
      },
      skey' = skey, queue' = queue, uq' = uq, results' = results,
      nextTok' = nextTok, keepKv, keepObs, used' = used,
    }
  }

  // redis: read <gid>.j and unpack (redis.py:1007-1017). A FAILURE raises
  // ChordError: chord_error_from_stack, then DEL <gid>.s (redis.py:1052-1060).
  action redisJoin(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val failed = jset.exists(x => x.res == Failed)
    all {
      CFG.proto == Redis, isHeader(e), r.pc == RedisClaimed,
      if (failed) all {
        markError, skey' = false,
        execs' = execs.set(w, HeaderTask({ ...r, pc: HeaderDone })),
        did(Join({ w: w, h: r.h, outcome: JoinError })),
      } else all {
        keepObs, skey' = skey,
        execs' = execs.set(w, HeaderTask({
          ...r, pc: RedisJoined({ args: jset.map(x => x.h),
                                  nargs: jset.size() }) })),
        did(Join({ w: w, h: r.h, outcome: JoinOk })),
      },
      jset' = jset, dset' = dset, ckey' = ckey,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, used' = used,
    }
  }

  // redis: CHORD_CLAIM_EXTEND_LUA (redis.py:171-185, called at 1021-1031)
  action extend(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val mine = ckey == Claimed(r.tok)
    val reacquire = ckey == NoClaim and skey
    all {
      CFG.proto == Redis, isHeader(e), at(r.pc) == AtRedisJoined,
      ckey' = if (not(mine) and reacquire) Claimed(r.tok) else ckey,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (mine or reacquire) RedisExtended(joinedOf(r.pc))
                  else HeaderDone })),
      did(Extend({
        w: w, h: r.h,
        outcome: if (mine) Extended
                 else if (reacquire) Reacquired else ClaimLost })),
      jset' = jset, dset' = dset, skey' = skey,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, keepObs, used' = used,
    }
  }

  // The run after its publish attempt.
  pure def afterPublish(proto: Proto, e: Exec, how: PublishOutcome): Exec =
    match e {
      | Free => Free
      | HeaderTask(r) => HeaderTask({
          ...r, pc: if (proto == Redis) RedisPublished(how) else KvClean })
      | UnlockTask(u) => UnlockTask({ ...u, pc: UnlockDone })
    }

  // Publish the body: app.amqp._retry_publish(callback.delay, ...)
  //   redis  redis.py:1032-1040 (then cleanup in finally)
  //   kv     base.py:1485-1492 (then deps.delete / delete counter in finally)
  //   unlock builtins.py:124-129
  // A refused publish calls chord_error_from_stack.
  action publish(w: ExecId, ok: bool): bool = {
    val e = execs.get(w)
    val body = publishingOf(e)
    all {
      isPublishing(e),
      ok or used.refusals < CFG.max.refusals,
      if (ok) all {
        dispatched' = dispatched + 1, bodyArgs' = body.args,
        bodyArgsSize' = body.nargs,
        dispatchAfterError' = (dispatchAfterError or chordError),
        premature' = (premature or neverStored),
        keepErrors, used' = used,
      } else all {
        markError, used' = { ...used, refusals: used.refusals + 1 },
      },
      execs' = execs.set(w, afterPublish(CFG.proto, e,
                                         if (ok) Sent else Refused)),
      did(Publish({ w: w, outcome: if (ok) Sent else Refused })),
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepRedis, keepKv,
    }
  }

  // redis: CHORD_CLEANUP_LUA (redis.py:150-168, called at 1041-1051)
  action cleanup(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val mine = ckey == Claimed(r.tok)
    all {
      CFG.proto == Redis, isHeader(e), at(r.pc) == AtRedisPublished,
      if (mine) all { jset' = Set(), dset' = Set(), skey' = false, ckey' = Done }
      else keepRedis,
      execs' = execs.set(w, HeaderTask({ ...r, pc: HeaderDone })),
      did(Cleanup({ w: w, h: r.h,
                              outcome: if (mine) Cleaned else NotOwner })),
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, keepObs, used' = used,
    }
  }

  // ---- kv: counter (base.py:1429-1503) ----
  action count(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      protoOf(w) == Kv, isHeader(e), isStored(r.pc),
      if (not(groupSaved)) all {
        // GroupResult.restore() is None: ChordError (base.py:1446-1455)
        markError, counter' = counter,
        execs' = execs.set(w, HeaderTask({ ...r, pc: HeaderDone })),
        did(Count({ w: w, h: r.h, outcome: NoGroup })),
      } else all {
        // val = self.incr(key); val == size -> join (base.py:1456-1466)
        keepObs, counter' = counter + 1,
        execs' = execs.set(w, HeaderTask({
          ...r, pc: if (counter + 1 == N) KvComplete else HeaderDone })),
        did(Count({
          w: w, h: r.h,
          outcome: if (counter + 1 == N) CountComplete
                   else if (counter + 1 > N) CountOver else CountedOne })),
      },
      groupSaved' = groupSaved, partKeys' = partKeys, doneKey' = doneKey,
      keepRedis,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // ---- KvSkip, KvOnce: the key of a header task that returned ----
  // KvSkip (09e5a59, base.py:1461-1488): add the key before the count; a
  //   run that finds it there returns without counting. With the group
  //   gone, only a part never recorded fails the chord.
  // KvOnce (1fa71d4, base.py:1510-1526 and _chord_part_returns :1266-1279):
  //   with the group gone, a copy that finds the chord claimed returns, and
  //   otherwise claims it and fails it; a header with ignore_result fails
  //   the chord at once (_fail_chord_once :1260-1266); otherwise add the
  //   key (whether or not it is there) and go on to count.
  action returned(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val p = protoOf(w)
    val recorded = partKeys.contains(r.h)
    val outcome: ReturnedOutcome =
      if (p == KvSkip)
        if (recorded) AlreadyRecorded
        else if (not(groupSaved)) GroupGone else KeyNew
      else if (not(groupSaved)) (if (doneKey) ChordDone else GroupGone)
      else if (CFG.ignoreResult.contains(r.h))
        (if (doneKey) ChordDone else IgnoresResult)
      else if (recorded) KeyThere else KeyNew
    val goesOn = outcome == KeyNew or outcome == KeyThere
    val fails = outcome == GroupGone or outcome == IgnoresResult
    all {
      p == KvSkip or p == KvOnce, isHeader(e), isStored(r.pc),
      partKeys' = if (goesOn or (p == KvSkip and outcome == GroupGone))
                    partKeys.union(Set(r.h))
                  else partKeys,
      doneKey' = (doneKey or (p == KvOnce and fails)),
      if (fails) markError else keepObs,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (goesOn) KeyAdded else HeaderDone })),
      did(Returned({ w: w, h: r.h, outcome: outcome })),
      counter' = counter, groupSaved' = groupSaved, keepRedis,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // val = self.incr(key)
  //   KvSkip (09e5a59 base.py:1490-1505): val == size -> join, as Kv.
  //   KvOnce (1fa71d4 base.py:1527-1540): val >= size -> check the results.
  action countKey(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val p = protoOf(w)
    val v = counter + 1
    all {
      p == KvSkip or p == KvOnce, isHeader(e), r.pc == KeyAdded,
      counter' = v,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (p == KvOnce) (if (v >= N) OnceCheck else HeaderDone)
                  else if (v == N) KvComplete else HeaderDone })),
      did(Count({ w: w, h: r.h,
                  outcome: if (v == N) CountComplete
                           else if (v > N) CountOver else CountedOne })),
      groupSaved' = groupSaved, partKeys' = partKeys, doneKey' = doneKey,
      keepRedis, keepObs,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // KvOnce: _complete_chord_once (1fa71d4 base.py:1289-1310), one action
  // per round trip.
  // 1. read the header results (_missing_results).
  action readResults(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val missing = HEADERS.filter(h => isMissing(results.get(h)))
    all {
      isHeader(e), r.pc == OnceCheck,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (missing == Set()) OnceClaim else OnceKeys(missing) })),
      did(ReadResults({ w: w, h: r.h, missing: missing })),
      keepRedis, keepKv, keepObs,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // 2. read the keys of the missing headers. A header without a key has
  //    not returned: wait for it. With every key there, 1fa71d4 reads the
  //    missing results again, because a header stores its result before
  //    its key and can do both between the two reads; cd8aa46
  //    (rereadMissing = false) failed the chord here.
  action checkKeys(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val missing = missingOf(r.pc)
    val outcome: CheckKeysOutcome =
      if (not(missing.subseteq(partKeys))) StillRunning
      else if (CFG.rereadMissing) KeysThere
      else if (doneKey) LostButClaimed else ResultLost
    all {
      isHeader(e), at(r.pc) == AtOnceKeys,
      doneKey' = (doneKey or outcome == ResultLost),
      if (outcome == ResultLost) markError else keepObs,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (outcome == KeysThere) OnceReread(missing)
                  else HeaderDone })),
      did(CheckKeys({ w: w, h: r.h, outcome: outcome })),
      counter' = counter, groupSaved' = groupSaved, partKeys' = partKeys,
      keepRedis,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // 3. the second read: a result still missing is lost, and the chord is
  //    failed by the one run that adds the done key (_fail_chord_once).
  action reread(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val missing = missingOf(r.pc).filter(h => isMissing(results.get(h)))
    val outcome: RereadOutcome =
      if (missing == Set()) Arrived
      else if (doneKey) StillLostButClaimed else StillLost
    all {
      isHeader(e), at(r.pc) == AtOnceReread,
      doneKey' = (doneKey or outcome == StillLost),
      if (outcome == StillLost) markError else keepObs,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (outcome == Arrived) OnceClaim else HeaderDone })),
      did(Reread({ w: w, h: r.h, outcome: outcome })),
      counter' = counter, groupSaved' = groupSaved, partKeys' = partKeys,
      keepRedis,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // 4. _claim_chord: add the done key if absent (memcached add, DynamoDB
  //    PutItem attribute_not_exists, Firestore create; cache.py:255-261).
  //    The one run that adds it joins and publishes.
  action claim(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      isHeader(e), r.pc == OnceClaim,
      doneKey' = true,
      execs' = execs.set(w, HeaderTask({
        ...r, pc: if (doneKey) HeaderDone else KvComplete })),
      did(ClaimChord({ w: w, h: r.h,
                       outcome: if (doneKey) ClaimTaken else ClaimWon })),
      counter' = counter, groupSaved' = groupSaved, partKeys' = partKeys,
      keepRedis, keepObs,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // kv: deps.join_native(timeout=result_chord_join_timeout, propagate=True)
  // (base.py:1467-1483). It waits for every header result; ok=false is the
  // join timing out (or a FAILURE propagating).
  action kvJoin(w: ExecId, ok: bool): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      counts(CFG.proto), isHeader(e), r.pc == KvComplete,
      if (ok and allReady and not(anyFailed)) all {
        keepObs,
        execs' = execs.set(w, HeaderTask({
          ...r, pc: KvJoined({ args: HEADERS, nargs: N }) })),
        did(Join({ w: w, h: r.h, outcome: JoinOk })),
      } else all {
        // join raises: a header FAILURE propagates, or a header result is
        // still missing when result_chord_join_timeout runs out
        anyFailed or (not(ok) and not(allReady)),
        markError,
        execs' = execs.set(w, HeaderTask({ ...r, pc: KvClean })),
        did(Join({ w: w, h: r.h, outcome: JoinError })),
      },
      keepRedis, keepKv,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // kv: finally: deps.delete(); self.delete(key) (base.py:1493-1495)
  action kvClean(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      counts(CFG.proto), isHeader(e), r.pc == KvClean,
      groupSaved' = false, counter' = 0,
      partKeys' = partKeys, doneKey' = doneKey,
      execs' = execs.set(w, HeaderTask({ ...r, pc: HeaderDone })),
      did(Cleanup({ w: w, h: r.h, outcome: Cleaned })),
      keepRedis, keepObs,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      used' = used,
    }
  }

  // ---- unlock: chord_unlock (builtins.py:50-130) ----
  // ready -> join; not ready -> self.retry() publishes a new chord_unlock
  // and this one is acked (Retry). A refused retry publish raises Reject:
  // chord_error_from_stack (celery PR 10959, builtins.py:87-106).
  action unlockPoll(w: ExecId, ok: bool): bool = {
    val e = execs.get(w)
    val u = unlockOf(e)
    all {
      CFG.proto == Unlock, isUnlock(e), u.pc == Polling,
      if (allReady) all {
        ok,
        if (anyFailed) all {
          // join(propagate=True) raises: builtins.py:114-122
          markError, execs' = execs.set(w, UnlockTask({ ...u, pc: UnlockDone })),
          did(Poll({ w: w, outcome: PollError })),
        } else all {
          keepObs,
          execs' = execs.set(w, UnlockTask({
            ...u, pc: UnlockJoined({ args: HEADERS, nargs: N }) })),
          did(Poll({ w: w, outcome: PollReady })),
        },
        uq' = uq, used' = used,
      } else if (ok) all {
        // the real task retries without limit; see Faults.idlePolls
        used.idlePolls < CFG.max.idlePolls,
        keepObs, uq' = uq + 1,
        used' = { ...used, idlePolls: used.idlePolls + 1 },
        execs' = execs.set(w, UnlockTask({ ...u, pc: UnlockDone })),
        did(Poll({ w: w, outcome: PollRetry })),
      } else all {
        used.refusals < CFG.max.refusals,
        markError, uq' = uq,
        used' = { ...used, refusals: used.refusals + 1 },
        execs' = execs.set(w, UnlockTask({ ...u, pc: UnlockDone })),
        did(Poll({ w: w, outcome: PollRefused })),
      },
      keepRedis, keepKv,
      queue' = queue, results' = results, nextTok' = nextTok,
    }
  }

  // The task returns; the message is acked (acks_late) and the process is free.
  action finish(w: ExecId): bool = all {
    isFinishing(execs.get(w)),
    execs' = execs.set(w, Free),
    did(Finish(w)),
    queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
    keepRedis, keepKv, keepObs, used' = used,
  }

  // ---- faults ----------------------------------------------------------------
  // The broker delivers a header message again (redelivery after a lost
  // connection, visibility timeout, or a duplicate publish).
  action dup(h: HeaderId): bool = all {
    used.dups < CFG.max.dups,
    queue' = queue.set(h, queue.get(h) + 1),
    used' = { ...used, dups: used.dups + 1 },
    uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepRedis, keepKv, keepObs,
    did(Dup(h)),
  }

  // The message of a run goes back to its queue.
  pure def requeued(q: HeaderId -> int, e: Exec): HeaderId -> int = match e {
    | Free => q
    | HeaderTask(r) => q.set(r.h, q.get(r.h) + 1)
    | UnlockTask(_) => q
  }

  pure def inKillWindow(e: Exec): bool = match CFG.killWindow {
    | Anywhere => true
    | BeforeCount => isHeader(e) and (at(headerOf(e).pc) == AtStored
                                      or at(headerOf(e).pc) == AtKeyAdded)
    | AfterClaim => isHeader(e) and (
        at(headerOf(e).pc) == AtKvComplete or at(headerOf(e).pc) == AtKvJoined
        or at(headerOf(e).pc) == AtRedisClaimed
        or at(headerOf(e).pc) == AtRedisJoined
        or at(headerOf(e).pc) == AtRedisExtended)
  }

  // The whole worker is lost: nothing else runs; an unacked message goes
  // back to the queue (broker).
  action loseWorker(w: ExecId): bool = {
    val e = execs.get(w)
    all {
      not(isFree(e)), used.workerLost < CFG.max.workerLost,
      inKillWindow(e),
      execs' = execs.set(w, Free),
      queue' = if (isAcked(e)) queue else requeued(queue, e),
      uq' = if (not(isAcked(e)) and isUnlock(e)) uq + 1 else uq,
      used' = { ...used, workerLost: used.workerLost + 1 },
      results' = results, nextTok' = nextTok, keepRedis, keepKv, keepObs,
      did(LoseWorker({
        w: w, outcome: if (isAcked(e)) MessageLost else MessageRequeued })),
    }
  }

  // A prefork pool child is lost; the main process gets WorkerLostError
  // and runs Request.on_failure (worker/request.py:709-790):
  //  - SUCCESS already stored: ack (acks_late) and return, without a chord
  //    part return (celery PR 10752, request.py:714-736);
  //  - acks_late and reject_on_worker_lost: requeue (request.py:751-758);
  //  - otherwise ack (acks_on_failure) and mark_as_failure, which stores
  //    FAILURE and runs on_chord_part_return in the main process
  //    (request.py:778-783, base.py:298-309).
  // chord_unlock runs with ignore_result, so the SUCCESS check is skipped
  // (request.py:850-851) and mark_as_failure has no chord to report to.
  action loseChild(w: ExecId): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    all {
      // also at HeaderDone: the child can die after on_chord_part_return
      // returned, in on_success, a task_postrun receiver or process_cleanup
      not(isFree(e)), not(r.main), used.childLost < CFG.max.childLost,
      used' = { ...used, childLost: used.childLost + 1 },
      if (isHeader(e) and results.get(r.h) == Success) all {
        execs' = execs.set(w, Free), queue' = queue, uq' = uq, results' = results,
        did(LoseChild({ w: w, outcome: SuccessStored })),
      } else if (CFG.acksLate and CFG.rejectOnWorkerLost and not(isAcked(e))) all {
        execs' = execs.set(w, Free), results' = results,
        queue' = requeued(queue, e),
        uq' = if (isUnlock(e)) uq + 1 else uq,
        did(LoseChild({ w: w, outcome: ChildRequeued })),
      } else if (isHeader(e)) all {
        // store_result keeps a stored SUCCESS (resultstate.qnt)
        results' = if (results.get(r.h) == Success) results
                   else results.set(r.h, Failure),
        execs' = execs.set(w, HeaderTask({
          ...r, main: true, acked: true,
          pc: if (CFG.proto == Unlock) HeaderDone else Stored(Failed) })),
        queue' = queue, uq' = uq,
        did(LoseChild({ w: w, outcome: MarkedFailure })),
      } else all {
        execs' = execs.set(w, Free), queue' = queue, uq' = uq, results' = results,
        did(LoseChild({ w: w, outcome: ChildAcked })),
      },
      nextTok' = nextTok, keepRedis, keepKv, keepObs,
    }
  }

  // A chord key expires or is evicted (memcached under memory pressure, a
  // TTL shorter than the chord). The header's key and the done key live
  // twice as long as the results (1fa71d4 base.py:1246-1251), so a result
  // can go while they stay; evicting them takes its own budget.
  //   ResultKey   the header result reads as PENDING again
  //   CounterKey  the next incr starts from 0 (DynamoDB ADD, Firestore
  //               Increment; memcached incr on a missing key returns None)
  action evict(k: Key): bool = all {
    used.evictions < CFG.max.evictions, CFG.evictable.contains(k),
    match k {
      | ResultKey(h) => all {
          not(isMissing(results.get(h))),
          results' = results.set(h, Evicted),
          keepKv }
      | PartKey(h) => all {
          partKeys.contains(h), partKeys' = partKeys.exclude(Set(h)),
          counter' = counter, groupSaved' = groupSaved, doneKey' = doneKey,
          results' = results }
      | DoneKey => all {
          doneKey, doneKey' = false, counter' = counter,
          groupSaved' = groupSaved, partKeys' = partKeys, results' = results }
      | CounterKey => all {
          counter > 0, counter' = 0, groupSaved' = groupSaved,
          partKeys' = partKeys, doneKey' = doneKey, results' = results }
      | GroupKey => all {
          groupSaved, groupSaved' = false, counter' = counter,
          partKeys' = partKeys, doneKey' = doneKey, results' = results }
    },
    used' = { ...used, evictions: used.evictions + 1 },
    queue' = queue, uq' = uq, execs' = execs, nextTok' = nextTok,
    keepRedis, keepObs,
    did(Evict(k)),
  }

  // celery issue 11039. A hard time limit (or a cold shutdown) raises
  // SystemExit in the pool child at `point` of on_chord_part_return, after
  // this run took the dispatch claim. The worker's main process marks the
  // task failed and acks it (request.py:539-579); its own part return finds
  // the header counted and the claim held, and does nothing. In the child:
  //   Unhandled (768e476)   the exception leaves; only the `finally` around
  //                         the publish runs the cleanup (redis.py:1041-1051)
  //   FailWhileDispatching  5f844ea: `except BaseException` fails the chord
  //                         whenever this run took the dispatch
  //   FailUntilSent         5446659 redis.py:1043-1046, 1076-1089: the same,
  //                         but not once _retry_publish returned
  //   FailAndClose          ee48301 redis.py:1085-1094: the same, and DEL
  //                         <gid>.s after failing the chord, so a copy of a
  //                         header task that arrives later cannot complete it
  //   CloseAlways           ad00b4a redis.py:980, 1043-1046, 1081-1097: a
  //                         `sent` flag decides whether to fail the chord,
  //                         and <gid>.s is dropped whenever this run took
  //                         the dispatch, so a later copy cannot send the
  //                         body a second time either
  // The reply of the claim script is not covered by either fix: the child
  // does not know yet that it holds the claim.
  action interrupt(w: ExecId, point: InterruptPoint, ran: bool): bool = {
    val e = execs.get(w)
    val r = headerOf(e)
    val here: bool = match point {
      | InClaimReply => r.pc == RedisClaimed
      | InJoin => r.pc == RedisClaimed
      | InExtend => at(r.pc) == AtRedisJoined
      | InPublishUnsent => at(r.pc) == AtRedisExtended
      | InPublishSent => at(r.pc) == AtRedisExtended
      | InCleanup => at(r.pc) == AtRedisPublished
    }
    // the body went out in this step
    val sends = point == InPublishSent
    // the `finally` runs the cleanup script: always after an interrupted
    // publish; in InCleanup the script itself ran or not
    val cleans = (point == InPublishUnsent or point == InPublishSent
                  or (point == InCleanup and ran))
                 and ckey == Claimed(r.tok)
    val untilSent =
      point == InJoin or point == InExtend or point == InPublishUnsent
      or point == InPublishSent
      or (point == InCleanup and r.pc == RedisPublished(Refused))
    val handled: bool = match CFG.onInterrupt {
      | Unhandled => false
      | FailWhileDispatching => point != InClaimReply
      | FailUntilSent => untilSent
      | FailAndClose => untilSent
      | CloseAlways => untilSent
    }
    val closes = match CFG.onInterrupt {
      | Unhandled => false
      | FailWhileDispatching => false
      | FailUntilSent => false
      | FailAndClose => handled
      | CloseAlways => point != InClaimReply
    }
    all {
      CFG.proto == Redis, isHeader(e), not(r.main), here,
      CFG.interruptAt.contains(point),
      // ran only matters in InCleanup
      point == InCleanup or ran,
      used.interrupts < CFG.max.interrupts,
      used' = { ...used, interrupts: used.interrupts + 1 },
      dispatched' = if (sends) dispatched + 1 else dispatched,
      bodyArgs' = if (sends) joinedOf(r.pc).args else bodyArgs,
      bodyArgsSize' = if (sends) joinedOf(r.pc).nargs else bodyArgsSize,
      dispatchAfterError' = (dispatchAfterError or (sends and chordError)),
      premature' = (premature or (sends and neverStored)),
      chordError' = (chordError or handled),
      errors' = if (handled) errors + 1 else errors,
      errorAfterDispatch' = (errorAfterDispatch
                             or (handled and (dispatched > 0 or sends))),
      jset' = if (cleans) Set() else jset,
      dset' = if (cleans) Set() else dset,
      skey' = if (cleans or closes) false else skey,
      ckey' = if (cleans) Done else ckey,
      execs' = execs.set(w, Free),
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv,
      did(Interrupt({ w: w, point: point, handled: handled })),
    }
  }

  // A live run holds the claim.
  def claimHeld: bool = EXECS.exists(w =>
    isHeader(execs.get(w)) and ckey == Claimed(headerOf(execs.get(w)).tok))

  // The claim key's TTL runs out (redis.py:963, 1018-1020). With leaseSafe
  // only once the holder is gone.
  action claimExpires: bool = all {
    CFG.proto == Redis, isClaimed(ckey),
    not(CFG.leaseSafe) or not(claimHeld),
    ckey' = NoClaim,
    jset' = jset, dset' = dset, skey' = skey,
    queue' = queue, uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepKv, keepObs, used' = used,
    did(ClaimExpires),
  }

  // The "done" marker expires (done_expires, redis.py:966).
  action doneExpires: bool = all {
    CFG.proto == Redis, ckey == Done,
    ckey' = NoClaim,
    jset' = jset, dset' = dset, skey' = skey,
    queue' = queue, uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepKv, keepObs, used' = used,
    did(DoneExpires),
  }

  // The choices are made next to the actions that use them: the simulator
  // and TLC try every combination of the choices in scope for every action.
  action step: bool = any {
    {
      nondet w = EXECS.oneOf()
      any {
        deliverUnlock(w), store(w), partReturn(w), redisJoin(w), extend(w),
        cleanup(w), count(w), kvClean(w), finish(w), loseWorker(w),
        loseChild(w),
        returned(w), countKey(w), readResults(w), checkKeys(w), reread(w),
        claim(w),
      }
    },
    {
      nondet w = EXECS.oneOf()
      nondet h = HEADERS.oneOf()
      deliver(w, h)
    },
    {
      nondet w = EXECS.oneOf()
      nondet ok = Set(true, false).oneOf()
      any { publish(w, ok), kvJoin(w, ok), unlockPoll(w, ok) }
    },
    {
      nondet h = HEADERS.oneOf()
      dup(h)
    },
    claimExpires, doneExpires,
    {
      nondet k = KEYS.oneOf()
      evict(k)
    },
    {
      nondet w = EXECS.oneOf()
      nondet point = INTERRUPT_POINTS.oneOf()
      nondet ran = Set(true, false).oneOf()
      interrupt(w, point, ran)
    },
  }

  // ---- properties --------------------------------------------------------------
  // Safety: the body is dispatched at most once.
  val atMostOnce: bool = dispatched <= 1
  // Safety: the body is not dispatched before every header result is stored.
  val noPremature: bool = not(premature)
  // Safety: the body gets exactly one result per header.
  val argsComplete: bool =
    dispatched > 0 implies (bodyArgs == HEADERS and bodyArgsSize == N)
  // Safety: a chord whose body was dispatched is not failed afterwards
  // (chord_error_from_stack stores FAILURE for the body id and calls its
  // errbacks, base.py:464-520).
  val noErrorAfterDispatch: bool = not(errorAfterDispatch)
  // Safety: the chord is failed only for a reason: a header FAILURE or a
  // refused publish (not a duplicate delivery, a timeout on a header that
  // would have finished, or a cleaned-up group).
  val noSpuriousError: bool = chordError implies (
    anyFailed or used.refusals > 0
    // or a result or key that was lost, a header that ignores its result,
    // or an interrupted dispatch
    or used.evictions > 0 or CFG.ignoreResult != Set() or used.interrupts > 0)
  // Safety: the body is not dispatched after the chord was failed (its
  // errbacks ran and FAILURE is stored for the body id).
  val noDispatchAfterError: bool = not(dispatchAfterError)
  // Safety: the chord is failed at most once.
  val failsOnce: bool = errors <= 1
  // Witness (expected to be violated): some behavior dispatches the body.
  val neverDispatched: bool = dispatched == 0
  // Witnesses (expected to be violated in some instance): the chord is
  // failed; the chord is both failed and dispatched; a run ends.
  val neverFailed: bool = not(chordError)
  val neverQuiet: bool = not(quiet)

  // ---- liveness, in layers ------------------------------------------------
  // 1. Every run is finite: each action lowers "measure", which is never
  //    negative, so steps + measure never exceeds what the instance starts
  //    with. The exact longest run D of each instance is measured by
  //    exhaustive search and checked as steps <= D.
  // 2. A run stops only in a quiet state: the search reports any other
  //    state without a successor (stepOrQuiet below).
  // 3. A quiet state has ended: the body was dispatched or the chord was
  //    failed (quietDone).
  // Together: every run ends, within D steps, with the body dispatched or
  // the chord failed. An instance that breaks 3 has a run that hangs.
  val remaining: Faults = {
    dups: CFG.max.dups - used.dups,
    workerLost: CFG.max.workerLost - used.workerLost,
    childLost: CFG.max.childLost - used.childLost,
    refusals: CFG.max.refusals - used.refusals,
    evictions: CFG.max.evictions - used.evictions,
    interrupts: CFG.max.interrupts - used.interrupts,
    idlePolls: if (CFG.proto == Unlock) CFG.max.idlePolls - used.idlePolls
               else 0,
  }
  pure def budgetRank(b: Faults): int =
    FAULT * (b.dups + b.workerLost + b.childLost + b.evictions + b.interrupts)
    + IDLE_POLL * b.idlePolls
  val measure: int =
    QUEUED_HEADER * HEADERS.fold(0, (s, h) => s + queue.get(h))
    + QUEUED_UNLOCK * uq
    + EXECS.fold(0, (s, w) => s + execRank(execs.get(w)))
    + claimRank(ckey) + budgetRank(remaining)
  pure val MEASURE0: int =
    QUEUED_HEADER * N + (if (CFG.proto == Unlock) QUEUED_UNLOCK else 0)
    + budgetRank({ ...CFG.max, idlePolls: if (CFG.proto == Unlock)
                                             CFG.max.idlePolls else 0 })
  val progress: bool = measure >= 0 and steps + measure <= MEASURE0

  // Quiet: nothing is left that could change the chord's outcome without a
  // new fault.
  //   redis, kv  no message queued, no process busy, no claim left to
  //              expire (a claim that expires lets a later copy claim).
  //   unlock     no header message queued or running, and either every
  //              header result is stored and no chord_unlock is queued or
  //              running, or a result is missing and chord_unlock can only
  //              poll again: it would retry forever.
  def headersSettled: bool =
    HEADERS.forall(h => queue.get(h) == 0)
    and EXECS.forall(w => not(isHeader(execs.get(w))))
  def unlockIdle: bool = EXECS.forall(w =>
    isUnlock(execs.get(w)) implies unlockOf(execs.get(w)).pc == Polling)
  val quiet: bool =
    if (CFG.proto == Unlock)
      headersSettled
      and (if (allReady) uq == 0 and EXECS.forall(w => isFree(execs.get(w)))
           else unlockIdle)
    else
      EXECS.forall(w => isFree(execs.get(w)))
      and HEADERS.forall(h => queue.get(h) == 0) and not(isClaimed(ckey))
  val quietDone: bool = quiet implies (dispatched > 0 or chordError)

  // The step the exhaustive search runs: a quiet state may stutter, so the
  // only states without a successor are runs that stopped without being
  // quiet, which the search reports as deadlocks.
  action stutter: bool = all {
    quiet,
    queue' = queue, uq' = uq, execs' = execs, results' = results,
    nextTok' = nextTok, keepRedis, keepKv, keepObs, used' = used,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  val safety: bool =
    atMostOnce and noPremature and argsComplete and noErrorAfterDispatch
}
