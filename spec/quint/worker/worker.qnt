// -*- mode: Bluespec; -*-
// A message from the broker to a finished task, with several workers:
// exactly once without a fault, at least once with one, and which
// duplicates a design can avoid.
//
// Two workers, each with one pool process and a prefetch count of 2, and
// two messages on one queue of a Redis broker. Rules cite celery at 768e476
// and kombu at 445027e (main) unless a line names another commit.
//
// The worker's pool hand-off (celery/concurrency/asynpool.py,
// celery/worker/request.py):
//
//   Received   the message is reserved; its job waits in the pool's
//              outbound buffer (Scheduled first, for an ETA not yet due)
//   Written    the job is written to the child, which starts the task
//   Accepted   the parent read the child's accept; early ack is sent here
//              (request.py:525-533)
//   Finished   the child is done; the result waits to be read
//   read       the parent reads the result; late ack is sent here
//
// A warm shutdown (SIGTERM) and a reconnect call Consumer.on_close()
// (worker/consumer/consumer.py:581-621), which forgets reserved requests
// that are not active and calls AsynPool.flush() (asynpool.py:1079-1150)
// between two turns of the event loop.
//
// The broker side is kombu's Redis transport: a popped message is recorded
// in `unacked` under its delivery tag; an ack removes the record by tag,
// whoever popped it, and is not bound to a connection; a channel that
// closes restores what it has delivered and not acked, if it can reach
// Redis (virtual/base.py:277-314); any consumer restores a record older
// than the visibility timeout (redis.py:521-547).
//
// CFG.design picks, per mechanism, main's behavior or a variant:
//
//   pop               BrpopThenRecord (main) | PopScript (kombu PR 2831)
//   flush             DropUnaccepted (main, since celery PR 6863)
//                     | DropUnwritten (keep a job the child already has)
//   renewal           NoRenewal (main) | RenewAll | RenewUnstarted
//                     | RenewScheduled: a live, connected worker moves the
//                     visibility deadline of messages it holds
//   failedAck         Forgotten (main: QoS.ack raises before it marks the
//                     tag, redis.py:497-499) | Resent after the reconnect
//   collectedAck      WorkerExits (main: an ack through a channel whose
//                     connection was collected raises AttributeError,
//                     redis.py:1957, celery issue 11054) | StillAcks
//   closeRestore      CloseRestoresAll (main) | CloseRestoresIdle: a
//                     closing channel does not restore a message whose
//                     task is running
//   reconnectRestore  WaitsForTimeout (main) | ReconnectRestoresAll
//                     | ReconnectRestoresIdle: restore after the reconnect
//                     what the closing channel could not
//   restoreBy         ByTag (main: restore_by_tag takes whatever record is
//                     under the tag, and a message keeps its tag when it is
//                     delivered again) | ByPop: a channel restores only a
//                     record its own pop wrote
//
// Time is abstract. A record of a live worker that still has a job for it
// is restored only with the `timeouts` budget (a task, an ETA or a
// partition that outlasts the visibility timeout). A record nothing will
// ever ack (its worker is gone, the job was dropped, the task hangs) is
// restored for free: the timeout always passes. Renewal blocks both.
//
// Not modeled here: the gap between BRPOP and the record without a
// partition, wake lists and put-backs (redis_transport.qnt); the publisher
// (publisher.qnt); a broker that loses writes it acknowledged (Redis with
// appendfsync everysec); several queues and priorities; more than one pool
// process per worker; RabbitMQ, where an ack is bound to its channel.
module workertypes {
  // Ids are one-field records with different field names (see chordtypes).
  type MsgId = { m: int }
  type WorkerId = { c: int }
  pure def M(i: int): MsgId = { m: i }
  pure def C(i: int): WorkerId = { c: i }

  type AckMode = Early | Late
  type Pop = BrpopThenRecord | PopScript
  type Flush = DropUnaccepted | DropUnwritten
  type Renewal = NoRenewal | RenewAll | RenewUnstarted | RenewScheduled
  type FailedAck = Forgotten | Resent
  type CollectedAck = WorkerExits | StillAcks
  type CloseRestore = CloseRestoresAll | CloseRestoresIdle
  type ReconnectRestore =
    WaitsForTimeout | ReconnectRestoresAll | ReconnectRestoresIdle
  type RestoreBy = ByTag | ByPop
  type Design = {
    pop: Pop, flush: Flush, renewal: Renewal, failedAck: FailedAck,
    collectedAck: CollectedAck, closeRestore: CloseRestore,
    reconnectRestore: ReconnectRestore, restoreBy: RestoreBy,
  }
  pure val MAIN: Design = {
    pop: BrpopThenRecord, flush: DropUnaccepted, renewal: NoRenewal,
    failedAck: Forgotten, collectedAck: WorkerExits,
    closeRestore: CloseRestoresAll, reconnectRestore: WaitsForTimeout,
    restoreBy: ByTag,
  }
  // kombu PR 2831 on top of main.
  pure val PR: Design = { ...MAIN, pop: PopScript }
  // The variants compared in README.md, each on top of PR. Instances name
  // them in lower case: <design>_<ack mode>_<fault class>.
  // 1. flush() keeps a job the child already has
  pure val KEEPWRITTEN: Design = { ...PR, flush: DropUnwritten }
  // 2. renewal of unacked messages
  pure val RENEWALL: Design = { ...PR, renewal: RenewAll }
  pure val RENEWIDLE: Design = { ...PR, renewal: RenewUnstarted }
  pure val RENEWETA: Design = { ...PR, renewal: RenewScheduled }
  // 3. a failed ack is sent again; a collected channel can still ack
  pure val RESEND: Design = { ...PR, failedAck: Resent }
  pure val ACKFULL: Design =
    { ...PR, failedAck: Resent, collectedAck: StillAcks }
  // 4. restore at reconnect what the closing channel could not
  pure val RESTOREALL: Design =
    { ...ACKFULL, reconnectRestore: ReconnectRestoresAll }
  pure val RESTOREIDLE: Design =
    { ...ACKFULL, reconnectRestore: ReconnectRestoresIdle }
  // 5. a channel restores only a record its own pop wrote; the pop token
  // on its own, and with the flush variant so that the restore is the only
  // source of a third run (TAGRESTORE is its baseline)
  pure val TOKEN: Design = { ...PR, restoreBy: ByPop }
  pure val TAGRESTORE: Design = KEEPWRITTEN
  pure val OWNRECORD: Design = { ...KEEPWRITTEN, restoreBy: ByPop }
  // 6. all of them, and a closing channel leaves running tasks alone
  pure val BEST: Design = {
    ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent,
    collectedAck: StillAcks, closeRestore: CloseRestoresIdle,
    reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop,
  }

  // A worker's connection to the broker.
  type Conn =
    | Up
    | RepliesDropped   // a partition: commands arrive, replies do not
    | Cut              // a partition: nothing passes; it can still heal
    | Broken           // dead (the broker died); the worker has not noticed
    | Closed           // the worker noticed and collected it; it reconnects
  type Phase = Consuming | Draining
  // Where the job of a message is in one worker.
  type Stage =
    | NoJob
    | Scheduled        // reserved, waiting for its ETA
    | Received         // reserved, waiting for the pool process
    | Written          // the child has it and runs it; accept not read yet
    | Accepted
    | Finished         // the child is done; result not read yet
    | Hung             // accepted, and the task never returns
    | Dropped          // flush() forgot the job; the child runs it anyway
  type Live = {
    conn: Conn,
    phase: Phase,
    // Its pop went out into a partition: no second one until that ends.
    stuckPop: bool,
    // QoS._delivered of the live channel: delivered and not acked.
    tags: Set[MsgId],
    // The same of a channel that was collected without restoring them.
    oldTags: Set[MsgId],
    // Acks sent into a connection that does not answer.
    blocked: Set[MsgId],
    // Failed acks kept to be sent again (failedAck = Resent).
    pending: Set[MsgId],
    jobs: MsgId -> Stage,
  }
  type Worker = Dead | Alive(Live)
  pure def isAlive(w: Worker): bool = match w {
    | Dead => false | Alive(_) => true
  }

  // A record in the unacked hash and index, with who popped it.
  type Record = NoRec | HeldBy(WorkerId)
  pure def hasRec(r: Record): bool = match r {
    | NoRec => false | HeldBy(_) => true
  }

  // Why a message went back to the queue, for a task that then runs twice.
  //   Unavoidable  late ack, and its first run was in progress when its
  //                worker or its connection was lost
  //   Avoidable    the first run had finished, had not started, was acked
  //                early, or only outlasted the visibility timeout
  //   Recovery     the first run hangs: the second one is the recovery
  type DupClass = NoCause | Avoidable | Unavoidable | Recovery

  type AckOutcome =
    | NoAckDue            // not this step's job (the other ack mode)
    | AckDone
    | AckAppliedReplyLost // Redis removed the record; the worker cannot know
    | AckBlocked          // sent into a connection that does not answer
    | CollectedExits      // through a collected channel: the worker exits
    | CollectedAcks
    | CollectedFails      // and the broker cannot be reached
    | TagGone             // the tag is on no channel of this worker any more
  type RestoreWhy =
    | HolderDead | NobodyWillAck | HungTask | BehindHungTask
    | HeldPastTimeout
  type Action =
    | Init
    | Publish(MsgId)
    | Deliver({ c: WorkerId, m: MsgId })
    | LostPop({ c: WorkerId, m: MsgId })
    | EtaDue({ c: WorkerId, m: MsgId })
    | Write({ c: WorkerId, m: MsgId })
    | Accept({ c: WorkerId, m: MsgId, outcome: AckOutcome })
    | Finish({ c: WorkerId, m: MsgId })
    | FinishDropped({ c: WorkerId, m: MsgId })
    | Hang({ c: WorkerId, m: MsgId })
    | ReadResult({ c: WorkerId, m: MsgId, outcome: AckOutcome })
    | Sigterm(WorkerId)
    | Exit(WorkerId)
    | Kill(WorkerId)
    | Restart(WorkerId)
    | Crash
    | BrokerRestart
    | Partition(WorkerId)
    | CutOff(WorkerId)
    | Reset(WorkerId)
    | Heal(WorkerId)
    | NoticeLoss(WorkerId)
    | Reconnect(WorkerId)
    | RestoreVisible({ m: MsgId, reason: RestoreWhy })

  // Budgets. The first two are not faults: a warm shutdown, and something
  // held longer than the visibility timeout. `restarts` is the supervisor.
  type Budget = {
    shutdowns: int, timeouts: int,
    kills: int, crashes: int, partitions: int, cuts: int, resets: int,
    hangs: int,
    restarts: int,
  }
  pure val NONE: Budget = {
    shutdowns: 0, timeouts: 0, kills: 0, crashes: 0, partitions: 0, cuts: 0,
    resets: 0, hangs: 0, restarts: 0,
  }
  pure def faults(b: Budget): int =
    b.kills + b.crashes + b.partitions + b.cuts + b.resets + b.hangs

  // What the `timeouts` budget may be spent on: only a message waiting for
  // its ETA, or anything a worker still holds.
  type Outlasts = OnlyEta | AnythingHeld
  type Config = {
    ack: AckMode,
    design: Design,
    // Messages with an ETA: reserved, then held until it is due.
    eta: Set[MsgId],
    outlasts: Outlasts,
    max: Budget,
  }
  pure def config(a: AckMode, d: Design): Config =
    { ack: a, design: d, eta: Set(), outlasts: AnythingHeld, max: NONE }
}

module worker {
  import workertypes.*
  import observe.* from "../lib/observe"

  const CFG: Config

  pure val MSGS: Set[MsgId] = Set(M(1), M(2))
  pure val WORKERS: Set[WorkerId] = Set(C(1), C(2))
  pure val PREFETCH: int = 2
  pure val D: Design = CFG.design
  // Round trips to pop one message: BRPOP and the record, or one script.
  pure val POP_COST: int = if (D.pop == PopScript) 1 else 2

  // ---- broker ----
  var unpublished: Set[MsgId]
  var queue: Set[MsgId]
  var unacked: MsgId -> Record
  var brokerUp: bool
  // Popped for a reply that never arrived and recorded nowhere (main).
  var lost: Set[MsgId]
  // ---- workers ----
  var workers: WorkerId -> Worker
  // ---- observations ----
  var starts: MsgId -> int     // times a pool process started the task
  var finished: MsgId -> int   // times a run finished
  var cause: MsgId -> DupClass // why it last went back to the queue
  var dupAvoidable: int
  var dupUnavoidable: int
  // A closing or reconnecting channel restored a record that the pop of
  // another worker wrote.
  var strayRestore: bool
  var exits: int               // workers stopped by an error in the ack path
  var ops: int                 // round trips to the broker
  var used: Budget
  var lastAction: Action
  var steps: int

  pure val FRESH: Live = {
    conn: Up, phase: Consuming, stuckPop: false, tags: Set(), oldTags: Set(),
    blocked: Set(), pending: Set(), jobs: MSGS.mapBy(_ => NoJob),
  }
  action init: bool = all {
    unpublished' = MSGS, queue' = Set(), unacked' = MSGS.mapBy(_ => NoRec),
    brokerUp' = true, lost' = Set(),
    workers' = WORKERS.mapBy(_ => Alive(FRESH)),
    starts' = MSGS.mapBy(_ => 0), finished' = MSGS.mapBy(_ => 0),
    cause' = MSGS.mapBy(_ => NoCause), dupAvoidable' = 0, dupUnavoidable' = 0,
    strayRestore' = false,
    exits' = 0, ops' = 0, used' = NONE, lastAction' = Init, steps' = 0,
  }

  // ---- reading the state ----
  pure val NOT_LIVE: Live = { ...FRESH, conn: Closed }
  // Only read under the guard alive(c).
  pure def liveOf(w: Worker): Live = match w {
    | Dead => NOT_LIVE | Alive(l) => l
  }
  def alive(c: WorkerId): bool = isAlive(workers.get(c))
  def live(c: WorkerId): Live = liveOf(workers.get(c))
  def job(c: WorkerId, m: MsgId): Stage = live(c).jobs.get(m)
  // The child has a task: it runs one, or its result was not read yet.
  pure def busy(s: Stage): bool = match s {
    | NoJob => false | Scheduled => false | Received => false
    | Written => true | Accepted => true | Finished => true | Hung => true
    | Dropped => true
  }
  // The task was started here and has not finished.
  pure def running(s: Stage): bool = match s {
    | NoJob => false | Scheduled => false | Received => false
    | Written => true | Accepted => true | Finished => false | Hung => false
    | Dropped => true
  }
  pure def childIdle(l: Live): bool = MSGS.forall(m => not(busy(l.jobs.get(m))))
  pure def childHung(l: Live): bool = MSGS.exists(m => l.jobs.get(m) == Hung)
  def late: bool = CFG.ack == Late

  // Every action records itself, counts one step and `n` round trips.
  action did(a: Action, n: int): bool = all {
    lastAction' = a, steps' = steps + 1, ops' = ops + n,
  }
  action keepBroker: bool = all {
    unpublished' = unpublished, queue' = queue, unacked' = unacked,
    brokerUp' = brokerUp, lost' = lost,
  }
  action keepStarts: bool = all {
    starts' = starts, finished' = finished, dupAvoidable' = dupAvoidable,
    dupUnavoidable' = dupUnavoidable,
  }
  action keepRuns: bool = all { keepStarts, strayRestore' = strayRestore }
  def stray(c: WorkerId, back: Set[MsgId]): bool =
    back.exists(m => unacked.get(m) != HeldBy(c))

  // ---- why a message goes back, for the task that then runs twice ----
  // `connectionLost`: the restore is the closing or reconnecting channel's.
  def classify(m: MsgId, connectionLost: bool): DupClass =
    if (finished.get(m) > 0) Avoidable
    else if (WORKERS.exists(c => alive(c) and job(c, m) == Hung)) Recovery
    else if (starts.get(m) == 0) Avoidable
    else if (not(late)) Avoidable
    // a run that is in progress in a worker whose connection is fine only
    // outlasted the visibility timeout
    else if (not(connectionLost) and WORKERS.exists(c =>
               alive(c) and running(job(c, m)) and live(c).conn == Up))
      Avoidable
    else Unavoidable

  // The messages of `tags` that a channel of worker c puts back on the
  // queue: those with a record under the tag, or with its own record.
  def restorable(c: WorkerId, tags: Set[MsgId]): Set[MsgId] =
    tags.filter(m => if (D.restoreBy == ByPop) unacked.get(m) == HeldBy(c)
                     else hasRec(unacked.get(m)))
  action restoreAll(back: Set[MsgId], connectionLost: bool): bool = all {
    queue' = queue.union(back),
    unacked' = MSGS.mapBy(m => if (back.contains(m)) NoRec else unacked.get(m)),
    cause' = MSGS.mapBy(m => if (back.contains(m)) classify(m, connectionLost)
                             else cause.get(m)),
  }

  // ---- publish, deliver ----
  // @cite kombu kombu/transport/redis.py:1677-1680 "def _put(self, queue, message, **kwargs):"
  action publish(m: MsgId): bool = all {
    unpublished.contains(m), brokerUp,
    unpublished' = unpublished.exclude(Set(m)), queue' = queue.union(Set(m)),
    unacked' = unacked, brokerUp' = brokerUp, lost' = lost,
    workers' = workers, keepRuns, cause' = cause, exits' = exits,
    used' = used, did(Publish(m), 1),
  }

  // A consuming worker with prefetch credit pops a message. Credit counts
  // delivered and not acked (virtual/base.py:197-204), so with early ack a
  // slot frees when the pool accepts a task and with late ack when it ends.
  def credit(c: WorkerId): bool = live(c).tags.size() < PREFETCH
  // @cite kombu kombu/transport/virtual/base.py:197-204 "return not pcount or len(self._delivered) - len(self._dirty) < pcount"
  // @cite kombu kombu/transport/redis.py:474-495 "def append(self, message, delivery_tag):"
  action deliver(c: WorkerId, m: MsgId): bool = {
    val l = live(c)
    all {
      alive(c), l.phase == Consuming, l.conn == Up, credit(c),
      queue.contains(m), l.jobs.get(m) == NoJob,
      queue' = queue.exclude(Set(m)),
      unacked' = unacked.set(m, HeldBy(c)),
      workers' = workers.set(c, Alive({
        ...l, tags: l.tags.union(Set(m)),
        jobs: l.jobs.set(m, if (CFG.eta.contains(m)) Scheduled else Received),
      })),
      unpublished' = unpublished, brokerUp' = brokerUp, lost' = lost,
      keepRuns, cause' = cause, exits' = exits, used' = used,
      did(Deliver({ c: c, m: m }), POP_COST),
    }
  }

  // The pop arrives, its reply does not. BRPOP has removed the message and
  // nothing records it (kombu issue 305); the script has recorded it under
  // a tag this worker never learns.
  // @cite kombu kombu/transport/redis.py:1457-1460 "def _brpop_start(self, timeout=None):"
  action lostPop(c: WorkerId, m: MsgId): bool = {
    val l = live(c)
    all {
      alive(c), l.phase == Consuming, l.conn == RepliesDropped,
      not(l.stuckPop), credit(c), queue.contains(m),
      queue' = queue.exclude(Set(m)),
      unacked' = if (D.pop == PopScript) unacked.set(m, HeldBy(c)) else unacked,
      lost' = if (D.pop == PopScript) lost else lost.union(Set(m)),
      workers' = workers.set(c, Alive({ ...l, stuckPop: true })),
      unpublished' = unpublished, brokerUp' = brokerUp,
      keepRuns, cause' = cause, exits' = exits, used' = used,
      did(LostPop({ c: c, m: m }), 1),
    }
  }

  // ---- the pool hand-off ----
  action setJob(c: WorkerId, m: MsgId, s: Stage): bool =
    workers' = workers.set(c, Alive({ ...live(c), jobs: live(c).jobs.set(m, s) }))

  // @cite celery celery/worker/consumer/consumer.py:734-738 "def apply_eta_task(self, task):"
  action etaDue(c: WorkerId, m: MsgId): bool = all {
    alive(c), job(c, m) == Scheduled,
    setJob(c, m, Received),
    keepBroker, keepRuns, cause' = cause, exits' = exits, used' = used,
    did(EtaDue({ c: c, m: m }), 0),
  }

  // The job is written to the idle child, which starts the task.
  // @cite celery celery/concurrency/asynpool.py:977-980 "def _write_job(proc, fd, job):"
  action write(c: WorkerId, m: MsgId): bool = {
    val again = starts.get(m) > 0
    all {
      alive(c), job(c, m) == Received, live(c).phase == Consuming,
      childIdle(live(c)),
      setJob(c, m, Written),
      starts' = starts.setBy(m, n => n + 1), finished' = finished, strayRestore' = strayRestore,
      dupAvoidable' = if (again and cause.get(m) == Avoidable) dupAvoidable + 1
                      else dupAvoidable,
      dupUnavoidable' = if (again and cause.get(m) == Unavoidable)
                          dupUnavoidable + 1
                        else dupUnavoidable,
      keepBroker, cause' = cause, exits' = exits, used' = used,
      did(Write({ c: c, m: m }), 0),
    }
  }

  // What an ack of m by worker c does.
  type Acked = {
    worker: Worker, unacked: MsgId -> Record, ops: int, outcome: AckOutcome,
  }
  def ackOf(c: WorkerId, l: Live, m: MsgId): Acked =
    if (l.tags.contains(m))
      // through the live channel (QoS.ack, redis.py:497-499)
      if (l.conn == Up)
        { worker: Alive({ ...l, tags: l.tags.exclude(Set(m)) }),
          unacked: unacked.set(m, NoRec), ops: 1, outcome: AckDone }
      else if (l.conn == RepliesDropped and brokerUp)
        { worker: Alive({ ...l, blocked: l.blocked.union(Set(m)) }),
          unacked: unacked.set(m, NoRec), ops: 1,
          outcome: AckAppliedReplyLost }
      else
        // no socket timeout: the call blocks until the connection heals or
        // closes
        { worker: Alive({ ...l, blocked: l.blocked.union(Set(m)) }),
          unacked: unacked, ops: 0, outcome: AckBlocked }
    else if (l.oldTags.contains(m))
      // through a channel whose connection was collected
      if (D.collectedAck == WorkerExits)
        { worker: Dead, unacked: unacked, ops: 0, outcome: CollectedExits }
      else if (l.conn == Up)
        { worker: Alive({ ...l, oldTags: l.oldTags.exclude(Set(m)) }),
          unacked: unacked.set(m, NoRec), ops: 1, outcome: CollectedAcks }
      else if (D.failedAck == Resent)
        { worker: Alive({ ...l, oldTags: l.oldTags.exclude(Set(m)),
                          pending: l.pending.union(Set(m)) }),
          unacked: unacked, ops: 0, outcome: CollectedFails }
      else
        { worker: Alive(l), unacked: unacked, ops: 0, outcome: CollectedFails }
    else
      { worker: Alive(l), unacked: unacked, ops: 0, outcome: TagGone }

  // The parent reads the accept (on_accepted): early ack.
  // @cite celery celery/worker/request.py:524-533 "if not self.task.acks_late:"
  action accept(c: WorkerId, m: MsgId): bool = {
    val l = { ...live(c), jobs: live(c).jobs.set(m, Accepted) }
    val a = ackOf(c, l, m)
    all {
      alive(c), job(c, m) == Written,
      if (late) all {
        workers' = workers.set(c, Alive(l)), unacked' = unacked,
        exits' = exits, did(Accept({ c: c, m: m, outcome: NoAckDue }), 0),
      } else all {
        workers' = workers.set(c, a.worker), unacked' = a.unacked,
        exits' = if (a.outcome == CollectedExits) exits + 1 else exits,
        did(Accept({ c: c, m: m, outcome: a.outcome }), a.ops),
      },
      unpublished' = unpublished, queue' = queue, brokerUp' = brokerUp,
      lost' = lost, keepRuns, cause' = cause, used' = used,
    }
  }

  // @cite celery celery/app/trace.py:564 "R = retval = fun(*args, **kwargs)"
  action finish(c: WorkerId, m: MsgId): bool = all {
    alive(c), job(c, m) == Accepted,
    setJob(c, m, Finished),
    finished' = finished.setBy(m, n => n + 1), starts' = starts, strayRestore' = strayRestore,
    dupAvoidable' = dupAvoidable, dupUnavoidable' = dupUnavoidable,
    keepBroker, cause' = cause, exits' = exits, used' = used,
    did(Finish({ c: c, m: m }), 0),
  }

  // A job flush() dropped: the child runs the task to the end, and its
  // result has no job to go to, so nothing acks the message.
  // @cite celery celery/concurrency/asynpool.py:1079-1090 "if not job._accepted:"
  action finishDropped(c: WorkerId, m: MsgId): bool = all {
    alive(c), job(c, m) == Dropped,
    setJob(c, m, NoJob),
    finished' = finished.setBy(m, n => n + 1), starts' = starts, strayRestore' = strayRestore,
    dupAvoidable' = dupAvoidable, dupUnavoidable' = dupUnavoidable,
    keepBroker, cause' = cause, exits' = exits, used' = used,
    did(FinishDropped({ c: c, m: m }), 0),
  }

  // @env a task body that never returns (no time limit)
  action hang(c: WorkerId, m: MsgId): bool = all {
    alive(c), job(c, m) == Accepted, used.hangs < CFG.max.hangs,
    setJob(c, m, Hung),
    keepBroker, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, hangs: used.hangs + 1 },
    did(Hang({ c: c, m: m }), 0),
  }

  // The parent reads the result (on_success): late ack.
  // @cite celery celery/worker/request.py:616-631 "if self.task.acks_late:"
  action readResult(c: WorkerId, m: MsgId): bool = {
    val l = { ...live(c), jobs: live(c).jobs.set(m, NoJob) }
    val a = ackOf(c, l, m)
    all {
      alive(c), job(c, m) == Finished,
      if (late) all {
        workers' = workers.set(c, a.worker), unacked' = a.unacked,
        exits' = if (a.outcome == CollectedExits) exits + 1 else exits,
        did(ReadResult({ c: c, m: m, outcome: a.outcome }), a.ops),
      } else all {
        workers' = workers.set(c, Alive(l)), unacked' = unacked,
        exits' = exits, did(ReadResult({ c: c, m: m, outcome: NoAckDue }), 0),
      },
      unpublished' = unpublished, queue' = queue, brokerUp' = brokerUp,
      lost' = lost, keepRuns, cause' = cause, used' = used,
    }
  }

  // ---- on_close() and flush() ----
  // Reserved requests that are not active are forgotten, and flush() drops
  // every job the parent has not seen accepted: one in the outbound buffer
  // is gone, one the child already has runs anyway. DropUnwritten keeps the
  // second kind (asynpool.py:1085-1090 with `elif job._write_to is None`).
  pure def flushed(d: Design, s: Stage): Stage = match s {
    | NoJob => NoJob | Scheduled => NoJob | Received => NoJob
    | Written => if (d.flush == DropUnaccepted) Dropped else Written
    | Accepted => Accepted | Finished => Finished | Hung => Hung
    | Dropped => Dropped
  }
  pure def flushJobs(d: Design, l: Live): MsgId -> Stage =
    MSGS.mapBy(m => flushed(d, l.jobs.get(m)))

  // Warm shutdown: stop consuming, flush, let the pool finish, then restore
  // what is left unacked (restore_at_shutdown) and exit.
  // @cite celery celery/worker/consumer/consumer.py:581-590 "def on_close(self):"
  action sigterm(c: WorkerId): bool = {
    val l = live(c)
    all {
      alive(c), l.phase == Consuming, l.conn == Up,
      used.shutdowns < CFG.max.shutdowns,
      workers' = workers.set(c, Alive({
        ...l, phase: Draining, jobs: flushJobs(D, l) })),
      keepBroker, keepRuns, cause' = cause, exits' = exits,
      used' = { ...used, shutdowns: used.shutdowns + 1 },
      did(Sigterm(c), 0),
    }
  }
  // @cite kombu kombu/transport/virtual/base.py:277-290 "def restore_unacked_once(self, stderr=None):"
  action exit(c: WorkerId): bool = {
    val l = live(c)
    val back = if (l.conn == Up) restorable(c, l.tags) else Set()
    all {
      alive(c), l.phase == Draining, childIdle(l),
      restoreAll(back, false),
      workers' = workers.set(c, Dead),
      unpublished' = unpublished, brokerUp' = brokerUp, lost' = lost,
      keepStarts, strayRestore' = (strayRestore or stray(c, back)),
      exits' = exits, used' = used,
      did(Exit(c), back.size()),
    }
  }

  // ---- faults ----
  // @env SIGKILL of the worker and its pool
  action kill(c: WorkerId): bool = all {
    alive(c), used.kills < CFG.max.kills,
    workers' = workers.set(c, Dead),
    keepBroker, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, kills: used.kills + 1 },
    did(Kill(c), 0),
  }
  // The supervisor starts a worker that is gone.
  // @env the supervisor starts the worker again
  action restart(c: WorkerId): bool = all {
    not(alive(c)), used.restarts < CFG.max.restarts,
    workers' = workers.set(c, Alive(if (brokerUp) FRESH
                                    else { ...FRESH, conn: Closed })),
    keepBroker, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, restarts: used.restarts + 1 },
    did(Restart(c), 0),
  }

  // The broker dies and comes back with what it had (queue and unacked).
  // Every connection is dead.
  pure def broken(w: Worker): Worker = match w {
    | Dead => Dead
    | Alive(l) => Alive(if (l.conn == Closed) l else { ...l, conn: Broken })
  }
  // @env the Redis server dies; it keeps the queue and the unacked hash
  action crash: bool = all {
    brokerUp, used.crashes < CFG.max.crashes,
    brokerUp' = false,
    workers' = WORKERS.mapBy(c => broken(workers.get(c))),
    unpublished' = unpublished, queue' = queue, unacked' = unacked,
    lost' = lost, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, crashes: used.crashes + 1 },
    did(Crash, 0),
  }
  // @env the Redis server comes back
  action brokerRestart: bool = all {
    not(brokerUp), brokerUp' = true,
    unpublished' = unpublished, queue' = queue, unacked' = unacked,
    lost' = lost, workers' = workers, keepRuns, cause' = cause,
    exits' = exits, used' = used, did(BrokerRestart, 0),
  }

  action setConn(c: WorkerId, state: Conn): bool =
    workers' = workers.set(c, Alive({ ...live(c), conn: state }))
  // @env a partition that drops the replies to this worker
  action partition(c: WorkerId): bool = all {
    alive(c), live(c).conn == Up, used.partitions < CFG.max.partitions,
    setConn(c, RepliesDropped),
    keepBroker, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, partitions: used.partitions + 1 },
    did(Partition(c), 0),
  }
  // @env a partition that drops everything to and from this worker
  action cutOff(c: WorkerId): bool = all {
    alive(c), live(c).conn == Up, used.cuts < CFG.max.cuts,
    setConn(c, Cut),
    keepBroker, keepRuns, cause' = cause, exits' = exits,
    used' = { ...used, cuts: used.cuts + 1 },
    did(CutOff(c), 0),
  }

  // The partition heals: a blocked ack goes through.
  // @env the partition heals
  action heal(c: WorkerId): bool = {
    val l = live(c)
    all {
      alive(c), l.conn == RepliesDropped or l.conn == Cut, brokerUp,
      workers' = workers.set(c, Alive({
        ...l, conn: Up, stuckPop: false, blocked: Set(),
        tags: l.tags.exclude(l.blocked) })),
      unacked' = MSGS.mapBy(m => if (l.blocked.contains(m)) NoRec
                                 else unacked.get(m)),
      unpublished' = unpublished, queue' = queue, brokerUp' = brokerUp,
      lost' = lost, keepRuns, cause' = cause, exits' = exits, used' = used,
      did(Heal(c), if (l.conn == Cut) l.blocked.size() else 0),
    }
  }

  // The worker's channel after on_close(), flush() and a failed ack.
  // A blocked ack has failed: main leaves the tag with the channel, which
  // restores it when it closes; Resent keeps it to send again.
  pure def closing(d: Design, l: Live): Live = {
    ...l, stuckPop: false, blocked: Set(), jobs: flushJobs(d, l),
    tags: if (d.failedAck == Resent) l.tags.exclude(l.blocked) else l.tags,
    pending: if (d.failedAck == Resent) l.pending.union(l.blocked)
             else l.pending,
  }
  // The worker notices a dead connection that cannot reach the broker: the
  // closing channel cannot restore anything (redis.py:491-495).
  // @cite kombu kombu/transport/redis.py:491-495 "def restore_unacked(self, client=None):"
  // @cite celery celery/worker/consumer/consumer.py:466-470 "def on_connection_error_after_connected(self, exc):"
  action noticeLoss(c: WorkerId): bool = {
    val l = closing(D, live(c))
    all {
      alive(c),
      live(c).conn == RepliesDropped or live(c).conn == Cut
        or live(c).conn == Broken,
      workers' = workers.set(c, Alive({
        ...l, conn: Closed, tags: Set(), oldTags: l.oldTags.union(l.tags) })),
      keepBroker, keepRuns, cause' = cause, exits' = exits, used' = used,
      did(NoticeLoss(c), 0),
    }
  }
  // A connection reset: the worker notices at once and Redis answers a new
  // connection, so the closing channel restores its messages.
  // @env the connection is reset and Redis answers a new one
  // @cite kombu kombu/transport/virtual/base.py:277-290 "def restore_unacked_once(self, stderr=None):"
  action reset(c: WorkerId): bool = {
    val l = closing(D, live(c))
    // CloseRestoresIdle leaves a message whose task still runs here
    val kept = if (D.closeRestore == CloseRestoresIdle)
                 l.tags.filter(m => l.jobs.get(m) != NoJob)
               else Set()
    val back = restorable(c, l.tags.exclude(kept))
    all {
      alive(c), live(c).conn == Up, used.resets < CFG.max.resets,
      restoreAll(back, true),
      workers' = workers.set(c, Alive({
        ...l, conn: Closed, tags: Set(), oldTags: l.oldTags.union(kept) })),
      unpublished' = unpublished, brokerUp' = brokerUp, lost' = lost,
      keepStarts, strayRestore' = (strayRestore or stray(c, back)),
      exits' = exits,
      used' = { ...used, resets: used.resets + 1 },
      did(Reset(c), back.size()),
    }
  }
  // The worker reconnects. Resent sends the acks it kept; the
  // reconnectRestore variants restore what the closed channel could not.
  // @cite celery celery/worker/consumer/consumer.py:466-470 "def on_connection_error_after_connected(self, exc):"
  action reconnect(c: WorkerId): bool = {
    val l = live(c)
    val tried = match D.reconnectRestore {
      | WaitsForTimeout => Set()
      | ReconnectRestoresAll => l.oldTags
      | ReconnectRestoresIdle => l.oldTags.filter(m => l.jobs.get(m) == NoJob)
    }
    val back = restorable(c, tried)
    all {
      alive(c), l.conn == Closed, brokerUp,
      queue' = queue.union(back),
      unacked' = MSGS.mapBy(m =>
        if (back.contains(m) or l.pending.contains(m)) NoRec
        else unacked.get(m)),
      cause' = MSGS.mapBy(m => if (back.contains(m)) classify(m, true)
                               else cause.get(m)),
      workers' = workers.set(c, Alive({
        ...l, conn: Up, pending: Set(), oldTags: l.oldTags.exclude(tried) })),
      unpublished' = unpublished, brokerUp' = brokerUp, lost' = lost,
      keepStarts, strayRestore' = (strayRestore or stray(c, back)),
      exits' = exits, used' = used,
      did(Reconnect(c), back.size() + l.pending.size()),
    }
  }

  // ---- the visibility timeout ----
  // A live, connected worker renews what its channel has delivered and not
  // acked (the prototype: ZADD XX over QoS._delivered every third of the
  // timeout), all of it or only what has not started.
  pure def renews(d: Design, l: Live, m: MsgId): bool =
    l.conn == Up and l.tags.contains(m) and (match d.renewal {
      | NoRenewal => false
      | RenewAll => true
      | RenewUnstarted =>
          l.jobs.get(m) == Scheduled or l.jobs.get(m) == Received
      | RenewScheduled => l.jobs.get(m) == Scheduled
    })
  // Why the record of m can be restored now; HeldPastTimeout needs budget.
  def restoreWhy(m: MsgId, c: WorkerId): RestoreWhy = {
    val l = live(c)
    val s = l.jobs.get(m)
    if (not(alive(c))) HolderDead
    else if (s == Hung) HungTask
    else if ((s == Received or s == Scheduled) and childHung(l)) BehindHungTask
    // not while a warm shutdown or a reconnect is about to restore it
    else if (s == NoJob and not(l.blocked.contains(m))
             and not(l.pending.contains(m))
             and not(l.phase == Draining and l.tags.contains(m))
             and not(l.conn == Closed and l.oldTags.contains(m)
                     and D.reconnectRestore != WaitsForTimeout))
      NobodyWillAck
    else HeldPastTimeout
  }
  // @cite kombu kombu/transport/redis.py:521-530 "def restore_visible(self, start=0, num=10, interval=10):"
  action restoreVisible(m: MsgId, c: WorkerId): bool = {
    val why = restoreWhy(m, c)
    all {
      unacked.get(m) == HeldBy(c), brokerUp,
      WORKERS.exists(d => alive(d) and live(d).conn == Up),
      not(alive(c) and renews(D, live(c), m)),
      why != HeldPastTimeout or (used.timeouts < CFG.max.timeouts
        and (CFG.outlasts == AnythingHeld or job(c, m) == Scheduled)),
      restoreAll(Set(m), false),
      workers' = workers, unpublished' = unpublished, brokerUp' = brokerUp,
      lost' = lost, keepRuns, exits' = exits,
      used' = if (why == HeldPastTimeout)
                { ...used, timeouts: used.timeouts + 1 }
              else used,
      did(RestoreVisible({ m: m, reason: why }), 1),
    }
  }

  // The choices are made next to the actions that use them: the simulator
  // and TLC try every combination of the choices in scope for every action.
  action step: bool = any {
    {
      nondet m = MSGS.oneOf()
      publish(m)
    },
    {
      nondet c = WORKERS.oneOf()
      nondet m = MSGS.oneOf()
      any {
        deliver(c, m), lostPop(c, m), etaDue(c, m), write(c, m), accept(c, m),
        finish(c, m), finishDropped(c, m), hang(c, m), readResult(c, m),
        restoreVisible(m, c),
      }
    },
    {
      nondet c = WORKERS.oneOf()
      any {
        sigterm(c), exit(c), kill(c), restart(c), partition(c), cutOff(c),
        reset(c), heal(c), noticeLoss(c), reconnect(c),
      }
    },
    crash, brokerRestart,
  }

  // ---- properties ---------------------------------------------------------
  def published: Set[MsgId] = MSGS.exclude(unpublished)
  def inBroker(m: MsgId): bool = queue.contains(m) or hasRec(unacked.get(m))
  // The observation the shared properties read (../lib/observe.qnt): a run
  // is a start in a pool process; done is a finished run; held is in the
  // broker, or (early ack) running in a live worker; settled is finished
  // and gone from the broker.
  val obs: Set[MsgObs] = MSGS.map(m => {
    published: published.contains(m), runs: starts.get(m),
    done: finished.get(m) > 0,
    held: inBroker(m) or (not(late) and WORKERS.exists(c =>
      alive(c) and running(job(c, m)))),
    settled: finished.get(m) > 0 and not(inBroker(m)),
  })

  // Exactly once while no fault was used: a task never starts twice, and
  // when the system is quiet each one finished once and the broker is
  // empty. Warm shutdowns and visibility timeouts are not faults.
  val exactlyOnce: bool = exactlyOnceOf(faults(used) == 0, quiet, obs)
  // At least once, as a safety property: a published message is on the
  // queue or in unacked until a run of it has finished. With early ack a
  // run in a live worker counts too (at most once is the contract there).
  val neverLost: bool = neverLostOf(obs)
  // And as an outcome: a quiet system has finished every task and left
  // nothing in the broker.
  val quietDone: bool = quietDoneOf(quiet, obs)
  // No task runs twice for a reason a design could remove, and none at all.
  val noAvoidableDup: bool = dupAvoidable == 0
  val noUnavoidableDup: bool = dupUnavoidable == 0
  // No worker is stopped by an error in the ack path.
  val workerSurvives: bool = exits == 0
  // The normal path costs one publish, one pop (two round trips on main)
  // and one ack per message.
  val normalCost: bool =
    (used == NONE and quiet) implies ops == MSGS.size() * (2 + POP_COST)
  // Witnesses (violated means reachable).
  val neverQuiet: bool = not(quiet)
  val neverTwice: bool = MSGS.forall(m => starts.get(m) <= 1)
  // A channel that closes or reconnects puts back only what its own pop
  // recorded, not the copy another worker holds under the same tag.
  val restoresOwnOnly: bool = not(strayRestore)

  // ---- quiet: nothing can happen without a new fault ----
  def freeRestore(m: MsgId): bool = WORKERS.exists(c => and {
    unacked.get(m) == HeldBy(c), brokerUp,
    WORKERS.exists(d => alive(d) and live(d).conn == Up),
    not(alive(c) and renews(D, live(c), m)),
    restoreWhy(m, c) != HeldPastTimeout,
  })
  def canAct(c: WorkerId): bool = {
    val l = live(c)
    if (not(alive(c))) used.restarts < CFG.max.restarts
    else or {
      l.conn == RepliesDropped, l.conn == Cut, l.conn == Broken,
      l.conn == Closed and brokerUp,
      l.phase == Draining and childIdle(l),
      MSGS.exists(m => match l.jobs.get(m) {
        | NoJob => l.phase == Consuming and l.conn == Up
                   and l.tags.size() < PREFETCH and queue.contains(m)
        | Scheduled => true
        | Received => l.phase == Consuming and childIdle(l)
        | Written => true | Accepted => true | Finished => true
        | Hung => false | Dropped => true
      }),
    }
  }
  val quiet: bool = and {
    unpublished == Set(), brokerUp,
    WORKERS.forall(c => not(canAct(c))),
    MSGS.forall(m => not(freeRestore(m))),
  }
  action stutter: bool = all {
    quiet,
    keepBroker, workers' = workers, keepRuns, cause' = cause, exits' = exits,
    ops' = ops, used' = used, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
}
