// -*- mode: Bluespec; -*-
// Bounded redelivery, and every task ends: one acks_late or early-ack task
// on a Redis broker or a RabbitMQ classic or quorum queue, with a coarse
// clock, so that a run's length can be set against the Redis visibility
// timeout or RabbitMQ's consumer_timeout.
//
// Celery 768e476, kombu 445027e, RabbitMQ as documented (the server is not
// pinned: its rules are @env). One task message, one or two workers with
// one pool process each.
//
// Redis (kombu/transport/redis.py): a delivery writes a record of the
// message under its delivery tag (QoS.append, :474-495); an ack removes
// the record by tag, whoever wrote it, from any connection (:497-499); any
// consumer's periodic restore_visible puts a record older than the
// visibility timeout back on the queue, marked redelivered, and removes it
// (:521-560, 1263-1270). A message keeps its tag when it is delivered
// again, so a run's ack removes the record of a later delivery. A
// worker's prefetch credit counts the tags it holds unacked
// (virtual/base.py:197-204), one per message.
//
// AMQP (RabbitMQ): a delivery is unacked on the channel that received it;
// an ack is valid only on that channel (amqp 5.4.2 channel.py:1377-1382:
// "a client MUST NOT receive a message on one channel and then acknowledge
// it on another"); a channel that closes returns everything unacked on it
// to the queue, marked redelivered. A delivery unacked longer than
// consumer_timeout closes its channel with PRECONDITION_FAILED
// (docs/userguide/calling.rst:318-334). Celery treats that channel error
// as unrecoverable unless broker_channel_error_retry is set
// (consumer.py:394-397): the worker stops warm and a supervisor starts it
// again; with the setting it reconnects and the run goes on. A quorum
// queue counts each return and drops the message, or dead-letters it,
// past its delivery limit (rabbitmq.rst:246-257).
//
// Time: `tick` advances a coarse clock by one unit: every run in progress
// gets one unit closer to its end, and every Redis record and AMQP
// unacked delivery one unit older (ages stop at the timeout). Every other
// action takes no time, and time does not pass while one that is due can
// happen (a delivery to a worker with credit, a run's start or end, an
// ack, a restart, a timeout that is due): maximal progress. A timeout
// fires when its age reaches CFG.timeout, which stands for the timeout
// plus the delay of the sweep or of the broker's check.
//
// Not modeled: several messages (an early-ack task waiting behind a long
// one; worker.qnt has it), ETA and countdown, more than one pool process,
// the cut of a Redis connection (worker.qnt: ack-after-loss),
// worker_cancel_long_running_tasks_on_connection_loss, a pool process
// lost on its own (chain.qnt), retries.
module redeliverytypes {
  import deliverytypes.* from "../lib/delivery"
  export deliverytypes.*

  type WorkerId = { w: int }
  pure def W(i: int): WorkerId = { w: i }

  type Broker = Redis | Classic | Quorum
  type AckMode = Early | Late
  type Conn = Up | Cut

  // A delivery a worker holds. Waiting: reserved, not yet in the pool
  // process. Running: the process runs it (`left` units to go). Ran: the
  // result is stored, or deduplication skipped the run; the late ack is
  // due. `ch` is the worker's channel when it was delivered.
  type Stage = Waiting | Running | Ran
  type Held = { k: int, ch: int, stage: Stage, left: int, redelivered: bool }
  type Live = { conn: Conn, stopping: bool, held: Set[Held] }
  type Worker = Dead | Alive(Live)
  pure def isAlive(x: Worker): bool = match x { | Dead => false | Alive(_) => true }
  pure val NOT_LIVE: Live = { conn: Up, stopping: true, held: Set() }
  pure val FRESH: Live = { conn: Up, stopping: false, held: Set() }
  pure def liveOf(x: Worker): Live = match x { | Dead => NOT_LIVE | Alive(l) => l }

  // Redis: the record under the message's tag, with its age.
  type Record = NoRec | Recorded(int)
  pure def hasRec(r: Record): bool = match r { | NoRec => false | Recorded(_) => true }
  pure def ageOf(r: Record): int = match r { | NoRec => 0 | Recorded(a) => a }
  // AMQP: a delivery unacked on a channel of a worker.
  type Unacked = { k: int, by: WorkerId, ch: int, age: int }

  type Result = Pending | Success
  // What the broker did with the message at the delivery limit.
  type End = Open | DeadLettered | Dropped

  // The state without counters and ids: if a delivery starts from a view
  // an earlier delivery started from, the steps between them can repeat
  // for ever.
  // The workers are alike, so the view is a set of worker views: a state
  // and the one with two workers swapped have the same view. (A set of
  // two equal views stands for two workers in that state.)
  type HeldView = { cur: bool, stage: Stage, left: int, redelivered: bool }
  type UnackedView = { cur: bool, age: int }
  type WorkerView = {
    alive: bool, conn: Conn, stopping: bool, held: Set[HeldView], n: int,
    unacked: Set[UnackedView],
  }
  type Budget = { kills: int, cuts: int, losses: int }
  type Snap = {
    stored: Result, ready: int, redeliv: bool, rec: Record,
    returns: int, ended: End, workers: Set[WorkerView], used: Budget,
  }

  type Lost = LostReady | LostRecord
  type AckOutcome = AckDone | AckOnClosedChannel | AckIntoCut | NoAck
  type Action =
    | Init
    | Publish
    | Deliver(WorkerId)
    | Start({ w: WorkerId, k: int, outcome: AckOutcome })
    | DedupSkip({ w: WorkerId, k: int })
    | Finish({ w: WorkerId, k: int })
    | Ack({ w: WorkerId, k: int, outcome: AckOutcome })
    | RestoreVisible
    | ConsumerTimeout(WorkerId)
    | Exit(WorkerId)
    | Restart(WorkerId)
    | Notice(WorkerId)
    | Kill(WorkerId)
    | CutOff(WorkerId)
    | LoseData(Lost)
    | Tick

  // Faults, each a bounded number of times. A timeout, a warm stop and a
  // supervisor's restart are not faults.
  pure val NONE: Budget = { kills: 0, cuts: 0, losses: 0 }

  type Config = {
    broker: Broker,
    ack: AckMode,
    workers: int,          // 1 or 2, one pool process each
    prefetch: int,         // the prefetch count (multiplier x 1 process)
    disablePrefetch: bool, // worker_disable_prefetch (Redis only)
    runLen: int,           // units of time a run takes
    timeout: int,          // visibility timeout or consumer_timeout, units
    dedup: bool,           // worker_deduplicate_successful_tasks
    channelRetry: bool,    // broker_channel_error_retry
    limit: int,            // a quorum queue's delivery limit (0: none)
    dlx: bool,             // the queue has a dead letter exchange
    confirm: bool,         // publisher confirms (a classic queue)
    cap: int,              // the search stops a run at this many deliveries
    max: Budget,
  }
  pure def config(b: Broker, a: AckMode): Config = {
    broker: b, ack: a, workers: 2, prefetch: 1, disablePrefetch: false,
    runLen: 1, timeout: 2, dedup: false, channelRetry: false,
    limit: if (b == Quorum) 2 else 0, dlx: false, confirm: true, cap: 4,
    max: NONE,
  }
}

module redelivery {
  import redeliverytypes.*
  import observe.* from "../lib/observe"

  const CFG: Config

  pure val WORKERS: Set[WorkerId] = 1.to(CFG.workers).map(i => W(i))
  pure val L: int = CFG.runLen
  pure val T: int = CFG.timeout
  pure val REDIS: bool = CFG.broker == Redis
  pure val LATE: bool = CFG.ack == Late

  // ---- state ----
  // broker
  var published: bool
  var ready: int               // copies on the queue
  var redeliv: bool            // the copy on the queue is marked redelivered
  var rec: Record              // Redis
  var unacked: Set[Unacked]    // AMQP
  var returns: int             // a quorum queue's delivery count
  var ended: End
  // workers
  var workers: WorkerId -> Worker
  var gen: WorkerId -> int     // the worker's current channel
  // the result backend
  var stored: Result
  // observations
  var deliveries: int
  var runs: int                // bodies started
  var snaps: Set[Snap]
  var looped: bool
  var timedOut: bool           // a restore or a consumer timeout fired
  var ackFailed: bool          // an ack went to a closed channel or a cut
  var rerun: bool              // a run started while SUCCESS was stored
  var prevCopies: MsgId -> Copies  // the projection before the last step
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    published' = false, ready' = 0, redeliv' = false, rec' = NoRec,
    unacked' = Set(), returns' = 0, ended' = Open,
    workers' = WORKERS.mapBy(_ => Alive(FRESH)), gen' = WORKERS.mapBy(_ => 1),
    stored' = Pending, deliveries' = 0, runs' = 0, snaps' = Set(),
    looped' = false, timedOut' = false, ackFailed' = false, rerun' = false,
    prevCopies' = Set(M(1)).mapBy(_ => NO_COPIES),
    used' = NONE, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepBroker: bool = all {
    published' = published, ready' = ready, redeliv' = redeliv, rec' = rec,
    unacked' = unacked, returns' = returns, ended' = ended,
  }
  action keepObs: bool = all {
    deliveries' = deliveries, runs' = runs, snaps' = snaps, looped' = looped,
    timedOut' = timedOut, ackFailed' = ackFailed, rerun' = rerun,
  }

  // ---- reading the state ----
  def alive(w: WorkerId): bool = isAlive(workers.get(w))
  def lv(w: WorkerId): Live = liveOf(workers.get(w))
  def held(w: WorkerId): Set[Held] = lv(w).held
  def busy(w: WorkerId): bool =
    held(w).exists(h => h.stage == Running or h.stage == Ran)
  def runningIn(w: WorkerId): Set[Held] = held(w).filter(h => h.stage == Running)
  // Early ack: a delivery is acked when the pool process starts it.
  pure def unackedHere(h: Held): bool = LATE or h.stage == Waiting

  // Prefetch credit. Redis: the channel's QoS counts delivered tags not
  // acked, and one message has one tag. AMQP: the broker counts the
  // deliveries unacked on the channel.
  def counted(w: WorkerId): int =
    if (REDIS) (if (held(w).exists(h => unackedHere(h))) 1 else 0)
    else unacked.filter(u => u.by == w and u.ch == gen.get(w)).size()
  // worker_disable_prefetch: no fetch while the reserved requests fill the
  // pool (tasks.py:59-83).
  def slotFree(w: WorkerId): bool =
    not(held(w).exists(h => h.stage == Waiting or h.stage == Running))
  def credit(w: WorkerId): bool = and {
    counted(w) < CFG.prefetch,
    not(CFG.disablePrefetch and REDIS) or slotFree(w),
  }

  // The view of the state that `noLoop` compares.
  def snapshot: Snap = {
    stored: stored, ready: ready, redeliv: redeliv, rec: rec,
    returns: returns, ended: ended, used: used,
    workers: WORKERS.map(w => {
      alive: alive(w), conn: lv(w).conn, stopping: lv(w).stopping,
      n: held(w).size(),
      held: held(w).map(h => { cur: h.ch == gen.get(w), stage: h.stage,
                               left: h.left, redelivered: h.redelivered }),
      unacked: unacked.filter(u => u.by == w)
                      .map(u => { cur: u.ch == gen.get(w), age: u.age }),
    }),
  }

  // The broker's copies, for the projection onto lib/delivery.qnt.
  def copies: MsgId -> Copies = Set(M(1)).mapBy(_ => {
    ready: ready,
    taken: if (REDIS) (if (hasRec(rec)) 1 else 0) else unacked.size(),
  })

  // ---- enabled: what is due before time can pass ----
  def canPublish: bool = not(published)
  def canDeliver(w: WorkerId): bool = and {
    alive(w), lv(w).conn == Up, not(lv(w).stopping), ready > 0,
    deliveries < CFG.cap, credit(w),
  }
  def waiting(w: WorkerId): Set[Held] = held(w).filter(h => h.stage == Waiting)
  def canStart(w: WorkerId): bool = and {
    alive(w), not(lv(w).stopping), not(busy(w)), waiting(w) != Set(),
  }
  def canFinish(w: WorkerId): bool =
    alive(w) and runningIn(w).exists(h => h.left == 0)
  def canAck(w: WorkerId): bool = alive(w) and held(w).exists(h => h.stage == Ran)
  def canRestore: bool = and {
    REDIS, ageOf(rec) == T, hasRec(rec),
    WORKERS.exists(w => alive(w) and lv(w).conn == Up),
  }
  def dueOn(w: WorkerId): Set[Unacked] =
    unacked.filter(u => u.by == w and u.ch == gen.get(w) and u.age == T)
  def canTimeout(w: WorkerId): bool = and {
    not(REDIS), alive(w), lv(w).conn == Up, dueOn(w) != Set(),
  }
  def canExit(w: WorkerId): bool =
    alive(w) and lv(w).stopping and held(w) == Set()
  def canRestart(w: WorkerId): bool = not(alive(w))
  def canNotice(w: WorkerId): bool = alive(w) and lv(w).conn == Cut
  def urgent: bool = or {
    canPublish, canRestore,
    WORKERS.exists(w => or {
      canDeliver(w), canStart(w), canFinish(w), canAck(w), canTimeout(w),
      canExit(w), canRestart(w), canNotice(w),
    }),
  }
  // Something that time changes.
  def canAge: bool = or {
    WORKERS.exists(w => alive(w) and runningIn(w).exists(h => h.left > 0)),
    REDIS and hasRec(rec) and ageOf(rec) < T,
    not(REDIS) and unacked.exists(u => u.age < T),
  }

  // A channel closes: the broker returns what is unacked on it. A quorum
  // queue counts each return and drops or dead-letters the message past
  // its limit (rabbitmq.rst:246-257).
  // @env RabbitMQ returns a closed channel's unacked deliveries; a quorum queue's delivery limit (docs/getting-started/backends-and-brokers/rabbitmq.rst:246-257)
  action returnAll(w: WorkerId, ch: int): bool = {
    val back = unacked.filter(u => u.by == w and u.ch == ch)
    val n = back.size()
    val over = CFG.broker == Quorum and CFG.limit > 0 and n > 0
               and returns + n > CFG.limit
    all {
      unacked' = unacked.exclude(back),
      returns' = if (CFG.broker == Quorum) returns + n else returns,
      ready' = if (over) ready else ready + n,
      redeliv' = if (over) redeliv else (redeliv or n > 0),
      ended' = if (over) (if (CFG.dlx) DeadLettered else Dropped) else ended,
      published' = published, rec' = rec,
    }
  }
  // On a reconnect or a stop, Consumer.on_close forgets reserved requests
  // that are not active, and flush() drops jobs not yet accepted
  // (consumer.py:581-621).
  pure def dropWaiting(l: Live): Live =
    { ...l, held: l.held.filter(h => h.stage != Waiting) }

  // ---- the client ----
  // The publish returned (with confirm_publish: the broker confirmed it).
  // @cite kombu kombu/transport/redis.py:1677-1680 "def _put(self, queue, message, **kwargs):"
  action publish: bool = all {
    canPublish,
    published' = true, ready' = 1, redeliv' = false, rec' = rec,
    unacked' = unacked, returns' = returns, ended' = ended,
    workers' = workers, gen' = gen, stored' = stored, keepObs, used' = used,
    did(Publish),
  }

  // ---- a worker ----
  // A worker with credit gets the message. Redis writes the record under
  // the tag (HSET replaces a record already there); AMQP holds it unacked
  // on the worker's channel.
  // @cite kombu kombu/transport/redis.py:474-495 "def append(self, message, delivery_tag):"
  // @cite kombu kombu/transport/virtual/base.py:197-204 "return not pcount or len(self._delivered) - len(self._dirty) < pcount"
  action deliver(w: WorkerId): bool = {
    val k = deliveries + 1
    val h = { k: k, ch: gen.get(w), stage: Waiting, left: L, redelivered: redeliv }
    val s = snapshot
    all {
      canDeliver(w),
      ready' = ready - 1, redeliv' = false,
      rec' = if (REDIS) Recorded(0) else rec,
      unacked' = if (REDIS) unacked
                 else unacked.union(Set({ k: k, by: w, ch: gen.get(w), age: 0 })),
      published' = published, returns' = returns, ended' = ended,
      workers' = workers.set(w, Alive({ ...lv(w), held: held(w).union(Set(h)) })),
      gen' = gen, stored' = stored,
      deliveries' = k, snaps' = snaps.union(Set(s)),
      looped' = (looped or snaps.contains(s)),
      runs' = runs, timedOut' = timedOut, ackFailed' = ackFailed,
      rerun' = rerun, used' = used, did(Deliver(w)),
    }
  }

  // The ack of delivery h by worker w: what the broker does with it.
  // Redis: by tag, from any connection. AMQP: only on the channel that
  // received it, and only if the connection carries it.
  def ackOutcome(w: WorkerId, h: Held): AckOutcome =
    if (REDIS) AckDone
    else if (h.ch != gen.get(w)) AckOnClosedChannel
    else if (lv(w).conn == Cut) AckIntoCut
    else AckDone
  def unackedAfter(w: WorkerId, h: Held): Set[Unacked] =
    if (not(REDIS) and ackOutcome(w, h) == AckDone)
      unacked.filter(u => u.k != h.k)
    else unacked

  // The pool process starts the run. With early ack the parent acks when
  // it reads the accept (request.py:524-533); a failed ack is logged and
  // the task runs anyway (kombu message.py:129-138); an ack written into a
  // connection that is cut gives no error at all.
  // @cite celery celery/worker/request.py:524-533 "if not self.task.acks_late:"
  // @cite kombu kombu/message.py:129-138 "def ack_log_error(self, logger, errors, multiple=False):"
  action start(w: WorkerId): bool = {
    nondet h = waiting(w).oneOf()
    val o = if (LATE) NoAck else ackOutcome(w, h)
    val dedup = CFG.dedup and LATE and h.redelivered and stored == Success
    all {
      canStart(w), not(dedup),
      workers' = workers.set(w, Alive({ ...lv(w),
        held: held(w).exclude(Set(h)).union(Set({ ...h, stage: Running })) })),
      rec' = if (REDIS and not(LATE)) NoRec else rec,
      unacked' = if (LATE) unacked else unackedAfter(w, h),
      published' = published, ready' = ready, redeliv' = redeliv,
      returns' = returns, ended' = ended, gen' = gen, stored' = stored,
      runs' = runs + 1, rerun' = (rerun or stored == Success),
      ackFailed' = (ackFailed or o == AckOnClosedChannel or o == AckIntoCut),
      deliveries' = deliveries, snaps' = snaps, looped' = looped,
      timedOut' = timedOut, used' = used,
      did(Start({ w: w, k: h.k, outcome: o })),
    }
  }
  // worker_deduplicate_successful_tasks: an acks_late message marked
  // redelivered whose SUCCESS is stored returns at once, and is acked
  // (trace.py:357-360, 487-528).
  // @cite celery celery/app/trace.py:487-528 "if deduplicate_successful_tasks and redelivered:"
  action dedupSkip(w: WorkerId): bool = {
    nondet h = waiting(w).oneOf()
    all {
      canStart(w), CFG.dedup, LATE, h.redelivered, stored == Success,
      workers' = workers.set(w, Alive({ ...lv(w),
        held: held(w).exclude(Set(h)).union(Set({ ...h, stage: Ran })) })),
      keepBroker, gen' = gen, stored' = stored, keepObs, used' = used,
      did(DedupSkip({ w: w, k: h.k })),
    }
  }
  // The run ends and stores SUCCESS (mark_as_done, trace.py:604-608). With
  // early ack it is done; with late ack the ack is due.
  // @cite celery celery/app/trace.py:604-608 "task.backend.mark_as_done("
  action finish(w: WorkerId): bool = {
    nondet h = runningIn(w).oneOf()
    all {
      alive(w), h.left == 0,
      workers' = workers.set(w, Alive({ ...lv(w),
        held: if (LATE) held(w).exclude(Set(h)).union(Set({ ...h, stage: Ran }))
              else held(w).exclude(Set(h)) })),
      stored' = Success,
      keepBroker, gen' = gen, keepObs, used' = used,
      did(Finish({ w: w, k: h.k })),
    }
  }
  // on_success: the late ack (request.py:616-631). Redis: HDEL by tag,
  // which also removes the record of a later delivery of the same message
  // (redis.py:497-499). AMQP: on a channel that was closed the ack fails
  // and is logged ("Couldn't ack"); the broker has already returned it.
  // @cite celery celery/worker/request.py:616-631 "if self.task.acks_late:"
  // @cite kombu kombu/transport/redis.py:497-499 "self._remove_from_indices(delivery_tag).execute()"
  action ack(w: WorkerId): bool = {
    nondet h = held(w).filter(x => x.stage == Ran).oneOf()
    val o = ackOutcome(w, h)
    all {
      alive(w), h.stage == Ran,
      workers' = workers.set(w, Alive({ ...lv(w), held: held(w).exclude(Set(h)) })),
      rec' = if (REDIS) NoRec else rec,
      unacked' = unackedAfter(w, h),
      published' = published, ready' = ready, redeliv' = redeliv,
      returns' = returns, ended' = ended, gen' = gen, stored' = stored,
      ackFailed' = (ackFailed or o == AckOnClosedChannel or o == AckIntoCut),
      deliveries' = deliveries, runs' = runs, snaps' = snaps,
      looped' = looped, timedOut' = timedOut, rerun' = rerun, used' = used,
      did(Ack({ w: w, k: h.k, outcome: o })),
    }
  }

  // ---- the broker's timeouts ----
  // Redis: a consumer's restore_visible puts a record older than the
  // visibility timeout back on the queue, marked redelivered, and removes
  // the record (redis.py:521-560, 1263-1270).
  // @cite kombu kombu/transport/redis.py:521-547 "ceil = time() - self.visibility_timeout"
  // @cite kombu kombu/transport/redis.py:1263-1268 "payload['headers']['redelivered'] = True"
  action restoreVisible: bool = all {
    canRestore,
    ready' = ready + 1, redeliv' = true, rec' = NoRec,
    published' = published, unacked' = unacked, returns' = returns,
    ended' = ended, workers' = workers, gen' = gen, stored' = stored,
    timedOut' = true, deliveries' = deliveries, runs' = runs, snaps' = snaps,
    looped' = looped, ackFailed' = ackFailed, rerun' = rerun, used' = used,
    did(RestoreVisible),
  }
  // RabbitMQ: a delivery unacked past consumer_timeout closes its channel
  // with PRECONDITION_FAILED, and everything unacked on it is returned.
  // Celery: a channel error is not recoverable unless
  // broker_channel_error_retry is set (consumer.py:394-397); the worker
  // then stops warm (worker.py:269-276) and its run goes on to the end.
  // With the setting the consumer reconnects on a new channel
  // (consumer.py:407-431) and the run goes on too.
  // @env RabbitMQ consumer_timeout (docs/userguide/calling.rst:318-334)
  // @cite celery celery/worker/consumer/consumer.py:394-397 "recoverable_errors = (self.connection_errors + self.channel_errors)"
  // @cite celery celery/worker/worker.py:269-276 "logger.critical('Unrecoverable error: %r', exc, exc_info=True)"
  action consumerTimeout(w: WorkerId): bool = all {
    canTimeout(w),
    returnAll(w, gen.get(w)),
    workers' = workers.set(w, Alive(
      if (CFG.channelRetry) dropWaiting(lv(w))
      else { ...dropWaiting(lv(w)), stopping: true })),
    gen' = if (CFG.channelRetry) gen.setBy(w, g => g + 1) else gen,
    stored' = stored, timedOut' = true,
    deliveries' = deliveries, runs' = runs, snaps' = snaps, looped' = looped,
    ackFailed' = ackFailed, rerun' = rerun, used' = used,
    did(ConsumerTimeout(w)),
  }
  // A stopping worker exits once its run has ended.
  // @cite celery celery/worker/worker.py:269-276 "self.stop(exitcode=EX_FAILURE)"
  action exit(w: WorkerId): bool = all {
    canExit(w),
    workers' = workers.set(w, Dead),
    keepBroker, gen' = gen, stored' = stored, keepObs, used' = used,
    did(Exit(w)),
  }
  // A supervisor starts it again, on a new connection.
  // @env the supervisor (systemd, Kubernetes) restarts a worker that exits
  action restart(w: WorkerId): bool = all {
    canRestart(w),
    workers' = workers.set(w, Alive(FRESH)), gen' = gen.setBy(w, g => g + 1),
    keepBroker, stored' = stored, keepObs, used' = used,
    did(Restart(w)),
  }
  // Both ends notice a cut connection (heartbeats): the broker closes the
  // channel and returns what is unacked on it, including what the worker
  // acked into the cut; the worker reconnects on a new channel and its run
  // goes on (worker_cancel_long_running_tasks_on_connection_loss is off by
  // default, consumer.py:466-499).
  // @cite celery celery/worker/consumer/consumer.py:466-499 "if self.app.conf.worker_cancel_long_running_tasks_on_connection_loss:"
  action notice(w: WorkerId): bool = all {
    canNotice(w),
    returnAll(w, gen.get(w)),
    workers' = workers.set(w, Alive({ ...dropWaiting(lv(w)), conn: Up })),
    gen' = gen.setBy(w, g => g + 1),
    stored' = stored, keepObs, used' = used,
    did(Notice(w)),
  }

  // ---- faults ----
  // The worker is killed with its pool (SIGKILL, out of memory). AMQP: the
  // connection closes and the broker returns what was unacked on it.
  // Redis: the record waits for the visibility timeout.
  // @env SIGKILL of the whole worker
  action kill(w: WorkerId): bool = all {
    alive(w), used.kills < CFG.max.kills,
    if (REDIS) all {
      published' = published, ready' = ready, redeliv' = redeliv, rec' = rec,
      unacked' = unacked, returns' = returns, ended' = ended,
    } else returnAll(w, gen.get(w)),
    workers' = workers.set(w, Dead),
    gen' = gen, stored' = stored, keepObs,
    used' = { ...used, kills: used.kills + 1 },
    did(Kill(w)),
  }
  // An AMQP connection stops carrying anything; neither end knows yet.
  // @env a partition between a worker and RabbitMQ
  action cutOff(w: WorkerId): bool = all {
    not(REDIS), alive(w), lv(w).conn == Up, used.cuts < CFG.max.cuts,
    workers' = workers.set(w, Alive({ ...lv(w), conn: Cut })),
    keepBroker, gen' = gen, stored' = stored, keepObs,
    used' = { ...used, cuts: used.cuts + 1 },
    did(CutOff(w)),
  }
  // The broker loses a write it acknowledged: Redis restarted without
  // fsync, or a failover to a replica that had not received it
  // (redis.rst:262-273); a classic queue killed before it wrote a publish
  // that was not confirmed. A quorum queue, and a confirmed publish to a
  // durable classic queue, survive a kill.
  // @env Redis persistence and replication (docs/getting-started/backends-and-brokers/redis.rst:262-273)
  action loseData(what: Lost): bool = all {
    used.losses < CFG.max.losses,
    match what {
      | LostReady => all {
          ready > 0, REDIS or (CFG.broker == Classic and not(CFG.confirm)),
          ready' = ready - 1, rec' = rec,
        }
      | LostRecord => all { REDIS, hasRec(rec), ready' = ready, rec' = NoRec }
    },
    published' = published, redeliv' = redeliv, unacked' = unacked,
    returns' = returns, ended' = ended,
    workers' = workers, gen' = gen, stored' = stored, keepObs,
    used' = { ...used, losses: used.losses + 1 },
    did(LoseData(what)),
  }

  // ---- time ----
  // One unit passes: nothing that is due can happen first.
  // @env the coarse clock
  action tick: bool = all {
    not(urgent), canAge,
    workers' = WORKERS.mapBy(w => match workers.get(w) {
      | Dead => Dead
      | Alive(l) => Alive({ ...l, held: l.held.map(h =>
          if (h.stage == Running and h.left > 0) { ...h, left: h.left - 1 }
          else h) })
    }),
    rec' = match rec {
      | NoRec => NoRec
      | Recorded(a) => Recorded(if (a < T) a + 1 else a)
    },
    unacked' = unacked.map(u => { ...u, age: if (u.age < T) u.age + 1 else u.age }),
    published' = published, ready' = ready, redeliv' = redeliv,
    returns' = returns, ended' = ended, gen' = gen, stored' = stored,
    keepObs, used' = used,
    did(Tick),
  }

  action inner: bool = any {
    publish, restoreVisible, tick,
    { nondet w = WORKERS.oneOf()
      any {
        deliver(w), start(w), dedupSkip(w), finish(w), ack(w),
        consumerTimeout(w), exit(w), restart(w), notice(w),
        kill(w), cutOff(w),
      } },
    { nondet what = Set(LostReady, LostRecord).oneOf() loseData(what) },
  }
  action step: bool = all { inner, prevCopies' = copies }

  // ---- properties ----------------------------------------------------------
  // invariants
  // No task is redelivered without bound: no delivery starts from a state
  // (counters and ids aside) that an earlier one started from.
  val noLoop: bool = not(looped)
  // Nothing is lost silently: a published task is on the queue, recorded
  // as unacked, or held by a live worker, or its result is final, or the
  // broker dead-lettered it.
  def heldByWorker: bool = WORKERS.exists(w => alive(w) and held(w) != Set())
  val noSilentLoss: bool = published implies or {
    ready > 0, hasRec(rec), unacked != Set(), heldByWorker,
    stored == Success, ended == DeadLettered,
  }
  // Early ack: a task that started is never executed again
  // (docs/userguide/tasks.rst:24-26).
  val earlyAtMostOnce: bool = LATE or runs <= 1
  // No run starts while the task's SUCCESS is stored.
  val noRerunAfterSuccess: bool = not(rerun)
  // Every step is a step of the abstract delivery environment.
  val projectionOk: bool =
    stepsOf(prevCopies, copies, Set(M(1))).forall(s => not(isBad(s)))

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state; a run stops at
  // CFG.cap deliveries (`capped`), which is what noLoop is about.
  val capped: bool = deliveries >= CFG.cap and ready > 0
  val quiet: bool = published and not(urgent) and not(canAge)
  action stutter: bool = all {
    quiet, keepBroker, workers' = workers, gen' = gen, stored' = stored,
    keepObs, prevCopies' = prevCopies, used' = used,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // A quiet state has a final result, or the broker dead-lettered the task.
  val quietDone: bool =
    (quiet and not(capped)) implies (stored == Success or ended == DeadLettered)
  // Exactly once while no fault was used.
  val obs: Set[MsgObs] = Set({
    published: published, runs: runs, done: stored == Success,
    held: ready > 0 or hasRec(rec) or unacked != Set() or heldByWorker,
    settled: stored == Success and ready == 0 and not(hasRec(rec))
             and unacked == Set(),
  })
  val exactlyOnce: bool =
    exactlyOnceOf(used == NONE, quiet and not(capped), obs)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet and not(capped))
  val neverDone: bool = stored != Success
  val neverTimedOut: bool = not(timedOut)
  val neverTwice: bool = runs <= 1
}
