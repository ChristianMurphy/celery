// -*- mode: Bluespec; -*-
// Revoke and expiry: does a revoked or expired task never start, does a
// persistent revoke survive a restart, do stored states move one way, and
// what do a lost connection, a cold or soft shutdown and a backend error
// do on the way?
//
// Celery 768e476, kombu 445027e. Task 1 may be revoked (one
// app.control.revoke(), by id or by stamped header) and may carry
// `expires`; task 2, if present, is an ordinary task beside it. Two
// workers with `slots` pool processes each, on the abstract delivery
// environment of ../lib/delivery.qnt (a message per task).
//
// A request moves Reserved (the strategy received it, strategy.py:162
// checked expiry and the revoked set) -> Applied (execute_using_pool
// checked them again when a pool slot was free, request.py:1003-1006,
// worker.py:291-299, and handed it to the pool) -> Running (the pool
// process accepted it; nothing checks the revoked set there) -> Ran (the
// result is stored; the late ack is due). A discard stores REVOKED and
// acks (_announce_revoked, request.py:464-475).
//
// The revoke is a broadcast over the pidbox: a worker gets it only while
// its consumer is connected (Redis: PUBLISH to a channel nobody
// re-reads, kombu redis.py:1704-1716; AMQP: an auto_delete queue, kombu
// pidbox.py:258-273). Each worker that gets it adds the id to its
// in-memory set and, unless the task is active there, reads the stored
// state and stores REVOKED if it is PENDING, RECEIVED, RETRY or REJECTED
// (control.py:221-246; a backend error there is logged). By stamped
// header it only adds to revoked_stamps (control.py:164-186): nothing is
// stored, saved or synced. The set is saved to the state db at exit only
// (atexit, components.py:216-218) and read back at start; mingle copies
// it from the other workers at every consumer start (mingle.py:36-80).
//
// The result backend is a key-value store that refuses to replace
// SUCCESS only (base.py:1369-1370); ../result/resultstate.qnt has the
// stores in detail.
//
// Not modeled: terminate=True (a last resort, workers.rst), ETA and
// rate limits (the same checks run when the timer fires), several ids per
// revoke, the LimitedSet's expiry of old ids (10800 s), the by-tag ack of
// a stale Redis delivery (restore-by-tag, worker.qnt), a job flushed
// after its child read it (flush-ack-modes; here the fix of celery PR
// 11076 is assumed: an Applied job is one no child has).
module revoketypes {
  import deliverytypes.* from "../lib/delivery"
  export deliverytypes.*

  type WorkerId = { w: int }
  pure def W(i: int): WorkerId = { w: i }

  type AckMode = Early | Late
  type Result = Pending | Started | Retrying | Success | Revoked
  pure def final(r: Result): bool = match r {
    | Pending => false | Started => false | Retrying => false
    | Success => true | Revoked => true
  }

  type Stage = Reserved | Applied | Running | Ran
  // A request for one delivery `k` of task `m`. `stale`: its message went
  // back to the broker at a reconnect, so its ack settles nothing.
  type Req = { k: int, m: MsgId, stage: Stage, stale: bool }
  type Conn = Connected | Reconnecting
  type Halt = Going | WarmStop | SoftStop | ColdStop
  // The control command's backend part: idle, or the state it read and
  // the write still to do.
  type Ctl = CtlIdle | CtlRead(Result)
  type Live = {
    conn: Conn, halt: Halt, reqs: Set[Req], revoked: bool, inbox: bool,
    ctl: Ctl,
  }
  type Worker = Down | Up(Live)
  pure def isUp(x: Worker): bool = match x { | Down => false | Up(_) => true }
  pure val GONE: Live = {
    conn: Reconnecting, halt: ColdStop, reqs: Set(), revoked: false,
    inbox: false, ctl: CtlIdle,
  }
  pure def liveOf(x: Worker): Live = match x { | Down => GONE | Up(l) => l }

  // Where a backend error hits a discard: at the strategy's check when the
  // message is received, or at execute_using_pool's.
  type Where = AtReceive | AtPool
  type Action =
    | Init
    | Apply(MsgId)
    | Revoke
    | CtlApply(WorkerId)
    | CtlWrite({ w: WorkerId, wrote: bool })
    | Receive({ w: WorkerId, m: MsgId, discarded: bool })
    | ToPool({ w: WorkerId, k: int, discarded: bool })
    | Accept({ w: WorkerId, k: int })
    | Finish({ w: WorkerId, k: int })
    | AckRun({ w: WorkerId, k: int })
    | DiscardFails({ w: WorkerId, k: int, at: Where })
    | Expire
    | Kill(WorkerId)
    | Cut(WorkerId)
    | Reconnect(WorkerId)
    | Halting({ w: WorkerId, how: Halt })
    | GoCold(WorkerId)
    | Exit(WorkerId)
    | Restart(WorkerId)
    | CtlDup
    | Duplicate(MsgId)

  // Faults, each a bounded number of times. A shutdown is an operator's
  // action and a restart a supervisor's; both are budgeted so that runs
  // are finite.
  type Budget = {
    kills: int, cuts: int, backendErrors: int, ctlDups: int, dups: int,
    halts: int,
  }
  pure val NONE: Budget = {
    kills: 0, cuts: 0, backendErrors: 0, ctlDups: 0, dups: 0, halts: 0,
  }

  type Config = {
    ack: AckMode,
    slots: int,            // pool processes per worker
    semaphore: bool,       // the pool is guarded by the semaphore (prefork);
                           // threads, gevent and eventlet are not
    other: bool,           // task 2 is there too
    revoke: bool,          // the client revokes task 1
    byStamp: bool,         // ... by stamped header instead of by id
    expires: bool,         // task 1 carries expires
    trackStarted: bool,    // task_track_started
    statedb: bool,         // --statedb
    mingle: bool,          // mingle on (the default)
    cancelOnLoss: bool,    // worker_cancel_long_running_tasks_on_connection_loss
    brokerError: bool,     // the backend's error is one of the broker's
                           // connection errors (a Redis backend on a Redis
                           // broker)
    alwaysRetry: bool,     // result_backend_always_retry
    syncOnRevoke: bool,    // design: save the state db when a revoke arrives
    checkOnAccept: bool,   // design: on_accepted checks the revoked set
    finalSticky: bool,     // design: the store refuses to replace any final
                           // state (a conditional write)
    max: Budget,
  }
  pure def config(a: AckMode): Config = {
    ack: a, slots: 1, semaphore: true, other: false, revoke: true, byStamp: false,
    expires: false, trackStarted: false, statedb: false, mingle: true,
    cancelOnLoss: false, brokerError: true, alwaysRetry: false,
    syncOnRevoke: false, checkOnAccept: false, finalSticky: false, max: NONE,
  }
}

module revoke {
  import revoketypes.*
  import delivery(MSGS = Set(M(1), M(2)), DMAX = { dups: CFG.max.dups,
                                                   losses: 0 }) as Q
    from "../lib/delivery"

  const CFG: Config

  pure val WORKERS: Set[WorkerId] = Set(W(1), W(2))
  pure val TASKS: Set[MsgId] = if (CFG.other) Set(M(1), M(2)) else Set(M(1))
  pure val LATE: bool = CFG.ack == Late

  // ---- state ----
  var applied: Set[MsgId]
  var revokeSent: bool
  var expired: bool
  var workers: WorkerId -> Worker
  var saved: WorkerId -> bool      // the state db file holds task 1's id
  var stored: MsgId -> Result
  var nextK: int
  // ---- observations ----
  var runs: MsgId -> int
  var runKnown: bool        // task 1 started where its revoke was known
  var runExpired: bool      // task 1 started after it expired
  var revokeSeen: bool      // some worker applied the revoke
  var runMissed: bool       // task 1 started, after the revoke was sent, in
                            // a worker that neither knows it nor has it
                            // pending: it missed the broadcast or forgot it
  var knew: WorkerId -> bool  // the worker knew the revoke when it went down
  var forgot: bool          // restarted with the state db, the revoke gone
  var parallel: bool        // two runs of one task at once in one worker
  var ctlApplied: WorkerId -> int
  var rerun: bool           // a task started while its SUCCESS was stored
  var prevStored: MsgId -> Result
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    Q::dinit, applied' = Set(), revokeSent' = false, expired' = false,
    workers' = WORKERS.mapBy(_ => Up({
      conn: Connected, halt: Going, reqs: Set(), revoked: false,
      inbox: false, ctl: CtlIdle })),
    saved' = WORKERS.mapBy(_ => false),
    stored' = Set(M(1), M(2)).mapBy(_ => Pending), nextK' = 1,
    runs' = Set(M(1), M(2)).mapBy(_ => 0), runKnown' = false,
    runExpired' = false, revokeSeen' = false, runMissed' = false,
    knew' = WORKERS.mapBy(_ => false), forgot' = false, parallel' = false,
    ctlApplied' = WORKERS.mapBy(_ => 0), rerun' = false,
    prevStored' = Set(M(1), M(2)).mapBy(_ => Pending),
    used' = NONE, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepObs: bool = all {
    runs' = runs, runKnown' = runKnown, runExpired' = runExpired,
    revokeSeen' = revokeSeen, runMissed' = runMissed,
    knew' = knew, forgot' = forgot, parallel' = parallel,
    ctlApplied' = ctlApplied, rerun' = rerun,
  }
  action keepClient: bool = all {
    applied' = applied, revokeSent' = revokeSent, expired' = expired,
  }

  // ---- reading the state ----
  def up(w: WorkerId): bool = isUp(workers.get(w))
  def lv(w: WorkerId): Live = liveOf(workers.get(w))
  def reqs(w: WorkerId): Set[Req] = lv(w).reqs
  def online(w: WorkerId): bool =
    up(w) and lv(w).conn == Connected and lv(w).halt == Going
  def inStage(w: WorkerId, s: Stage): Set[Req] = reqs(w).filter(r => r.stage == s)
  // The semaphore counts the jobs handed to the pool and not finished; a
  // reconnect clears it (consumer.py:585-586), forgetting stale runs.
  def slotsTaken(w: WorkerId): int =
    reqs(w).filter(r => not(r.stale) and
                        (r.stage == Applied or r.stage == Running)).size()
  // A pool process is busy with each run, stale or not.
  def busyProcs(w: WorkerId): int = inStage(w, Running).size()
  // The strategy's and execute_using_pool's check (request.py:477-519).
  def discards(w: WorkerId, m: MsgId): bool =
    m == M(1) and ((CFG.expires and expired) or lv(w).revoked)
  // A store never replaces SUCCESS (base.py:1369-1370); design
  // finalSticky: nor any final state.
  def storing(m: MsgId, r: Result): MsgId -> Result =
    val cur = stored.get(m)
    stored.set(m, if (cur == Success or (CFG.finalSticky and final(cur))) cur else r)
  // The ack of a request: a stale one settles nothing (its message went
  // back already).
  // @cite celery celery/worker/request.py:901-905 "def acknowledge(self):"
  action settle(r: Req): bool = if (r.stale) Q::dkeep else Q::ack(r.m)
  // Observations of a body that starts.
  // @cite celery celery/app/trace.py:564 "R = retval = fun(*args, **kwargs)"
  action bodyStarts(w: WorkerId, r: Req): bool = all {
    runs' = runs.setBy(r.m, n => n + 1),
    runKnown' = (runKnown or (r.m == M(1) and lv(w).revoked)),
    runExpired' = (runExpired or (r.m == M(1) and CFG.expires and expired)),
    runMissed' = (runMissed or (r.m == M(1) and revokeSent
                                and not(lv(w).revoked) and not(lv(w).inbox))),
    parallel' = (parallel or inStage(w, Running).exists(x => x.m == r.m)),
    rerun' = (rerun or stored.get(r.m) == Success),
    revokeSeen' = revokeSeen, knew' = knew, forgot' = forgot,
    ctlApplied' = ctlApplied,
  }
  def setLive(w: WorkerId, l: Live): WorkerId -> Worker = workers.set(w, Up(l))

  // What a lost connection does to a worker's requests: reserved ones and
  // jobs no child has are dropped (on_close, consumer.py:581-594), and
  // their messages go back; a run goes on (or is cancelled, with
  // cancelOnLoss) and its message goes back too, unacked.
  pure def afterLoss(rs: Set[Req], late: bool, cancel: bool): Set[Req] =
    rs.filter(r => r.stage == Running or r.stage == Ran)
      .filter(r => not(cancel and late and r.stage == Running))
      .map(r => if (late) { ...r, stale: true } else r)
  // The messages a lost connection (or a killed worker) puts back: every
  // delivery not yet settled. With early ack a run's was settled at accept.
  pure def unsettled(rs: Set[Req], late: bool): Set[Req] =
    rs.filter(r => not(r.stale) and
                   (late or r.stage == Reserved or r.stage == Applied))

  // ---- the client ----
  // @cite celery celery/app/task.py:587-590 "def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,"
  action apply(m: MsgId): bool = all {
    TASKS.contains(m), not(applied.contains(m)),
    Q::publish(m), applied' = applied.union(Set(m)),
    revokeSent' = revokeSent, expired' = expired,
    workers' = workers, saved' = saved, stored' = stored, nextK' = nextK,
    keepObs, prevStored' = stored, used' = used, did(Apply(m)),
  }
  // app.control.revoke(): one broadcast; a worker gets it only while its
  // consumer is connected.
  // @cite celery celery/app/control.py:488-510 "def revoke(self, task_id, destination=None, terminate=False,"
  // @cite kombu kombu/pidbox.py:258 "auto_delete=not self.queue_durable,"
  // @cite kombu kombu/transport/redis.py:1704-1716 "client.publish(topic, payload)"
  action revokeTask: bool = all {
    CFG.revoke, not(revokeSent), applied.contains(M(1)),
    revokeSent' = true, applied' = applied, expired' = expired,
    workers' = WORKERS.mapBy(w => match workers.get(w) {
      | Down => Down
      | Up(l) => Up({ ...l, inbox: l.inbox or
                       (l.conn == Connected and l.halt == Going) })
    }),
    Q::dkeep, saved' = saved, stored' = stored, nextK' = nextK, keepObs,
    prevStored' = stored, used' = used, did(Revoke),
  }
  // The broadcast's publish is sent again after a lost reply (Mailbox
  // _publish with retry=True): a second copy.
  // @cite kombu kombu/pidbox.py:307-330 "serializer=serializer, retry=True,"
  action ctlDup: bool = all {
    revokeSent, used.ctlDups < CFG.max.ctlDups,
    workers' = WORKERS.mapBy(w => match workers.get(w) {
      | Down => Down
      | Up(l) => Up({ ...l, inbox: l.inbox or
                       (l.conn == Connected and l.halt == Going) })
    }),
    keepClient, Q::dkeep, saved' = saved, stored' = stored, nextK' = nextK,
    keepObs, prevStored' = stored,
    used' = { ...used, ctlDups: used.ctlDups + 1 }, did(CtlDup),
  }
  // Time passes beyond task 1's expires.
  // @env the clock passes `expires`
  action expire: bool = all {
    CFG.expires, not(expired), expired' = true,
    applied' = applied, revokeSent' = revokeSent,
    workers' = workers, Q::dkeep, saved' = saved, stored' = stored,
    nextK' = nextK, keepObs, prevStored' = stored, used' = used, did(Expire),
  }

  // ---- the control command ----
  // The worker adds the id to its revoked set; unless the task is active
  // here it reads the stored state. By stamped header it only adds to
  // revoked_stamps: no read, no write.
  // @cite celery celery/worker/control.py:225-235 "task_state = backend.get_state(task_id)"
  // @cite celery celery/worker/control.py:164-186 "worker_state.revoked_stamps[header] = updated_stamps"
  action ctlApply(w: WorkerId): bool = {
    val l = lv(w)
    val active = l.reqs.exists(r => r.m == M(1) and r.stage == Running)
    all {
      online(w), l.inbox, l.ctl == CtlIdle,
      workers' = setLive(w, { ...l, inbox: false, revoked: true,
        ctl: if (CFG.byStamp or active) CtlIdle else CtlRead(stored.get(M(1))) }),
      saved' = if (CFG.syncOnRevoke and CFG.statedb and not(CFG.byStamp))
                 saved.set(w, true) else saved,
      keepClient, Q::dkeep, stored' = stored, nextK' = nextK,
      runs' = runs, runKnown' = runKnown, runExpired' = runExpired,
      revokeSeen' = true, runMissed' = runMissed, knew' = knew,
      forgot' = forgot, parallel' = parallel, rerun' = rerun,
      ctlApplied' = ctlApplied.setBy(w, n => n + 1),
      prevStored' = stored, used' = used, did(CtlApply(w)),
    }
  }
  // Then it stores REVOKED if what it read is PENDING, RECEIVED, RETRY or
  // REJECTED. A backend error here is logged and nothing is stored.
  // @cite celery celery/worker/control.py:236-246 "backend.mark_as_revoked(task_id, reason='revoked', store_result=True)"
  action ctlWrite(w: WorkerId, fails: bool): bool = {
    val l = lv(w)
    val s = match l.ctl { | CtlIdle => Pending | CtlRead(x) => x }
    val writes = not(fails) and (s == Pending or s == Retrying)
    all {
      up(w), l.ctl != CtlIdle,
      not(fails) or used.backendErrors < CFG.max.backendErrors,
      workers' = setLive(w, { ...l, ctl: CtlIdle }),
      stored' = if (writes) storing(M(1), Revoked) else stored,
      keepClient, Q::dkeep, saved' = saved, nextK' = nextK, keepObs,
      prevStored' = stored,
      used' = if (fails) { ...used, backendErrors: used.backendErrors + 1 } else used,
      did(CtlWrite({ w: w, wrote: writes })),
    }
  }

  // ---- a worker ----
  // The strategy gets a message: a discard stores REVOKED and acks it at
  // once; otherwise the request is reserved.
  // @cite celery celery/worker/strategy.py:162-163 "if (req.expires or req.id in revoked_tasks) and req.revoked():"
  // @cite celery celery/worker/request.py:464-473 "self.task.backend.mark_as_revoked("
  action receive(w: WorkerId, m: MsgId): bool = {
    val l = lv(w)
    val drop = discards(w, m)
    all {
      online(w), Q::ready(m), l.reqs.size() < 2,
      if (drop) Q::take(m) else Q::deliver(m),
      workers' = setLive(w, { ...l, reqs: if (drop) l.reqs else
        l.reqs.union(Set({ k: nextK, m: m, stage: Reserved, stale: false })) }),
      stored' = if (drop) storing(m, Revoked) else stored,
      nextK' = nextK + 1,
      keepClient, saved' = saved, keepObs, prevStored' = stored,
      used' = used, did(Receive({ w: w, m: m, discarded: drop })),
    }
  }
  // A pool slot is free: execute_using_pool checks again, then hands the
  // job to the pool. Without the semaphore (threads, gevent, eventlet:
  // base.py:68, prefork.py:155) every reserved request is handed over at
  // once and waits in the pool.
  // @cite celery celery/concurrency/prefork.py:155 "uses_semaphore = True"
  // @cite celery celery/worker/request.py:1003-1006 "if self.revoked():"
  // @cite celery celery/worker/worker.py:291-299 "req.execute_using_pool(self.pool)"
  action toPool(w: WorkerId): bool = {
    val l = lv(w)
    nondet r = inStage(w, Reserved).oneOf()
    val drop = discards(w, r.m)
    all {
      online(w), r.stage == Reserved,
      not(CFG.semaphore) or slotsTaken(w) < CFG.slots,
      if (drop) settle(r) else Q::dkeep,
      workers' = setLive(w, { ...l, reqs: if (drop) l.reqs.exclude(Set(r))
        else l.reqs.exclude(Set(r)).union(Set({ ...r, stage: Applied })) }),
      stored' = if (drop) storing(r.m, Revoked) else stored,
      keepClient, saved' = saved, nextK' = nextK, keepObs,
      prevStored' = stored, used' = used,
      did(ToPool({ w: w, k: r.k, discarded: drop })),
    }
  }
  // A pool process takes the job (on_accepted): with early ack the
  // message is acked; with task_track_started the child stores STARTED;
  // the body starts. Nothing here reads the revoked set (design
  // checkOnAccept does).
  // @cite celery celery/worker/request.py:524-533 "if not self.task.acks_late:"
  // @cite celery celery/app/trace.py:553-557 "if track_started:"
  action accept(w: WorkerId): bool = {
    val l = lv(w)
    nondet r = inStage(w, Applied).oneOf()
    all {
      up(w), r.stage == Applied, busyProcs(w) < CFG.slots,
      not(CFG.checkOnAccept and discards(w, r.m)),
      if (LATE) Q::dkeep else settle(r),
      workers' = setLive(w, { ...l,
        reqs: l.reqs.exclude(Set(r)).union(Set({ ...r, stage: Running })) }),
      stored' = if (CFG.trackStarted) storing(r.m, Started) else stored,
      bodyStarts(w, r),
      keepClient, saved' = saved, nextK' = nextK,
      prevStored' = stored, used' = used, did(Accept({ w: w, k: r.k })),
    }
  }
  // Design checkOnAccept: on_accepted finds the id revoked and terminates
  // the child before the body (as _terminate_on_ack does for terminate).
  // @env design variant, not in celery: on_accepted reads the revoked set
  action acceptDiscards(w: WorkerId): bool = {
    val l = lv(w)
    nondet r = inStage(w, Applied).oneOf()
    all {
      up(w), r.stage == Applied, busyProcs(w) < CFG.slots,
      CFG.checkOnAccept, discards(w, r.m),
      settle(r),
      workers' = setLive(w, { ...l, reqs: l.reqs.exclude(Set(r)) }),
      stored' = storing(r.m, Revoked),
      keepClient, saved' = saved, nextK' = nextK, keepObs,
      prevStored' = stored, used' = used,
      did(ToPool({ w: w, k: r.k, discarded: true })),
    }
  }
  // The body returns; the child stores SUCCESS.
  // @cite celery celery/app/trace.py:604-608 "task.backend.mark_as_done("
  action finish(w: WorkerId): bool = {
    val l = lv(w)
    nondet r = inStage(w, Running).oneOf()
    all {
      up(w), r.stage == Running, l.halt != ColdStop,
      workers' = setLive(w, { ...l,
        reqs: l.reqs.exclude(Set(r)).union(Set({ ...r, stage: Ran })) }),
      stored' = storing(r.m, Success),
      keepClient, Q::dkeep, saved' = saved, nextK' = nextK, keepObs,
      prevStored' = stored, used' = used, did(Finish({ w: w, k: r.k })),
    }
  }
  // on_success: the late ack. A stale request's ack settles nothing.
  // @cite celery celery/worker/request.py:1035-1038 "if acks_late:"
  action ackRun(w: WorkerId): bool = {
    val l = lv(w)
    nondet r = inStage(w, Ran).oneOf()
    all {
      up(w), r.stage == Ran,
      if (LATE) settle(r) else Q::dkeep,
      workers' = setLive(w, { ...l, reqs: l.reqs.exclude(Set(r)) }),
      keepClient, saved' = saved, stored' = stored, nextK' = nextK, keepObs,
      prevStored' = stored, used' = used, did(AckRun({ w: w, k: r.k })),
    }
  }

  // ---- what a lost connection, a shutdown and a restart do ----
  // The broker connection is lost (heartbeats, a restart of the broker):
  // the consumer drops reserved requests and jobs no child has, their
  // messages go back, and so do the unacked messages of runs that go on.
  // With cancelOnLoss a late-ack run is cancelled: RETRY is stored
  // (request.cancel, emit_retry=True) and its message goes back.
  // @cite celery celery/worker/consumer/consumer.py:489-497 "request.cancel(self.pool)"
  // @cite celery celery/worker/consumer/consumer.py:581-594 "reserved_requests.clear()"
  action loseConnection(w: WorkerId, l: Live): bool = {
    val back = unsettled(l.reqs, LATE)
    val cancelled = l.reqs.filter(r => CFG.cancelOnLoss and LATE and r.stage == Running)
    all {
      Q::putBack(Set(M(1), M(2)).mapBy(m => back.filter(r => r.m == m).size())),
      // the pidbox message not yet read is gone with the connection
      workers' = setLive(w, { ...l, conn: Reconnecting, inbox: false,
        reqs: afterLoss(l.reqs, LATE, CFG.cancelOnLoss) }),
      stored' = TASKS.fold(stored, (s, m) =>
        if (cancelled.exists(r => r.m == m) and s.get(m) != Success)
          s.set(m, Retrying) else s),
    }
  }
  // @env a broker connection lost
  action cut(w: WorkerId): bool = all {
    online(w), used.cuts < CFG.max.cuts,
    loseConnection(w, lv(w)),
    keepClient, saved' = saved, nextK' = nextK, keepObs,
    prevStored' = stored, used' = { ...used, cuts: used.cuts + 1 },
    did(Cut(w)),
  }
  // A backend error while a discard stores REVOKED: the exception leaves
  // _announce_revoked before the ack. A Redis backend on a Redis broker
  // raises redis' ConnectionError, which the consumer takes for a broker
  // connection error and reconnects (consumer.py:394-397); with any other
  // error the worker logs "Unrecoverable error" and stops warm
  // (worker.py:269-276). With result_backend_always_retry the write waits
  // for the backend instead.
  // @cite celery celery/worker/request.py:464-473 "self.task.backend.mark_as_revoked("
  // @cite celery celery/worker/consumer/consumer.py:394-397 "recoverable_errors = (self.connection_errors + self.channel_errors)"
  // @cite celery celery/worker/worker.py:269-276 "logger.critical('Unrecoverable error: %r', exc, exc_info=True)"
  action discardFails(w: WorkerId, at: Where): bool = {
    val l = lv(w)
    nondet m = TASKS.oneOf()
    nondet r0 = inStage(w, Reserved).oneOf()
    // The message being discarded is held unacked: at the receipt it has
    // just been delivered; at the pool it is the reserved request r0.
    val r = if (at == AtReceive) { k: nextK, m: m, stage: Reserved, stale: false } else r0
    all {
      online(w), not(CFG.alwaysRetry),
      used.backendErrors < CFG.max.backendErrors,
      if (at == AtReceive) and { Q::ready(m), l.reqs.size() < 2, discards(w, m) }
      else and { r0.stage == Reserved, not(CFG.semaphore) or slotsTaken(w) < CFG.slots,
                 discards(w, r0.m) },
      // At the receipt the message was delivered and goes back with the
      // others: no change to its copies.
      if (CFG.brokerError) all {
        loseConnection(w, l), nextK' = nextK + 1,
      } else all {
        // stops warm: as a warm shutdown
        Q::putBack(Set(M(1), M(2)).mapBy(x => l.reqs.filter(q =>
          (q.stage == Reserved or q.stage == Applied) and q.m == x
          and not(q.stale)).size())),
        workers' = setLive(w, { ...l, halt: WarmStop,
          reqs: l.reqs.filter(q => q.stage == Running or q.stage == Ran) }),
        stored' = stored, nextK' = nextK + 1,
      },
      keepClient, saved' = saved, keepObs, prevStored' = stored,
      used' = { ...used, backendErrors: used.backendErrors + 1 },
      did(DiscardFails({ w: w, k: r.k, at: at })),
    }
  }
  // The consumer is connected again. Mingle asks the other workers for
  // their revoked ids (at every consumer start).
  // @cite celery celery/worker/consumer/mingle.py:36-60 "our_revoked = list(c.controller.state.revoked)"
  // @cite celery celery/worker/consumer/mingle.py:78-80 "c.controller.state.merge_revoked(revoked)"
  action reconnect(w: WorkerId): bool = {
    val l = lv(w)
    all {
      up(w), l.conn == Reconnecting, l.halt == Going,
      workers' = setLive(w, { ...l, conn: Connected,
        revoked: l.revoked or minglesRevoke(w) }),
      keepClient, Q::dkeep, saved' = saved, stored' = stored, nextK' = nextK,
      keepObs, prevStored' = stored, used' = used, did(Reconnect(w)),
    }
  }
  def minglesRevoke(w: WorkerId): bool =
    CFG.mingle and not(CFG.byStamp) and
    WORKERS.exists(o => o != w and online(o) and lv(o).revoked)

  // A shutdown begins. Warm: reserved requests are dropped and their
  // messages go back; runs finish. Soft (worker_soft_shutdown_timeout):
  // the same, for a while, then cold. Cold: as soft at its end.
  // @cite celery celery/apps/worker.py:461-465 "worker.wait_for_soft_shutdown()"
  // @cite celery celery/worker/worker.py:521-524 "sleep(app.conf.worker_soft_shutdown_timeout)"
  action halt(w: WorkerId, how: Halt): bool = {
    val l = lv(w)
    val back = l.reqs.filter(r => (r.stage == Reserved or r.stage == Applied)
                                  and not(r.stale))
    all {
      up(w), l.halt == Going, how != Going, used.halts < CFG.max.halts,
      Q::putBack(Set(M(1), M(2)).mapBy(m => back.filter(r => r.m == m).size())),
      workers' = setLive(w, { ...l, halt: how,
        reqs: l.reqs.filter(r => r.stage == Running or r.stage == Ran) }),
      keepClient, saved' = saved, stored' = stored, nextK' = nextK, keepObs,
      prevStored' = stored, used' = { ...used, halts: used.halts + 1 },
      did(Halting({ w: w, how: how })),
    }
  }
  // The cold part: cancel_active_requests terminates every run that is not
  // acked and not successful. A late-ack run's message goes back when the
  // connection closes (no state is stored, emit_retry=False); an early-ack
  // run's was acked already, and RETRY is stored ("cancelled by Celery").
  // @cite celery celery/worker/consumer/consumer.py:907-929 "request.cancel(self.pool, emit_retry=emit_retry)"
  // @cite celery celery/worker/request.py:446-457 "self.task.backend.mark_as_retry(self.id,"
  // @cite celery celery/apps/worker.py:476-482 "worker.consumer.cancel_active_requests()"
  action goCold(w: WorkerId): bool = {
    val l = lv(w)
    val cancelled = l.reqs.filter(r => r.stage == Running)
    val back = if (LATE) cancelled.filter(r => not(r.stale)) else Set()
    all {
      up(w), l.halt == SoftStop or l.halt == ColdStop,
      cancelled != Set(),
      Q::putBack(Set(M(1), M(2)).mapBy(m => back.filter(r => r.m == m).size())),
      workers' = setLive(w, { ...l, halt: ColdStop, reqs: l.reqs.exclude(cancelled) }),
      stored' = if (LATE) stored
                else TASKS.fold(stored, (s, m) =>
                  if (cancelled.exists(r => r.m == m) and s.get(m) != Success)
                    s.set(m, Retrying) else s),
      keepClient, saved' = saved, nextK' = nextK, keepObs,
      prevStored' = stored, used' = used, did(GoCold(w)),
    }
  }
  // The worker exits once nothing is left; atexit saves the revoked set to
  // the state db.
  // @cite celery celery/worker/components.py:216-218 "atexit.register(w._persistence.save)"
  action exit(w: WorkerId): bool = {
    val l = lv(w)
    all {
      up(w), l.halt != Going, l.reqs == Set(),
      workers' = workers.set(w, Down),
      saved' = if (CFG.statedb and not(CFG.byStamp)) saved.set(w, l.revoked) else saved,
      keepClient, Q::dkeep, stored' = stored, nextK' = nextK,
      runs' = runs, runKnown' = runKnown, runExpired' = runExpired,
      revokeSeen' = revokeSeen, runMissed' = runMissed,
      knew' = knew.set(w, l.revoked), forgot' = forgot, parallel' = parallel,
      ctlApplied' = ctlApplied, rerun' = rerun,
      prevStored' = stored, used' = used, did(Exit(w)),
    }
  }
  // The worker is killed (SIGKILL, out of memory): its unsettled messages
  // go back (AMQP at once, Redis after the visibility timeout), the
  // revoked set in memory is gone, the state db is not written.
  // @env SIGKILL of the whole worker
  action kill(w: WorkerId): bool = {
    val l = lv(w)
    val back = unsettled(l.reqs, LATE)
    all {
      up(w), used.kills < CFG.max.kills,
      Q::putBack(Set(M(1), M(2)).mapBy(m => back.filter(r => r.m == m).size())),
      workers' = workers.set(w, Down),
      keepClient, saved' = saved, stored' = stored, nextK' = nextK,
      runs' = runs, runKnown' = runKnown, runExpired' = runExpired,
      revokeSeen' = revokeSeen, runMissed' = runMissed,
      knew' = knew.set(w, l.revoked), forgot' = forgot, parallel' = parallel,
      ctlApplied' = ctlApplied, rerun' = rerun,
      prevStored' = stored, used' = { ...used, kills: used.kills + 1 },
      did(Kill(w)),
    }
  }
  // A supervisor starts it again: the revoked set is what the state db
  // holds, merged with what mingle gets from the others.
  // @cite celery celery/worker/state.py:302-310 "self.merge()"
  // @env the supervisor restarts a worker that exited or was killed
  action restart(w: WorkerId): bool = {
    val r = (CFG.statedb and saved.get(w)) or minglesRevoke(w)
    all {
      not(up(w)),
      workers' = setLive(w, { conn: Connected, halt: Going, reqs: Set(),
                              revoked: r, inbox: false, ctl: CtlIdle }),
      keepClient, Q::dkeep, saved' = saved, stored' = stored, nextK' = nextK,
      runs' = runs, runKnown' = runKnown, runExpired' = runExpired,
      revokeSeen' = revokeSeen, runMissed' = runMissed, knew' = knew,
      forgot' = (forgot or (CFG.statedb and knew.get(w) and not(r))),
      parallel' = parallel, ctlApplied' = ctlApplied, rerun' = rerun,
      prevStored' = stored, used' = used, did(Restart(w)),
    }
  }
  // @env a message is delivered again (lib/delivery.qnt)
  action duplicate(m: MsgId): bool = all {
    TASKS.contains(m), Q::duplicate(m),
    keepClient, workers' = workers, saved' = saved, stored' = stored,
    nextK' = nextK, keepObs, prevStored' = stored,
    used' = { ...used, dups: used.dups + 1 }, did(Duplicate(m)),
  }

  // ---- step ----
  action step: bool = any {
    revokeTask, ctlDup, expire,
    { nondet m = TASKS.oneOf() any { apply(m), duplicate(m) } },
    { nondet w = WORKERS.oneOf()
      any {
        ctlApply(w), toPool(w), accept(w), acceptDiscards(w), finish(w),
        ackRun(w), cut(w), reconnect(w), goCold(w), exit(w), kill(w),
        restart(w),
        { nondet fails = Set(false, true).oneOf() ctlWrite(w, fails) },
        { nondet m = TASKS.oneOf() receive(w, m) },
        { nondet at = Set(AtReceive, AtPool).oneOf() discardFails(w, at) },
        { nondet how = Set(WarmStop, SoftStop, ColdStop).oneOf() halt(w, how) },
      } },
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // No run of task 1 starts in a worker whose revoked set holds it.
  val noRunAfterRevokeKnown: bool = not(runKnown)
  // No run of task 1 starts after it expired.
  val noRunAfterExpiry: bool = not(runExpired)
  // No run of task 1 starts, once the revoke was sent, in a worker that
  // neither knows it nor still has it to read: no worker misses or
  // forgets a revoke. (A worker that has the broadcast still to read may
  // start the task first: the broadcast reaches workers one by one.)
  val noRunMissingRevoke: bool = not(runMissed)
  // With --statedb, a worker that knew the revoke knows it after a restart.
  val revokeSurvivesRestart: bool = not(forgot)
  // A lost connection never leaves two runs of one task at once in one
  // worker.
  val noParallelRuns: bool = not(parallel)
  // A stored final state (SUCCESS, REVOKED) is never replaced.
  val finalStable: bool =
    TASKS.forall(m => final(prevStored.get(m)) implies stored.get(m) == prevStored.get(m))
  // Each worker applies the control command at most once.
  val ctlOnce: bool = WORKERS.forall(w => ctlApplied.get(w) <= 1)
  // No task starts while its SUCCESS is stored.
  val noRerunAfterSuccess: bool = not(rerun)
  // A quiet system shows a final state for every task.
  val outcomeVisible: bool = quiet implies TASKS.forall(m => final(stored.get(m)))

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = and {
    TASKS.forall(m => applied.contains(m) and not(Q::ready(m))),
    CFG.revoke implies revokeSent,
    WORKERS.forall(w => and {
      online(w), reqs(w) == Set(), not(lv(w).inbox), lv(w).ctl == CtlIdle,
    }),
  }
  action stutter: bool = all {
    quiet, Q::dkeep, keepClient, workers' = workers, saved' = saved,
    stored' = stored, nextK' = nextK, keepObs, prevStored' = prevStored,
    used' = used, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // A quiet system has nothing held: every message settled.
  val quietDone: bool = quiet implies TASKS.forall(m => Q::copies.get(m) == NO_COPIES)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRan: bool = runs.get(M(1)) == 0
  val neverRevokedStored: bool = stored.get(M(1)) != Revoked
  val neverRevokeApplied: bool = not(revokeSeen)
}
