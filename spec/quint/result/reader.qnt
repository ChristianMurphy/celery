// -*- mode: Bluespec; -*-
// What a client reads: does every reader of a group's results get an
// answer once the tasks finished, and do two readers get the same one?
//
// Celery 768e476 (celery/result.py, celery/backends/base.py, redis.py,
// rpc.py and asynchronous.py are the same at 9c2f238). Two tasks T(1) and
// T(2) of one group, two client processes: Rd(1) applied the group (it owns
// the RPC reply queue and saves the GroupResult), Rd(2) is another process
// (it restores the GroupResult by id, or builds AsyncResults by id). Each
// reader joins the group: it waits for every task.
//
// A worker writes STARTED, then SUCCESS or FAILURE, for each task. A late
// write is one more write after the final one: the original's RETRY after
// its copy stored a final state (finding retry-over-failure), or a duplicate
// run's other final state. A key-value store refuses to replace SUCCESS only
// (base.py:1369); the design KeepReady refuses to replace any ready state.
//
// MODE is how a reader waits:
//   Poll    SyncBackendMixin.wait_for: get_task_meta until a ready state
//           (base.py:1123-1150): database, MongoDB, memcached, ...
//   PubSub  the Redis ResultConsumer: subscribe to the task's channel, read
//           the key once, then the messages every write publishes
//           (redis.py:816-826, 296-320, 401-411); after a lost pub/sub
//           connection it reads every subscribed key (MGET) and subscribes
//           again, in that order (redis.py:245-271).
//   Rpc     rpc://: every write is a message to the reply queue of the
//           client that applied the task (rpc.py:239-252, 283-300); nothing
//           is stored.
// AsyncResult caches the first ready state it reads and never reads again
// (result.py:255, 463-475): a reader's answer is fixed once it has one.
// Result expiry is its own state of a key (Expired): it reads as PENDING,
// like a key never written (base.py:1387-1392).
//
// Faults (budgets): late writes, expiries (result_expires of a task key or
// the group key passed), cuts (a reader's pub/sub connection is lost),
// backend errors (a read, a save_group or a restore_group raises in the
// client; base.py:977-998 retries only under result_backend_always_retry).
//
// Not modeled: the backend's process cache of SUCCESS metas (base.py:951-
// 955; it serves only what a read returned), join_native's order, the
// _pending_messages buffer, the order in which Redis serves the SUBSCRIBE
// and the GET of a first read (two connections), RPC reply queue loss, and
// the chord's own restore of the group (canvas/chord.qnt).
module readertypes {
  type Status = Pending | Started | Retry | Success | Failure
  pure def isReady(s: Status): bool = match s {
    | Pending => false | Started => false | Retry => false
    | Success => true | Failure => true
  }

  type TaskId = { t: int }
  pure def T(i: int): TaskId = { t: i }
  type ReaderId = { rd: int }
  pure def Rd(i: int): ReaderId = { rd: i }

  // What the store holds under a task's key.
  type Stored = Absent | Present(Status) | Expired
  pure def readOf(s: Stored): Status = match s {
    | Absent => Pending | Present(x) => x | Expired => Pending
  }
  type GroupKey = NoGroup | Saved | GroupExpired

  type Mode = Poll | PubSub | Rpc
  type Keep = KeepSuccess | KeepReady

  // A task's writer: no write yet, STARTED written, final written (a late
  // write may follow), done (no more writes).
  type WStage = WIdle | WStarted | WFinal | WDone

  // One reader's view of one task.
  type Answer = Waiting | Got(Status)
  type View = {
    ans: Answer,
    last: Status,         // what the last read returned (Poll)
    sub: bool,            // subscribed to the task's channel (PubSub)
    primed: bool,         // the read after subscribing was done (PubSub)
    inbox: List[Status],  // messages received, not yet handled
  }
  pure val FRESH: View =
    { ans: Waiting, last: Pending, sub: false, primed: false, inbox: List() }
  pure def isWaiting(v: View): bool = match v.ans {
    | Waiting => true | Got(_) => false
  }

  // The pub/sub connection. Half: one of the two steps of a reconnect is
  // done (on main the MGET, in the design subscribeFirst the SUBSCRIBE).
  type Conn = Up | Down | Half
  // Begin: not joining yet (a save or a restore may come first).
  type RStage = Begin | Joining | Raised | NotFound

  type RestoreOutcome = Found | Missing
  type ErrorPoint = AtSave | AtRestore | AtRead
  type Action =
    | Init
    | Write({ t: TaskId, status: Status, written: bool })
    | Expire(TaskId)
    | ExpireGroup
    | Save(ReaderId)
    | Restore({ rd: ReaderId, outcome: RestoreOutcome })
    | Start(ReaderId)
    | Read({ rd: ReaderId, t: TaskId, saw: Status })
    | Subscribe({ rd: ReaderId, t: TaskId })
    | Drain({ rd: ReaderId, t: TaskId, saw: Status })
    | Cut(ReaderId)
    | Refetch(ReaderId)
    | Resubscribe(ReaderId)
    | BackendError({ rd: ReaderId, point: ErrorPoint })

  type Budget = { late: int, expiries: int, cuts: int, errors: int }
  pure val NONE: Budget = { late: 0, expiries: 0, cuts: 0, errors: 0 }

  type Config = {
    mode: Mode,
    keep: Keep,
    // a task's final state can be FAILURE
    canFail: bool,
    // Rd(1) saves the GroupResult and Rd(2) restores it (else Rd(2) builds
    // AsyncResults from the ids)
    group: bool,
    // design: after a lost pub/sub connection, subscribe again before the
    // MGET (main: MGET first)
    subscribeFirst: bool,
    max: Budget,
  }
  pure def config(m: Mode): Config = {
    mode: m, keep: KeepSuccess, canFail: true, group: true,
    subscribeFirst: false, max: NONE,
  }
}

module reader {
  import readertypes.*

  const CFG: Config

  pure val TASKS: Set[TaskId] = Set(T(1), T(2))
  pure val READERS: Set[ReaderId] = Set(Rd(1), Rd(2))
  pure val LATE: Set[Status] = Set(Retry, Success, Failure)

  // ---- state ----
  var store: TaskId -> Stored
  var gkey: GroupKey
  var wst: TaskId -> WStage
  var stage: ReaderId -> RStage
  var conn: ReaderId -> Conn
  var views: ReaderId -> (TaskId -> View)
  // ---- observations ----
  var wrote: TaskId -> Set[Status]   // every status a write stored
  var saveOk: bool                    // GroupResult.save() returned
  var restoreMissed: bool             // a restore after a save that returned found nothing
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    store' = TASKS.mapBy(_ => Absent), gkey' = NoGroup,
    wst' = TASKS.mapBy(_ => WIdle),
    stage' = READERS.mapBy(_ => Begin), conn' = READERS.mapBy(_ => Up),
    views' = READERS.mapBy(_ => TASKS.mapBy(_ => FRESH)),
    wrote' = TASKS.mapBy(_ => Set()), saveOk' = false, restoreMissed' = false,
    used' = NONE, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepObs: bool = all {
    wrote' = wrote, saveOk' = saveOk, restoreMissed' = restoreMissed,
  }
  action keepClient: bool = all {
    gkey' = gkey, stage' = stage, conn' = conn, views' = views,
  }
  def view(r: ReaderId, t: TaskId): View = views.get(r).get(t)
  def setView(r: ReaderId, t: TaskId, v: View): ReaderId -> (TaskId -> View) =
    views.set(r, views.get(r).set(t, v))
  def joining(r: ReaderId): bool = stage.get(r) == Joining
  // The view after a ready answer: cached, unsubscribed (cancel_for,
  // redis.py:290-293), nothing more to handle.
  pure def answered(v: View, s: Status): View =
    { ...v, ans: Got(s), last: s, sub: false, inbox: List() }
  // A view after reading s: a ready state is cached (result.py:463-470).
  pure def afterRead(v: View, s: Status): View =
    if (isReady(s)) answered(v, s) else { ...v, last: s }

  // ---- writers ----
  // A write as the store does it: a key-value store reads the key first and
  // keeps SUCCESS (base.py:1355-1377); KeepReady keeps any ready state; RPC
  // sends every write (rpc.py:283-300). A write that is done publishes it on
  // the task's channel in the same pipeline (redis.py:816-826), or sends it
  // to Rd(1)'s reply queue (rpc.py:286).
  def refuses(t: TaskId): bool = {
    val cur = readOf(store.get(t))
    if (CFG.mode == Rpc) false
    else match CFG.keep {
      | KeepSuccess => cur == Success
      | KeepReady => isReady(cur)
    }
  }
  def delivers(r: ReaderId, t: TaskId): bool = match CFG.mode {
    | Poll => false
    | PubSub => view(r, t).sub and (conn.get(r) == Up or
                                    (conn.get(r) == Half and CFG.subscribeFirst))
    | Rpc => r == Rd(1)
  }
  // @cite celery celery/backends/base.py:1367-1370 "if current_meta['status'] == states.SUCCESS:"
  action write(t: TaskId, s: Status, after: WStage): bool = {
    val ok = not(refuses(t))
    all {
      store' = if (ok) store.set(t, Present(s)) else store,
      wrote' = if (ok) wrote.set(t, wrote.get(t).union(Set(s))) else wrote,
      views' = if (not(ok)) views else READERS.mapBy(r =>
        if (delivers(r, t))
          views.get(r).set(t, { ...view(r, t), inbox: view(r, t).inbox.append(s) })
        else views.get(r)),
      wst' = wst.set(t, after),
      saveOk' = saveOk, restoreMissed' = restoreMissed,
      gkey' = gkey, stage' = stage, conn' = conn,
      did(Write({ t: t, status: s, written: ok })),
    }
  }
  // @cite celery celery/backends/base.py:286-288 "def mark_as_started(self, task_id, **meta):"
  action writeStarted(t: TaskId): bool = all {
    wst.get(t) == WIdle, write(t, Started, WStarted), used' = used,
  }
  // @cite celery celery/backends/base.py:1355-1377 "if current_meta['status'] == states.SUCCESS:"
  // @cite celery celery/backends/redis.py:816-826 "pipe.publish(key, value)"
  // @cite celery celery/backends/rpc.py:283-300 "routing_key, correlation_id = self.destination_for(task_id, request)"
  action writeFinal(t: TaskId, s: Status): bool = all {
    wst.get(t) == WStarted, s == Success or (s == Failure and CFG.canFail),
    write(t, s, WFinal), used' = used,
  }
  // A late write: the original run's RETRY after the copy's final state
  // (trace.py:191-202), or a duplicate run's other final state.
  // @cite celery celery/app/trace.py:191-202 "task.backend.mark_as_retry("
  action writeLate(t: TaskId, s: Status): bool = all {
    wst.get(t) == WFinal, used.late < CFG.max.late,
    s != readOf(store.get(t)),
    write(t, s, WDone), used' = { ...used, late: used.late + 1 },
  }

  // ---- expiry ----
  // result_expires passed: the key is gone and reads as PENDING
  // (base.py:1387-1392). A day later: no late write follows.
  // @cite celery celery/backends/base.py:1387-1392 "return {'status': states.PENDING, 'result': None}"
  action expire(t: TaskId): bool = all {
    CFG.mode != Rpc, used.expiries < CFG.max.expiries,
    wst.get(t) == WFinal or wst.get(t) == WDone,
    store' = store.set(t, Expired), wst' = wst.set(t, WDone),
    keepObs, keepClient, used' = { ...used, expiries: used.expiries + 1 },
    did(Expire(t)),
  }
  // The saved GroupResult's key expires too (_save_group sets it with the
  // result expiry, base.py:1379-1382).
  // @cite celery celery/backends/base.py:1379-1382 "def _save_group(self, group_id, result):"
  action expireGroup: bool = all {
    CFG.mode != Rpc, gkey == Saved, used.expiries < CFG.max.expiries,
    gkey' = GroupExpired, store' = store, wst' = wst,
    stage' = stage, conn' = conn, views' = views, keepObs,
    used' = { ...used, expiries: used.expiries + 1 },
    did(ExpireGroup),
  }

  // ---- readers: save, restore, start ----
  // Rd(1) saves the GroupResult (result.py:979-987, base.py:996-998).
  // @cite celery celery/result.py:979-987 "return (backend or self.app.backend).save_group(self.id, self)"
  // @cite celery celery/backends/base.py:996-998 "return self._ensure_retryable(self._save_group, group_id, result)"
  action save: bool = all {
    CFG.group, CFG.mode != Rpc, stage.get(Rd(1)) == Begin,
    gkey' = Saved, saveOk' = true, stage' = stage.set(Rd(1), Joining),
    wrote' = wrote, restoreMissed' = restoreMissed,
    conn' = conn, views' = views, store' = store, wst' = wst, used' = used,
    did(Save(Rd(1))),
  }
  // Rd(2) restores it by id once Rd(1) is past its save: a key that is not
  // there gives None (base.py:1410-1420, 990-994).
  // @cite celery celery/result.py:1036-1042 "return backend.restore_group(id)"
  // @cite celery celery/backends/base.py:1410-1420 "meta = self.get(self.get_key_for_group(group_id))"
  action restore: bool = {
    val found = gkey == Saved
    all {
      CFG.group, CFG.mode != Rpc, stage.get(Rd(2)) == Begin,
      stage.get(Rd(1)) != Begin,
      stage' = stage.set(Rd(2), if (found) Joining else NotFound),
      restoreMissed' = (restoreMissed or (not(found) and saveOk)),
      wrote' = wrote, saveOk' = saveOk,
      gkey' = gkey, conn' = conn, views' = views, store' = store, wst' = wst,
      used' = used,
      did(Restore({ rd: Rd(2), outcome: if (found) Found else Missing })),
    }
  }
  // Without a saved group each reader joins AsyncResults built from the
  // ids it has (ResultSet.join, result.py:759-842).
  // @cite celery celery/result.py:829-836 "value = result.get("
  action start(r: ReaderId): bool = all {
    not(CFG.group) or CFG.mode == Rpc, stage.get(r) == Begin,
    stage' = stage.set(r, Joining),
    keepObs, gkey' = gkey, conn' = conn, views' = views, store' = store,
    wst' = wst, used' = used, did(Start(r)),
  }

  // ---- readers: Poll ----
  // wait_for: get_task_meta until a ready state (base.py:1138-1142). A read
  // that would return what the last one did changes nothing and is left out.
  // @cite celery celery/backends/base.py:1138-1142 "if meta['status'] in states.READY_STATES:"
  // @cite celery celery/result.py:463-470 "if state in states.READY_STATES:"
  action pollRead(r: ReaderId, t: TaskId): bool = {
    val s = readOf(store.get(t))
    all {
      CFG.mode == Poll, joining(r), isWaiting(view(r, t)), s != view(r, t).last,
      views' = setView(r, t, afterRead(view(r, t), s)),
      keepObs, gkey' = gkey, stage' = stage, conn' = conn, store' = store,
      wst' = wst, used' = used, did(Read({ rd: r, t: t, saw: s })),
    }
  }

  // ---- readers: PubSub ----
  // get() subscribes first (add_pending_result -> consume_from,
  // asynchronous.py:285-294, redis.py:401-411) ...
  // @cite celery celery/backends/asynchronous.py:285-294 "self.result_consumer.consume_from(task_id)"
  // @cite celery celery/backends/redis.py:401-411 "self._pubsub.subscribe(key)"
  action subscribe(r: ReaderId, t: TaskId): bool = all {
    CFG.mode == PubSub, joining(r), conn.get(r) == Up,
    isWaiting(view(r, t)), not(view(r, t).sub),
    views' = setView(r, t, { ...view(r, t), sub: true }),
    keepObs, gkey' = gkey, stage' = stage, conn' = conn, store' = store,
    wst' = wst, used' = used, did(Subscribe({ rd: r, t: t })),
  }
  // ... then reads the key once (on_wait_for_pending, redis.py:308-311).
  // @cite celery celery/backends/redis.py:308-311 "for meta in result._iter_meta(**kwargs):"
  action firstRead(r: ReaderId, t: TaskId): bool = {
    val s = readOf(store.get(t))
    all {
      CFG.mode == PubSub, joining(r), view(r, t).sub, not(view(r, t).primed),
      views' = setView(r, t, { ...afterRead(view(r, t), s), primed: true }),
      keepObs, gkey' = gkey, stage' = stage, conn' = conn, store' = store,
      wst' = wst, used' = used, did(Read({ rd: r, t: t, saw: s })),
    }
  }
  // A message: on_state_change caches a ready state (asynchronous.py:448-
  // 466) and unsubscribes (redis.py:290-297). The RPC consumer does the
  // same with the reply queue (rpc.py:325-370).
  // @cite celery celery/backends/redis.py:373-378 "message = pubsub.get_message(timeout=timeout)"
  // @cite celery celery/backends/asynchronous.py:448-466 "result._maybe_set_cache(meta)"
  // @cite celery celery/backends/rpc.py:325-370 "for acc in self._slurp_from_queue(task_id, self.accept, backlog_limit):"
  action drain(r: ReaderId, t: TaskId): bool = {
    val v = view(r, t)
    val s = v.inbox.head()
    all {
      CFG.mode != Poll, joining(r), v.inbox.length() > 0,
      views' = setView(r, t,
        if (not(isWaiting(v))) { ...v, inbox: List() }
        else { ...afterRead(v, s), inbox: if (isReady(s)) List() else v.inbox.tail() }),
      keepObs, gkey' = gkey, stage' = stage, conn' = conn, store' = store,
      wst' = wst, used' = used, did(Drain({ rd: r, t: t, saw: s })),
    }
  }
  // The reconnect after a lost pub/sub connection (reconnect_on_error,
  // asynchronous.py:367-390): MGET of every subscribed key, each ready one
  // handled like a message (redis.py:251-257) ...
  // @cite celery celery/backends/redis.py:251-257 "metas = self.backend.client.mget(self.subscribed_to)"
  action refetch(r: ReaderId): bool = all {
    CFG.mode == PubSub, joining(r),
    if (CFG.subscribeFirst) conn.get(r) == Half else conn.get(r) == Down,
    views' = views.set(r, TASKS.mapBy(t => {
      val v = view(r, t)
      val s = readOf(store.get(t))
      if (v.sub and isReady(s)) answered(v, s) else v
    })),
    conn' = conn.set(r, if (CFG.subscribeFirst) Up else Half),
    keepObs, gkey' = gkey, stage' = stage, store' = store,
    wst' = wst, used' = used, did(Refetch(r)),
  }
  // ... then a new pub/sub connection subscribes to every key still
  // subscribed (redis.py:260-271). Writes between the MGET and this reach
  // nobody.
  // @cite celery celery/backends/redis.py:260-271 "self._pubsub.subscribe(*self.subscribed_to)"
  action resubscribe(r: ReaderId): bool = all {
    CFG.mode == PubSub, joining(r),
    if (CFG.subscribeFirst) conn.get(r) == Down else conn.get(r) == Half,
    conn' = conn.set(r, if (CFG.subscribeFirst) Half else Up),
    keepObs, gkey' = gkey, stage' = stage, views' = views, store' = store,
    wst' = wst, used' = used, did(Resubscribe(r)),
  }

  // ---- faults ----
  // The pub/sub connection is lost; messages not yet read are lost with it.
  // @env a lost connection between the client and Redis
  action cut(r: ReaderId): bool = all {
    CFG.mode == PubSub, joining(r), conn.get(r) == Up,
    used.cuts < CFG.max.cuts,
    TASKS.exists(t => view(r, t).sub),
    conn' = conn.set(r, Down),
    views' = views.set(r, TASKS.mapBy(t => { ...view(r, t), inbox: List() })),
    keepObs, gkey' = gkey, stage' = stage, store' = store, wst' = wst,
    used' = { ...used, cuts: used.cuts + 1 }, did(Cut(r)),
  }
  // A backend error in the client: a save_group, a restore_group or a read
  // raises to the caller (get_task_meta and get_group_meta retry only under
  // result_backend_always_retry, base.py:827-860, 977-988).
  // @cite celery celery/backends/base.py:977-988 "meta = self._ensure_retryable(self._restore_group, group_id)"
  // @cite celery celery/backends/base.py:934-956 "fallback_exc=partial(BackendGetMetaError, task_id=task_id),"
  action backendError(r: ReaderId, p: ErrorPoint): bool = all {
    CFG.mode != Rpc, used.errors < CFG.max.errors,
    match p {
      | AtSave => CFG.group and r == Rd(1) and stage.get(r) == Begin
      | AtRestore => CFG.group and r == Rd(2) and stage.get(r) == Begin
                     and stage.get(Rd(1)) != Begin
      | AtRead => joining(r) and TASKS.exists(t => isWaiting(view(r, t)))
    },
    stage' = stage.set(r, Raised),
    keepObs, gkey' = gkey, conn' = conn, views' = views, store' = store,
    wst' = wst, used' = { ...used, errors: used.errors + 1 },
    did(BackendError({ rd: r, point: p })),
  }

  // ---- step ----
  // What the code and the clock do on their own.
  action work: bool = any {
    { nondet t = TASKS.oneOf()
      any {
        writeStarted(t),
        { nondet s = Set(Success, Failure).oneOf() writeFinal(t, s) },
      } },
    save, restore,
    { nondet r = READERS.oneOf()
      any {
        start(r), refetch(r), resubscribe(r),
        { nondet t = TASKS.oneOf()
          any { pollRead(r, t), subscribe(r, t), firstRead(r, t), drain(r, t) } },
      } },
  }

  // What may happen and need not: each of these uses up a budget.
  action fault: bool = any {
    { nondet t = TASKS.oneOf()
      any {
        { nondet s = LATE.oneOf() writeLate(t, s) },
        expire(t),
      } },
    expireGroup,
    { nondet r = READERS.oneOf()
      any {
        cut(r),
        { nondet p = Set(AtSave, AtRestore, AtRead).oneOf() backendError(r, p) },
      } },
  }

  action step: bool = any { work, fault }

  // ---- properties ----------------------------------------------------------
  // invariants
  // Two readers that have a ready answer for a task have the same one.
  val readersAgree: bool = TASKS.forall(t => READERS.forall(a => READERS.forall(b =>
    match view(a, t).ans {
      | Waiting => true
      | Got(x) => match view(b, t).ans { | Waiting => true | Got(y) => x == y }
    })))
  // Nothing from nothing: every answer a reader has was stored by a write.
  val answerWasWritten: bool = TASKS.forall(t => READERS.forall(r =>
    match view(r, t).ans {
      | Waiting => true
      | Got(x) => wrote.get(t).contains(x)
    }))
  // A restore after a save that returned finds the group.
  val restoreFinds: bool = not(restoreMissed)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  def viewSettled(r: ReaderId, t: TaskId): bool = {
    val v = view(r, t)
    not(isWaiting(v)) or match CFG.mode {
      | Poll => readOf(store.get(t)) == v.last
      | PubSub => v.sub and v.primed and v.inbox.length() == 0
      | Rpc => v.inbox.length() == 0
    }
  }
  def readerSettled(r: ReaderId): bool = match stage.get(r) {
    | Begin => false
    | Joining => conn.get(r) == Up and TASKS.forall(t => viewSettled(r, t))
    | Raised => true
    | NotFound => true
  }
  val quiet: bool = and {
    TASKS.forall(t => wst.get(t) == WFinal or wst.get(t) == WDone),
    READERS.forall(r => readerSettled(r)),
  }
  action stutter: bool = all {
    quiet, store' = store, wst' = wst, keepClient, keepObs, used' = used,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run in which the code keeps
  // working ends in a quiet state, for good, and that state has ended. The
  // fairness is on "work" only: no fault has to happen, and any that the
  // budgets allow may. Every action raises "steps", so that names the
  // steps of "work" that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(work, steps) implies eventually(always(quiet and quietDone))
  // In a quiet state every reader has ended: it has every answer, or its
  // save, restore or read raised, or the restore found nothing.
  val quietDone: bool = quiet implies READERS.forall(r =>
    stage.get(r) != Joining or TASKS.forall(t => not(isWaiting(view(r, t)))))

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverAnswered: bool = READERS.forall(r => TASKS.exists(t => isWaiting(view(r, t))))
  val neverFailed: bool = TASKS.forall(t => not(wrote.get(t).contains(Failure)))
}
