// -*- mode: Bluespec; -*-
// A chain with a link callback: a.s() | b.s(), where a also has a link
// callback l and an errback e. Is the next step published once, only
// after its parent returned, and never lost?
//
// Celery 768e476. The worker's side of a successful run of a
// (celery/app/trace.py:584-613): the body returns; the link callbacks are
// published (_dispatch_callbacks_and_chain, trace.py:405-453, links first,
// then the next chain step), each through AMQP._retry_publish, which
// retries a refused publish and then raises (amqp.py:299-307, 621-650);
// then the result is stored with its children (mark_as_done); with late
// ack the message is acked after that (request.py:616-631). Any error
// after the body returned goes to on_error: FAILURE is stored for a, every
// later step of the chain is marked FAILURE (_fail_chain, base.py:298-317)
// and the errbacks run (base.py:391-421). A pool process lost while the
// parent lives is handled by Request.on_failure (request.py:709-790): if
// SUCCESS was stored it acks, otherwise with late ack it acks (or requeues
// with reject_on_worker_lost) and marks the task FAILURE the same way.
//
// The broker is the abstract at-least-once environment of
// ../lib/delivery.qnt. Two workers with one pool process each; three
// messages: a, b and l. With worker_deduplicate_successful_tasks a
// redelivered message whose SUCCESS is stored is acked without a run
// (trace.py:485-543; children are always stored, so the links are not
// published again).
//
// Not modeled: b failing on its own, several links (published as a group,
// trace.py:419-441), errbacks published as tasks (here they run inline, as
// a function errback does), a chain inside a chord (see chord.qnt), retry.
module chaintypes {
  import deliverytypes.* from "../lib/delivery"
  export deliverytypes.*

  type WorkerId = { w: int }
  pure def W(i: int): WorkerId = { w: i }

  type Task = TaskA | TaskB | TaskL
  pure val MA: MsgId = M(1)
  pure val MB: MsgId = M(2)
  pure val ML: MsgId = M(3)
  pure def taskOf(m: MsgId): Task =
    if (m == MA) TaskA else if (m == MB) TaskB else TaskL

  type AckMode = Early | Late
  // What a stored result says.
  type Result = Absent | Success | Failure
  pure def ready(s: Result): bool = match s {
    | Absent => false | Success => true | Failure => true
  }

  // A run in a pool process. a's run goes Started, Returned, LinkSent,
  // NextSent, then stores SUCCESS (Stored); b's and l's go Started, Stored.
  // `acked`: the message was acked (early ack, at the accept).
  type Stage = Started | Returned | LinkSent | NextSent | Stored
  type Run = { m: MsgId, stage: Stage, acked: bool }
  type Job = Idle | Running(Run)
  pure def isRunning(j: Job): bool = match j {
    | Idle => false | Running(_) => true
  }
  pure val NO_RUN: Run = { m: M(0), stage: Started, acked: false }
  pure def runOf(j: Job): Run = match j {
    | Idle => NO_RUN | Running(r) => r
  }

  // Why a run ended with FAILURE.
  type FailWhy =
    | BodyRaised | PublishRefused | StoreRaised | ChildLost | EncodeRaised
  type Action =
    | Init
    | Apply                                  // the client calls the chain
    | Deliver({ w: WorkerId, m: MsgId })
    | DedupSkip({ w: WorkerId, m: MsgId })
    | Body({ w: WorkerId, m: MsgId })
    | SendLink(WorkerId)
    | SendNext(WorkerId)
    | Store({ w: WorkerId, m: MsgId })
    | Ack({ w: WorkerId, m: MsgId })
    | Fail({ w: WorkerId, m: MsgId, why: FailWhy })
    | LoseNode(WorkerId)
    | Duplicate(MsgId)

  type Budget = {
    nodes: int,        // a worker is killed with its process (SIGKILL)
    children: int,     // a pool process is lost; the parent lives
    refusals: int,     // a link or next-step publish is refused for good
    storeErrors: int,  // mark_as_done raises once (a backend error)
    dups: int,         // a message is delivered again (delivery.qnt)
  }
  pure val NONE: Budget =
    { nodes: 0, children: 0, refusals: 0, storeErrors: 0, dups: 0 }
  pure def faults(b: Budget): int =
    b.nodes + b.children + b.refusals + b.storeErrors + b.dups

  type Config = {
    ack: AckMode,
    dedup: bool,            // worker_deduplicate_successful_tasks
    rejectOnLost: bool,     // task_reject_on_worker_lost
    aRaises: bool,          // a's body raises
    // a returns a value the result serializer cannot encode: mark_as_done
    // raises EncodeError every time (not a fault: the same each run)
    unencodable: bool,
    // design: encode the result before anything is published, so that an
    // unencodable result fails the task with nothing sent (main: false)
    encodeFirst: bool,
    // design: once the callbacks were sent, a store error records FAILURE
    // for the task only: no _fail_chain, no errbacks (main: false)
    quietAfterSend: bool,
    max: Budget,
  }
  pure def config(a: AckMode): Config =
    { ack: a, dedup: false, rejectOnLost: false, aRaises: false,
      unencodable: false, encodeFirst: false, quietAfterSend: false,
      max: NONE }
}

module chain {
  import chaintypes.*
  import observe.* from "../lib/observe"
  import delivery(MSGS = Set(MA, MB, ML), DMAX = { dups: CFG.max.dups,
                                                    losses: 0 }) as Q
    from "../lib/delivery"

  const CFG: Config

  pure val WORKERS: Set[WorkerId] = Set(W(1), W(2))
  pure val TASKS: Set[MsgId] = Set(MA, MB, ML)
  def late: bool = CFG.ack == Late

  // ---- state ----
  var applied: bool
  var jobs: WorkerId -> Job
  var stored: MsgId -> Result
  // ---- observations ----
  var pubs: MsgId -> int        // publishes of b and l by runs of a
  var starts: MsgId -> int      // runs whose body started
  var errbacks: int             // runs of a's errback
  var returnedOk: bool          // some run of a returned without an error
  var startedEarly: bool        // b started while a's result was not SUCCESS
  var flipped: bool             // a ready result of b changed
  var delivered: Set[MsgId]     // delivered at least once
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    Q::dinit, applied' = false, jobs' = WORKERS.mapBy(_ => Idle),
    stored' = TASKS.mapBy(_ => Absent), pubs' = TASKS.mapBy(_ => 0),
    starts' = TASKS.mapBy(_ => 0), errbacks' = 0, returnedOk' = false,
    startedEarly' = false, flipped' = false, delivered' = Set(),
    used' = NONE, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepObs: bool = all {
    pubs' = pubs, starts' = starts, errbacks' = errbacks,
    returnedOk' = returnedOk, startedEarly' = startedEarly,
    flipped' = flipped, delivered' = delivered,
  }
  def job(w: WorkerId): Run = runOf(jobs.get(w))
  def busy(w: WorkerId): bool = isRunning(jobs.get(w))
  action setRun(w: WorkerId, r: Run): bool =
    jobs' = jobs.set(w, Running(r))
  // A store that replaces a ready result with another one is seen by a
  // reader as a change of a final result.
  pure def flips(before: Result, after: Result): bool =
    ready(before) and before != after

  // ---- the client ----
  // chain.apply_async publishes the first task with the rest of the chain
  // in its options (canvas.py:1241-1276).
  // @cite celery celery/canvas.py:1241-1276 "result_from_apply = first_task.apply_async(**options)"
  action apply: bool = all {
    not(applied), applied' = true, Q::publish(MA),
    jobs' = jobs, stored' = stored, keepObs, used' = used, did(Apply),
  }

  // ---- a worker ----
  // An idle pool process gets a message. With early ack the message is
  // acked when the pool accepts it (request.py:524-533).
  // @cite celery celery/worker/request.py:524-533 "if not self.task.acks_late:"
  action deliver(w: WorkerId, m: MsgId): bool = all {
    not(busy(w)), Q::ready(m),
    not(CFG.dedup and late and delivered.contains(m)
        and stored.get(m) == Success),
    if (late) Q::deliver(m) else Q::take(m),
    setRun(w, { m: m, stage: Started, acked: not(late) }),
    starts' = starts.setBy(m, n => n + 1),
    startedEarly' = (startedEarly or (m == MB and stored.get(MA) != Success)),
    delivered' = delivered.union(Set(m)),
    pubs' = pubs, errbacks' = errbacks, returnedOk' = returnedOk,
    flipped' = flipped, stored' = stored, applied' = applied, used' = used,
    did(Deliver({ w: w, m: m })),
  }
  // worker_deduplicate_successful_tasks: a redelivered message whose
  // SUCCESS is stored is acked without a run; the stored children say the
  // links were sent (trace.py:485-543).
  // @cite celery celery/app/trace.py:485-543 "if deduplicate_successful_tasks and redelivered:"
  action dedupSkip(w: WorkerId, m: MsgId): bool = all {
    not(busy(w)), Q::ready(m), CFG.dedup, late, delivered.contains(m),
    stored.get(m) == Success,
    Q::take(m),
    jobs' = jobs, stored' = stored, keepObs, applied' = applied,
    used' = used, did(DedupSkip({ w: w, m: m })),
  }

  // a's body returns. (b and l store their result in one step: their
  // body and store are not what this spec is about.)
  // @cite celery celery/app/trace.py:564 "R = retval = fun(*args, **kwargs)"
  action body(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.m == MA, r.stage == Started, not(CFG.aRaises),
      setRun(w, { ...r, stage: Returned }),
      returnedOk' = true,
      pubs' = pubs, starts' = starts, errbacks' = errbacks,
      startedEarly' = startedEarly, flipped' = flipped, delivered' = delivered,
      Q::dkeep, stored' = stored, applied' = applied, used' = used,
      did(Body({ w: w, m: MA })),
    }
  }
  // The link callbacks first, then the next step of the chain, each its own
  // publish (trace.py:419-453).
  // @cite celery celery/app/trace.py:440-444 "signature(callbacks[0], app=app).apply_async, (retval,),"
  action sendLink(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.m == MA, r.stage == Returned,
      not(CFG.unencodable and CFG.encodeFirst),
      setRun(w, { ...r, stage: LinkSent }), Q::publish(ML),
      pubs' = pubs.setBy(ML, n => n + 1),
      starts' = starts, errbacks' = errbacks, returnedOk' = returnedOk,
      startedEarly' = startedEarly, flipped' = flipped, delivered' = delivered,
      stored' = stored, applied' = applied, used' = used,
      did(SendLink(w)),
    }
  }
  // @cite celery celery/app/trace.py:446-453 "_chsig.apply_async, (retval,), chain=chain[:-1],"
  action sendNext(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.m == MA, r.stage == LinkSent,
      setRun(w, { ...r, stage: NextSent }), Q::publish(MB),
      pubs' = pubs.setBy(MB, n => n + 1),
      starts' = starts, errbacks' = errbacks, returnedOk' = returnedOk,
      startedEarly' = startedEarly, flipped' = flipped, delivered' = delivered,
      stored' = stored, applied' = applied, used' = used,
      did(SendNext(w)),
    }
  }
  // mark_as_done: SUCCESS, with the children (trace.py:604-608).
  // @cite celery celery/app/trace.py:604-608 "task.backend.mark_as_done("
  action store(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.stage == (if (r.m == MA) NextSent else Started),
      not(r.m == MA and CFG.unencodable),
      setRun(w, { ...r, stage: Stored }),
      stored' = stored.set(r.m, Success),
      flipped' = (flipped or (r.m == MB and flips(stored.get(MB), Success))),
      pubs' = pubs, starts' = starts, errbacks' = errbacks,
      returnedOk' = returnedOk, startedEarly' = startedEarly,
      delivered' = delivered,
      Q::dkeep, applied' = applied, used' = used,
      did(Store({ w: w, m: r.m })),
    }
  }
  // on_success: late ack after the result is stored (request.py:616-631).
  // @cite celery celery/worker/request.py:616-631 "if self.task.acks_late:"
  action ack(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.stage == Stored,
      if (r.acked) Q::dkeep else Q::ack(r.m),
      jobs' = jobs.set(w, Idle),
      stored' = stored, keepObs, applied' = applied, used' = used,
      did(Ack({ w: w, m: r.m })),
    }
  }

  // ---- the failure path ----
  // FAILURE for the task, FAILURE for every later step of the chain (only
  // a carries the chain), and the errbacks (base.py:298-317, 319-360).
  // @cite celery celery/backends/base.py:298-317 "self._fail_chain(getattr(request, 'chain', None), exc, traceback,"
  def failed(m: MsgId): MsgId -> Result =
    if (m == MA) stored.set(MA, Failure).set(MB, Failure)
    else stored.set(m, Failure)
  // The run ends with FAILURE, and its message is acked: acks_on_failure
  // (request.py:749-768); an early-acked message is already gone.
  // @cite celery celery/worker/request.py:749-768 "ack_flag = self.task.acks_on_timeout if is_timeout else self.task.acks_on_failure"
  // quietAfterSend: a store error after the callbacks were sent
  // (callbacks_sent, trace.py:603) stores FAILURE for a and nothing else.
  action failRun(w: WorkerId, why: FailWhy): bool = {
    val r = job(w)
    val quietFail = CFG.quietAfterSend and r.m == MA and r.stage == NextSent
                    and why == StoreRaised
    all {
      stored' = if (quietFail) stored.set(MA, Failure) else failed(r.m),
      flipped' = (flipped
                  or (r.m == MA and not(quietFail)
                      and flips(stored.get(MB), Failure))
                  or (r.m == MB and flips(stored.get(MB), Failure))),
      errbacks' = if (r.m == MA and not(quietFail)) errbacks + 1
                  else errbacks,
      if (r.acked) Q::dkeep else Q::ack(r.m),
      jobs' = jobs.set(w, Idle),
      pubs' = pubs, starts' = starts, returnedOk' = returnedOk,
      startedEarly' = startedEarly, delivered' = delivered,
      applied' = applied,
      did(Fail({ w: w, m: r.m, why: why })),
    }
  }
  // a's body raises (trace.py:577-581, on_error).
  // @cite celery celery/app/trace.py:583-584 "I, R, state, retval = on_error(task_request, exc)"
  action bodyRaises(w: WorkerId): bool = all {
    busy(w), job(w).m == MA, job(w).stage == Started, CFG.aRaises,
    failRun(w, BodyRaised), used' = used,
  }
  // A link or the next step is refused until the retry policy gives up:
  // the OperationalError goes to on_error (trace.py:609-613; documented in
  // calling.rst, "the task fails through the normal failure and errback
  // handling").
  // @cite celery celery/app/trace.py:609-613 "except (EncodeError, OperationalError) as exc:"
  action refused(w: WorkerId): bool = all {
    busy(w), job(w).m == MA,
    job(w).stage == Returned or job(w).stage == LinkSent,
    used.refusals < CFG.max.refusals,
    failRun(w, PublishRefused),
    used' = { ...used, refusals: used.refusals + 1 },
  }
  // mark_as_done raises after the links were sent: the error is re-raised
  // (trace.py:610-611) and the tracer's last handler calls on_error
  // (trace.py:689-696), which stores FAILURE once the backend answers.
  // @cite celery celery/app/trace.py:689-696 "I, _, _, _ = on_error(task_request, exc)"
  // The same branch takes an OperationalError from mark_as_done: since
  // celery PR 10975 it is re-raised once the callbacks were sent
  // (trace.py:610-611), and reaches the same on_error; the worker then acks
  // with acks_on_failure (request.py:749-768), so nothing is redelivered.
  action storeRaises(w: WorkerId): bool = {
    val unencodable = job(w).m == MA and CFG.unencodable
    all {
      busy(w), job(w).stage == (if (job(w).m == MA) NextSent else Started),
      unencodable or used.storeErrors < CFG.max.storeErrors,
      failRun(w, StoreRaised),
      used' = if (unencodable) used
              else { ...used, storeErrors: used.storeErrors + 1 },
    }
  }
  // Design encodeFirst: the result is encoded before the links are sent;
  // EncodeError fails the task with nothing published.
  // @env design variant, not in celery: validate the result with the
  // backend's encoder before _dispatch_callbacks_and_chain
  action encodeFails(w: WorkerId): bool = all {
    busy(w), job(w).m == MA, job(w).stage == Returned,
    CFG.unencodable, CFG.encodeFirst,
    failRun(w, EncodeRaised), used' = used,
  }
  // The pool process is lost, the parent lives (Request.on_failure with
  // WorkerLostError, request.py:709-790): SUCCESS stored means ack; with
  // late ack and reject_on_worker_lost the message is requeued; otherwise
  // FAILURE as above.
  // @cite celery celery/worker/request.py:709-790 "success_meta = self._success_was_stored() if is_worker_lost else False"
  action childLost(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), used.children < CFG.max.children,
      if (stored.get(r.m) == Success) all {
        if (r.acked) Q::dkeep else Q::ack(r.m),
        jobs' = jobs.set(w, Idle), stored' = stored, keepObs,
        applied' = applied, did(Fail({ w: w, m: r.m, why: ChildLost })),
      } else if (late and CFG.rejectOnLost) all {
        Q::requeue(r.m), jobs' = jobs.set(w, Idle), stored' = stored,
        keepObs, applied' = applied,
        did(Fail({ w: w, m: r.m, why: ChildLost })),
      } else failRun(w, ChildLost),
      used' = { ...used, children: used.children + 1 },
    }
  }

  // ---- faults below the worker ----
  // The worker is killed with its pool: what it holds unacked goes back.
  // @env SIGKILL of the whole worker; the broker requeues what it held
  action loseNode(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), used.nodes < CFG.max.nodes,
      if (r.acked) Q::dkeep else Q::requeue(r.m),
      jobs' = jobs.set(w, Idle), stored' = stored, keepObs,
      applied' = applied, used' = { ...used, nodes: used.nodes + 1 },
      did(LoseNode(w)),
    }
  }
  // @env a message is delivered again (delivery.qnt)
  action duplicate(m: MsgId): bool = all {
    Q::duplicate(m), jobs' = jobs, stored' = stored, keepObs,
    applied' = applied, used' = { ...used, dups: used.dups + 1 },
    did(Duplicate(m)),
  }

  // The run of w stores a result that cannot be encoded.
  def unencodableFor(w: WorkerId): bool = job(w).m == MA and CFG.unencodable

  // What the code and the clock do on their own.
  action work: bool = any {
    apply,
    {
      nondet w = WORKERS.oneOf()
      any {
        body(w), sendLink(w), sendNext(w), store(w), ack(w),
        bodyRaises(w), encodeFails(w),
        // a result that cannot be encoded always raises: no budget
        all { unencodableFor(w), storeRaises(w) },
        { nondet m = TASKS.oneOf() any { deliver(w, m), dedupSkip(w, m) } },
      }
    },
  }

  // What may happen and need not: each of these uses up a budget.
  action fault: bool = any {
    {
      nondet w = WORKERS.oneOf()
      any {
        refused(w), childLost(w), loseNode(w),
        all { not(unencodableFor(w)), storeRaises(w) },
      }
    },
    { nondet m = TASKS.oneOf() duplicate(m) },
  }

  action step: bool = any { work, fault }

  // ---- properties ----------------------------------------------------------
  // invariants
  // The next step and the link are published at most once.
  val nextOnce: bool = pubs.get(MB) <= 1
  val linkOnce: bool = pubs.get(ML) <= 1
  // The next step is published only after a run of a returned (precedence).
  val nextAfterReturn: bool = pubs.get(MB) > 0 implies returnedOk
  // A run's callback and its errback are not both called.
  val notBoth: bool = not(errbacks > 0 and pubs.get(ML) > 0)
  val errbackOnce: bool = errbacks <= 1
  // A reader that saw b's result ready never sees it change.
  val finalStable: bool = not(flipped)
  // A reader never sees FAILURE for a task whose link or next step was
  // sent (and so runs).
  val noFailedParentAfterSend: bool =
    not(stored.get(MA) == Failure and (pubs.get(ML) > 0 or pubs.get(MB) > 0))

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = and {
    applied, WORKERS.forall(w => not(busy(w))),
    TASKS.forall(m => not(Q::ready(m))),
  }
  action stutter: bool = all {
    quiet, Q::dkeep, applied' = applied, jobs' = jobs, stored' = stored,
    keepObs, used' = used, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run in which the code keeps
  // working ends in a quiet state, for good, and that state has ended. The
  // fairness is on "work" only: no fault has to happen, and any that the
  // budgets allow may. Every action raises "steps", so that names the
  // steps of "work" that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(work, steps) implies eventually(always(quiet and quietDone))
  // A quiet chain has ended: a and b have a final result, and so has l
  // if it was sent. (Which final results is what the invariants are about.)
  val quietDone: bool = quiet implies and {
    ready(stored.get(MA)), ready(stored.get(MB)),
    pubs.get(ML) > 0 implies ready(stored.get(ML)),
  }
  // Exactly once while no fault was used: each task that was sent starts
  // once, and a quiet chain has stored a final result for each.
  val obs: Set[MsgObs] = Q::sent.map(m => {
    published: true, runs: starts.get(m), done: ready(stored.get(m)),
    held: Q::ready(m) or Q::taken(m), settled: ready(stored.get(m)),
  })
  val exactlyOnce: bool = exactlyOnceOf(faults(used) == 0, quiet, obs)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverDone: bool = stored.get(MB) != Success
  val neverFailed: bool = stored.get(MA) != Failure
  // b starts while a's SUCCESS is not stored yet: the links are published
  // before the result is stored, "so that result.children is populated"
  // (trace.py:591-593).
  val neverEarlyStart: bool = not(startedEarly)
}
