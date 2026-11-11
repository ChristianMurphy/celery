// -*- mode: Bluespec; -*-
// Task.retry(): is a retried task never lost, never run beyond max_retries,
// and never retried once its SUCCESS is stored?
//
// Celery 768e476. One acks_late task with max_retries = 1 (two attempts);
// each run's body either returns or calls self.retry(), as a transient
// error decides. retry() (celery/app/task.py:930-963) builds a copy of the
// task with retries + 1 and the same id; with retries above max_retries it
// raises MaxRetriesExceededError instead (:937-946), and FAILURE is stored;
// otherwise it publishes the copy (S.apply_async()) and raises Retry; a
// publish that fails becomes Reject(requeue=False) (:958-961), which the
// worker records as FAILURE (request.py:681-690). The tracer stores RETRY
// (handle_retry, trace.py:191-215) and the worker acks the message
// (Request.on_retry, request.py:633-636). A store never replaces SUCCESS
// (base.py:1369-1370).
//
// The broker is the abstract at-least-once environment of
// ../lib/delivery.qnt: a message per attempt, M(0) the first and M(1) the
// retry; a worker killed with the message unacked has it requeued; a
// message can be delivered again (dups).
//
// Fenced writes (item 7): the design `fenced` stores the attempt number
// with the state and refuses a write of an older attempt, and a write that
// would replace a ready state of the same attempt with an unready one: a
// per-attempt epoch, a conditional write in the store (not in celery). The
// fault lateCopies is result_backend_always_retry's resend
// (base.py:827-860): a write whose reply was lost is sent again, and the
// first send can still reach the store later, after other writes.
//
// Not modeled: countdown and ETA (the retry waits; redelivery.qnt has the
// clock), retry_backoff, autoretry_for (the same retry() call), a pool
// process recycled by max_tasks_per_child or autoscale (see ../README.md:
// the real runs in the exactly-once report found no loss or duplicate).
module retrytypes {
  import deliverytypes.* from "../lib/delivery"
  export deliverytypes.*

  type WorkerId = { w: int }
  pure def W(i: int): WorkerId = { w: i }

  type Result = Pending | Retrying | Success | Failure
  pure def final(r: Result): bool = match r {
    | Pending => false | Retrying => false | Success => true | Failure => true
  }
  // A write as it was sent: the attempt that sent it and its state.
  type Write = { att: int, res: Result }
  // The stored state, the attempt that wrote it, whether a write ever
  // replaced a newer attempt's state or a ready state of its own attempt
  // with an unready one (stale), and every write sent so far.
  type Stored = { res: Result, att: int, stale: bool, sent: Set[Write] }
  pure val NOTHING: Stored = { res: Pending, att: -1, stale: false, sent: Set() }

  // A run of attempt `att` (its message M(att)). Started: the body runs.
  // RetrySent: retry() published the copy. RetryStored (design
  // retryFirst): RETRY is stored, the copy not yet published. Done: the
  // result is stored, the ack is due.
  type Stage = Started | RetrySent | RetryStored | Done
  type Run = { att: int, stage: Stage }
  type Job = Idle | Running(Run)
  pure def isRunning(j: Job): bool = match j { | Idle => false | Running(_) => true }
  pure val NO_RUN: Run = { att: 0, stage: Started }
  pure def runOf(j: Job): Run = match j { | Idle => NO_RUN | Running(r) => r }

  type Ending = Returned | RetryCalled | MaxExceeded | RetryRefused
  type Action =
    | Init
    | Apply
    | Deliver({ w: WorkerId, att: int })
    | Body({ w: WorkerId, att: int, ending: Ending })
    | StoreRetry(WorkerId)
    | Ack(WorkerId)
    | LoseNode(WorkerId)
    | Duplicate(MsgId)
    | LateCopy(Write)

  type Budget = { nodes: int, refusals: int, dups: int, lateCopies: int }
  pure val NONE: Budget = { nodes: 0, refusals: 0, dups: 0, lateCopies: 0 }
  pure def faults(b: Budget): int = b.nodes + b.refusals + b.dups + b.lateCopies

  type Config = {
    // the body can return: false makes every run call retry()
    canSucceed: bool,
    // design: store RETRY before the copy is published, so that the copy's
    // final state is written after it (main: false)
    retryFirst: bool,
    // design: fenced writes, a per-attempt epoch (main: false)
    fenced: bool,
    max: Budget,
  }
  pure def config(ok: bool): Config =
    { canSucceed: ok, retryFirst: false, fenced: false, max: NONE }
}

module retry {
  import retrytypes.*
  import delivery(MSGS = Set(M(0), M(1)), DMAX = { dups: CFG.max.dups,
                                                   losses: 0 }) as Q
    from "../lib/delivery"

  const CFG: Config

  pure val MAX_RETRIES: int = 1
  pure val WORKERS: Set[WorkerId] = Set(W(1), W(2))
  pure val ATTEMPTS: Set[int] = 0.to(MAX_RETRIES)

  // ---- state ----
  var applied: bool
  var jobs: WorkerId -> Job
  var stored: Stored
  // ---- observations ----
  var bodies: int               // body runs, all attempts
  var retryAfterSuccess: bool   // a retry was published while SUCCESS was stored
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    Q::dinit, applied' = false, jobs' = WORKERS.mapBy(_ => Idle),
    stored' = NOTHING, bodies' = 0, retryAfterSuccess' = false,
    used' = NONE, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepObs: bool = all {
    bodies' = bodies, retryAfterSuccess' = retryAfterSuccess,
  }
  def busy(w: WorkerId): bool = isRunning(jobs.get(w))
  def job(w: WorkerId): Run = runOf(jobs.get(w))
  // A store never replaces SUCCESS (base.py:1369-1370). A fenced store
  // also refuses an older attempt's write, and an unready state over a
  // ready one of the same attempt.
  def storing(att: int, r: Result): Stored = {
    val down = att == stored.att and final(stored.res) and not(final(r))
    val sent = stored.sent.union(Set({ att: att, res: r }))
    if (stored.res == Success or (CFG.fenced and (att < stored.att or down)))
      { ...stored, sent: sent }
    else
      { res: r, att: att, sent: sent,
        stale: stored.stale or att < stored.att or down }
  }

  // ---- actions ----
  // @cite celery celery/app/task.py:587-590 "def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,"
  action apply: bool = all {
    not(applied), applied' = true, Q::publish(M(0)),
    jobs' = jobs, stored' = stored, keepObs, used' = used, did(Apply),
  }
  // An idle pool process gets the message of an attempt and runs its body.
  // @cite celery celery/app/trace.py:564 "R = retval = fun(*args, **kwargs)"
  action deliver(w: WorkerId, att: int): bool = all {
    not(busy(w)), Q::ready(M(att)), Q::deliver(M(att)),
    jobs' = jobs.set(w, Running({ att: att, stage: Started })),
    bodies' = bodies + 1, retryAfterSuccess' = retryAfterSuccess,
    stored' = stored, applied' = applied, used' = used,
    did(Deliver({ w: w, att: att })),
  }
  // The body returns: mark_as_done stores SUCCESS (trace.py:604-608).
  // @cite celery celery/app/trace.py:604-608 "task.backend.mark_as_done("
  action bodyReturns(w: WorkerId): bool = all {
    busy(w), job(w).stage == Started, CFG.canSucceed,
    jobs' = jobs.set(w, Running({ ...job(w), stage: Done })),
    stored' = storing(job(w).att, Success),
    Q::dkeep, keepObs, applied' = applied, used' = used,
    did(Body({ w: w, att: job(w).att, ending: Returned })),
  }
  // The body calls self.retry(). At max_retries it raises
  // MaxRetriesExceededError: FAILURE (task.py:937-946). Otherwise it
  // publishes the copy (task.py:958-959).
  // @cite celery celery/app/task.py:937-946 "if max_retries is not None and retries > max_retries:"
  // @cite celery celery/app/task.py:958-963 "S.apply_async()"
  action bodyRetries(w: WorkerId): bool = {
    val r = job(w)
    val atMax = r.att + 1 > MAX_RETRIES
    all {
      busy(w), r.stage == Started,
      jobs' = jobs.set(w, Running({ ...r, stage:
        if (atMax) Done else if (CFG.retryFirst) RetryStored else RetrySent })),
      stored' = if (atMax) storing(r.att, Failure)
                else if (CFG.retryFirst) storing(r.att, Retrying) else stored,
      if (atMax or CFG.retryFirst) Q::dkeep else Q::publish(M(r.att + 1)),
      bodies' = bodies,
      retryAfterSuccess' = (retryAfterSuccess or
        (not(atMax) and not(CFG.retryFirst) and stored.res == Success)),
      applied' = applied, used' = used,
      did(Body({ w: w, att: r.att, ending: if (atMax) MaxExceeded else RetryCalled })),
    }
  }
  // The copy's publish fails: Reject(requeue=False) (task.py:958-961); the
  // worker stores FAILURE and drops the message (request.py:681-690).
  // @cite celery celery/app/task.py:958-961 "raise Reject(exc, requeue=False)"
  // @cite celery celery/worker/request.py:681-690 "self.task.backend.mark_as_failure("
  action retryRefused(w: WorkerId): bool = {
    val r = job(w)
    all {
      busy(w), r.stage == Started, r.att + 1 <= MAX_RETRIES,
      used.refusals < CFG.max.refusals,
      jobs' = jobs.set(w, Running({ ...r, stage: Done })),
      stored' = storing(r.att, Failure),
      Q::dkeep, keepObs, applied' = applied,
      used' = { ...used, refusals: used.refusals + 1 },
      did(Body({ w: w, att: r.att, ending: RetryRefused })),
    }
  }
  // handle_retry stores RETRY (trace.py:191-215).
  // @cite celery celery/app/trace.py:191-202 "task.backend.mark_as_retry("
  action storeRetry(w: WorkerId): bool = all {
    busy(w), job(w).stage == RetrySent,
    jobs' = jobs.set(w, Running({ ...job(w), stage: Done })),
    stored' = storing(job(w).att, Retrying),
    Q::dkeep, keepObs, applied' = applied, used' = used,
    did(StoreRetry(w)),
  }
  // The ack: on_success, on_retry (request.py:633-636) or acks_on_failure.
  // @cite celery celery/worker/request.py:633-636 "def on_retry(self, exc_info):"
  action ack(w: WorkerId): bool = all {
    busy(w), job(w).stage == Done,
    Q::ack(M(job(w).att)),
    jobs' = jobs.set(w, Idle), stored' = stored, keepObs,
    applied' = applied, used' = used, did(Ack(w)),
  }

  // Design retryFirst: the copy is published after RETRY was stored.
  // @env design variant, not in celery: mark_as_retry before S.apply_async()
  action publishRetry(w: WorkerId): bool = all {
    busy(w), job(w).stage == RetryStored,
    jobs' = jobs.set(w, Running({ ...job(w), stage: Done })),
    Q::publish(M(job(w).att + 1)),
    bodies' = bodies,
    retryAfterSuccess' = (retryAfterSuccess or stored.res == Success),
    stored' = stored, applied' = applied, used' = used,
    did(StoreRetry(w)),
  }

  // ---- faults ----
  // The worker is killed with its pool; its unacked message is requeued.
  // @env SIGKILL of the whole worker
  action loseNode(w: WorkerId): bool = all {
    busy(w), used.nodes < CFG.max.nodes,
    Q::requeue(M(job(w).att)),
    jobs' = jobs.set(w, Idle), stored' = stored, keepObs,
    applied' = applied, used' = { ...used, nodes: used.nodes + 1 },
    did(LoseNode(w)),
  }
  // @env a message is delivered again (lib/delivery.qnt)
  action duplicate(m: MsgId): bool = all {
    Q::duplicate(m), jobs' = jobs, stored' = stored, keepObs,
    applied' = applied, used' = { ...used, dups: used.dups + 1 },
    did(Duplicate(m)),
  }

  // The first send of a write whose reply was lost reaches the store after
  // other writes: result_backend_always_retry sent it again and went on
  // (base.py:827-860).
  // @cite celery celery/backends/base.py:827-842 "if self.always_retry and self.exception_safe_to_retry(exc):"
  action lateCopy(x: Write): bool = all {
    stored.sent.contains(x), used.lateCopies < CFG.max.lateCopies,
    stored' = storing(x.att, x.res), Q::dkeep, jobs' = jobs, keepObs,
    applied' = applied, used' = { ...used, lateCopies: used.lateCopies + 1 },
    did(LateCopy(x)),
  }

  // ---- step ----
  action step: bool = any {
    apply,
    { nondet w = WORKERS.oneOf()
      any {
        bodyReturns(w), bodyRetries(w), retryRefused(w), storeRetry(w),
        publishRetry(w), ack(w),
        loseNode(w),
        { nondet att = ATTEMPTS.oneOf() deliver(w, att) },
      } },
    { nondet m = Set(M(0), M(1)).oneOf() duplicate(m) },
    { nondet x = stored.sent.oneOf() lateCopy(x) },
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // The body runs at most max_retries + 1 times while no fault was used.
  val runsBounded: bool = faults(used) == 0 implies bodies <= MAX_RETRIES + 1
  // The body runs at most max_retries + 1 times, whatever happens.
  val runsBoundedAlways: bool = bodies <= MAX_RETRIES + 1
  // No retry is published while the task's SUCCESS is stored.
  val noRetryAfterSuccess: bool = not(retryAfterSuccess)
  // A published attempt is held by the broker or a worker until a final
  // state is stored: a retried task is never lost.
  val neverLost: bool = applied implies or {
    final(stored.res),
    WORKERS.exists(w => busy(w)),
    ATTEMPTS.exists(a => Q::ready(M(a)) or Q::taken(M(a))),
  }

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = and {
    applied, WORKERS.forall(w => not(busy(w))),
    ATTEMPTS.forall(a => not(Q::ready(M(a)))),
  }
  action stutter: bool = all {
    quiet, Q::dkeep, applied' = applied, jobs' = jobs, stored' = stored,
    keepObs, used' = used, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // A quiet task has a final result.
  val quietDone: bool = quiet implies final(stored.res)
  // No write replaces a newer attempt's state, or a ready state of its own
  // attempt with an unready one.
  val noStaleWrite: bool = not(stored.stale)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRetried: bool = bodies <= 1
  val neverFailed: bool = stored.res != Failure
}
