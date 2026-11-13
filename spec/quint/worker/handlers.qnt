// -*- mode: Bluespec; -*-
// The end of one task run: which handlers, signals and error callbacks
// fire, how often, and what is stored and acked, when the task returns,
// raises, hits its hard time limit, or loses its pool process.
//
// Celery 768e476 (request.py and trace.py are the same at 9c2f238). One
// task message with a link_error errback, acks_late, one worker with the
// prefork pool. The child runs build_tracer: on a raise, handle_failure
// stores FAILURE (mark_as_failure, which calls the errbacks), then calls
// Task.on_failure and sends task_failure; after_return follows
// (trace.py:221-251, 640-665). The pool reports to the main process:
// a result goes to Request.on_success (request.py:1026-1040), which turns
// a failed trace into on_failure(return_ok=True); an error (WorkerLostError,
// TimeLimitExceeded) goes to Request.on_failure (request.py:642-830). On a
// hard time limit billiard calls the error callback first and then the
// timeout callback, Request.on_timeout (request.py:539-614; billiard
// pool.py TimeoutHandler.on_hard_timeout: job._set, then
// job.handle_timeout). Each of the two stores FAILURE and sends
// task_failure; ack and reject are guarded by `acknowledged`
// (request.py:901-911).
//
// A message that is requeued runs again; the second run returns. The
// settings are per instance: acks_on_timeout (task_acks_on_timeout,
// default true) and reject_on_worker_lost (default false).
//
// Not modeled: Retry, Reject and Ignore raised by the body (ackmatrix.qnt
// has their ack outcome), soft time limits (a SoftTimeLimitExceeded the
// task does not catch is a raise; one it catches is a return), the
// task-failed event (it follows task_failure in both callbacks), chords
// (canvas/), retry_on_worker_lost, a cold shutdown.
module handlerstypes {
  type Outcome = Returns | Raises | HardLimit | WorkerLost

  // The main process's view of the run.
  type Main =
    | Idle               // nothing running
    | Running            // the child runs the body
    | Report(Outcome)    // the pool has a report for the main process
    | TimeoutNext        // hard limit: the error callback ran, the timeout
                         // callback is next

  type Stored = NoState | Failure | Success

  type Msg = Queued | Delivered | Acked

  // What one run did, as counts.
  type Hooks = { errback: int, failSignal: int, onFailure: int,
                 afterReturn: int }
  pure val NO_HOOKS: Hooks =
    { errback: 0, failSignal: 0, onFailure: 0, afterReturn: 0 }

  type Action =
    | Init
    | Deliver
    | ChildEnds(Outcome)
    | PoolResult
    | ErrorCallback({ outcome: Outcome, requeue: bool })
    | TimeoutCallback({ requeue: bool })

  type Config = {
    // what the first run does; a second run returns
    outcomes: Set[Outcome],
    acksOnTimeout: bool,
    rejectOnWorkerLost: bool,
  }
  pure def config(o: Outcome): Config =
    { outcomes: Set(o), acksOnTimeout: true, rejectOnWorkerLost: false }
}

module handlers {
  import handlerstypes.*

  const CFG: Config

  // ---- state ----
  var msg: Msg
  var main: Main
  var acked: bool          // Request.acknowledged in this run
  var requeued: bool       // this run's message was rejected with requeue
  var runs: int
  var stored: Stored
  var cur: Hooks           // this run
  var failedRuns: int      // runs whose hooks recorded a failure
  var maxHooks: Hooks      // the largest count of each hook in one run
  var errbackThenRun: bool // a run started after an errback was called
  var errbackBeforeSuccess: bool
  var mismatch: bool       // a run stored FAILURE without one of the hooks
  var lastAction: Action
  var steps: int

  action init: bool = all {
    msg' = Queued, main' = Idle, acked' = false, requeued' = false,
    runs' = 0, stored' = NoState, cur' = NO_HOOKS, failedRuns' = 0,
    maxHooks' = NO_HOOKS, errbackThenRun' = false,
    errbackBeforeSuccess' = false, mismatch' = false,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  pure def maxOf(a: Hooks, b: Hooks): Hooks = {
    errback: if (a.errback > b.errback) a.errback else b.errback,
    failSignal: if (a.failSignal > b.failSignal) a.failSignal else b.failSignal,
    onFailure: if (a.onFailure > b.onFailure) a.onFailure else b.onFailure,
    afterReturn: if (a.afterReturn > b.afterReturn) a.afterReturn else b.afterReturn,
  }
  // mark_as_failure (stores, calls the errbacks) and task_failure, as both
  // Request.on_failure and handle_failure do; `handler`: Task.on_failure
  // and after_return too.
  pure def failHooks(h: Hooks, handler: bool): Hooks = {
    errback: h.errback + 1, failSignal: h.failSignal + 1,
    onFailure: if (handler) h.onFailure + 1 else h.onFailure,
    afterReturn: if (handler) h.afterReturn + 1 else h.afterReturn,
  }
  action setHooks(h: Hooks): bool = all {
    cur' = h, maxHooks' = maxOf(maxHooks, h),
  }

  // ---- actions ----
  // The worker receives the message and the pool starts it (acks_late:
  // nothing is acked yet).
  // @cite celery celery/worker/request.py:1000-1020 "error_callback=self.on_failure,"
  action deliver: bool = all {
    msg == Queued, main == Idle, runs < 2,
    msg' = Delivered, main' = Running, acked' = false, requeued' = false,
    runs' = runs + 1, setHooks(NO_HOOKS),
    errbackThenRun' = (errbackThenRun or failedRuns > 0),
    stored' = stored, failedRuns' = failedRuns,
    errbackBeforeSuccess' = errbackBeforeSuccess, mismatch' = mismatch,
    did(Deliver),
  }
  // The child ends the body. A return stores SUCCESS and calls
  // after_return; a raise runs handle_failure (FAILURE, the errback,
  // on_failure, task_failure) and after_return. A hard limit or a lost
  // process does nothing in the child.
  // @cite celery celery/app/trace.py:221-251 "task.on_failure(exc, req.id, req.args, req.kwargs, einfo)"
  // @cite celery celery/app/trace.py:640-665 "task_after_return("
  action childEnds: bool = all {
    main == Running,
    nondet o = (if (runs == 1) CFG.outcomes else Set(Returns)).oneOf()
    all {
      main' = Report(o),
      match o {
        | Returns => all {
            stored' = Success,
            setHooks({ ...cur, afterReturn: cur.afterReturn + 1 }),
            errbackBeforeSuccess' = (errbackBeforeSuccess or failedRuns > 0),
          }
        | Raises => all {
            stored' = Failure, setHooks(failHooks(cur, true)),
            errbackBeforeSuccess' = errbackBeforeSuccess,
          }
        | HardLimit => all {
            stored' = stored, setHooks(cur),
            errbackBeforeSuccess' = errbackBeforeSuccess,
          }
        | WorkerLost => all {
            stored' = stored, setHooks(cur),
            errbackBeforeSuccess' = errbackBeforeSuccess,
          }
      },
      failedRuns' = if (o == Raises) failedRuns + 1 else failedRuns,
      msg' = msg, acked' = acked, requeued' = requeued, runs' = runs,
      errbackThenRun' = errbackThenRun, mismatch' = mismatch,
      did(ChildEnds(o)),
    }
  }
  // The trace's result reaches Request.on_success: a return or a raise
  // (return_ok) is acked; nothing more is stored.
  // @cite celery celery/worker/request.py:1026-1040 "return self.on_failure(retval, return_ok=True)"
  action poolResult: bool = all {
    main == Report(Returns) or main == Report(Raises),
    main' = Idle, msg' = if (acked) msg else Acked, acked' = true,
    requeued' = requeued, runs' = runs, stored' = stored, setHooks(cur),
    failedRuns' = failedRuns, errbackThenRun' = errbackThenRun,
    errbackBeforeSuccess' = errbackBeforeSuccess, mismatch' = mismatch,
    did(PoolResult),
  }
  // The pool's error callback: Request.on_failure with WorkerLostError or
  // TimeLimitExceeded. It requeues (reject_on_worker_lost; a timeout with
  // acks_on_timeout off) and stores nothing, or acks and then stores
  // FAILURE (the errback) and sends task_failure; it does not call
  // Task.on_failure or after_return.
  // @cite celery celery/worker/request.py:745-776 "or (is_timeout and not ack_flag)"
  // @cite celery celery/worker/request.py:776-796 "if not requeue and (is_worker_lost or not return_ok):"
  action errorCallback: bool = all {
    main == Report(HardLimit) or main == Report(WorkerLost),
    val o = if (main == Report(HardLimit)) HardLimit else WorkerLost
    val rq = (o == WorkerLost and CFG.rejectOnWorkerLost)
             or (o == HardLimit and not(CFG.acksOnTimeout))
    val h = if (rq) cur else failHooks(cur, false)
    all {
      main' = if (o == HardLimit) TimeoutNext else Idle,
      msg' = if (rq) Queued else Acked, acked' = true, requeued' = rq,
      stored' = if (rq) stored else Failure, setHooks(h),
      failedRuns' = if (rq) failedRuns else failedRuns + 1,
      mismatch' = (mismatch or (not(rq) and o == WorkerLost and h.onFailure == 0)),
      runs' = runs, errbackThenRun' = errbackThenRun,
      errbackBeforeSuccess' = errbackBeforeSuccess,
      did(ErrorCallback({ outcome: o, requeue: rq })),
    }
  }
  // billiard then calls the timeout callback, Request.on_timeout(soft=
  // False): it stores FAILURE (the errback), calls Task.on_failure,
  // after_return and task_failure, then acks or requeues; both are no-ops
  // when the error callback settled the message already.
  // @cite celery celery/worker/request.py:539-567 "self.task.on_failure(exc, self.id, self.args, self.kwargs, einfo)"
  // @cite celery celery/worker/request.py:609-614 "self.reject(requeue=True)"
  action timeoutCallback: bool = all {
    main == TimeoutNext,
    val rq = not(CFG.acksOnTimeout)
    all {
      main' = Idle, stored' = Failure, setHooks(failHooks(cur, true)),
      failedRuns' = failedRuns + 1,
      // guarded by acknowledged: the error callback already settled it
      msg' = msg, acked' = acked, requeued' = requeued,
      runs' = runs, errbackThenRun' = errbackThenRun,
      errbackBeforeSuccess' = errbackBeforeSuccess, mismatch' = mismatch,
      did(TimeoutCallback({ requeue: rq })),
    }
  }

  // ---- step ----
  action step: bool = any {
    deliver, childEnds, poolResult, errorCallback, timeoutCallback,
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // In one run, the error callback, task_failure and Task.on_failure each
  // fire at most once.
  val hooksOnce: bool =
    maxHooks.errback <= 1 and maxHooks.failSignal <= 1 and
    maxHooks.onFailure <= 1
  // A run that stores FAILURE calls Task.on_failure (as it calls the
  // errback and sends task_failure).
  val failureCallsHandler: bool = not(mismatch)
  // A task whose errback was called is not run again, and does not end
  // SUCCESS.
  val failedIsFinal: bool = not(errbackThenRun) and not(errbackBeforeSuccess)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state (at most two
  // runs, each a fixed number of steps).
  val quiet: bool = main == Idle and (msg == Acked or (msg == Queued and runs >= 2))
  action stutter: bool = all {
    quiet, msg' = msg, main' = main, acked' = acked, requeued' = requeued,
    runs' = runs, stored' = stored, cur' = cur, failedRuns' = failedRuns,
    maxHooks' = maxHooks, errbackThenRun' = errbackThenRun,
    errbackBeforeSuccess' = errbackBeforeSuccess, mismatch' = mismatch,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run that keeps going ends in
  // a quiet state, for good, and that state has ended. The spec has no
  // fault budgets (its events are part of the scenario), so the fairness
  // is on every action. Every action raises "steps", so that names the
  // steps that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(step, steps) implies eventually(always(quiet and quietDone))
  // At the end the message is acked and a final state is stored.
  val quietDone: bool = quiet implies (msg == Acked and stored != NoState)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverFailed: bool = stored != Failure
}
