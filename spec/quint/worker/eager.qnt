// -*- mode: Bluespec; -*-
// Task options that meet: countdown with expires, retry with expires,
// retries up to max_retries, Ignore, Reject, link and link_error; run by a
// worker or eagerly (task_always_eager). Does a run never start before its
// countdown or after its expiry, does the caller see what the backend
// stores, and does the caller end with a final state?
//
// Celery 9c2f238. One task on one worker, a discrete clock t (the
// client's and the worker's clocks agree: eta.qnt has the skew).
//
// * Worker: the message waits until its eta (strategy.py:179-209); at the
//   start an expired request is marked REVOKED and not run
//   (request.py:415-421, 477-516). The body returns, raises, retries
//   (Task.retry publishes a copy with retries + 1 and the same expires,
//   task.py:195-210, 926-960, and RETRY is stored), raises Ignore
//   (nothing stored) or Reject(requeue=False) (FAILURE stored,
//   request.py:680-703). A link runs after SUCCESS, a link_error after
//   FAILURE.
// * Eager: apply_async calls apply() at once (task.py:736-760, 966-1030):
//   no countdown, no expiry, a retry runs again at once (task.py:949-955),
//   Ignore and Reject give the states IGNORED and REJECTED
//   (trace.py:563-576), and nothing is stored unless
//   task_store_eager_result is set.
//
// Probed on 9c2f238 and only listed in the README: autoretry_for with
// dont_autoretry_for, retry_backoff, throws, a caught soft_time_limit,
// typing=False.
module eagertypes {
  type Body = Returns | RetryOnce | RetryAlways | Ignores | Rejects | Raises
  // What the backend stores, or what the caller sees.
  type St = Pending | Retry | Success | Failure | Revoked | Ignored | Rejected
  type Msg = NoMsg | Waiting({ eta: int, retries: int })
  type Action =
    | Init
    | Call
    | Tick
    | Expire
    | Execute({ retries: int, outcome: St })

  type Config = {
    body: Body,
    eager: bool,
    countdown: int,
    expires: int,        // seconds after the call; -1: none
    retryCountdown: int,
    link: bool,          // a link and a link_error are given
  }
  pure def config(b: Body, e: bool): Config =
    { body: b, eager: e, countdown: 0, expires: -1, retryCountdown: 0,
      link: true }
  pure def final(s: St): bool = match s {
    | Pending => false | Retry => false | Success => true | Failure => true
    | Revoked => true | Ignored => false | Rejected => false
  }
}

module eager {
  import eagertypes.*

  const CFG: Config

  pure val MAX_RETRIES: int = 2
  pure val T: int = 8

  // ---- state ----
  var t: int
  var called: bool
  var msg: Msg
  var stored: St           // the result backend
  var seen: St             // what the caller's AsyncResult or EagerResult says
  var runs: int
  var early: bool          // a run started before its countdown or retry countdown
  var afterExpiry: bool    // a run started at or after its expiry
  var callbacks: int
  var errbacks: int
  var lastAction: Action
  var steps: int

  action init: bool = all {
    t' = 0, called' = false, msg' = NoMsg, stored' = Pending, seen' = Pending,
    runs' = 0, early' = false, afterExpiry' = false, callbacks' = 0,
    errbacks' = 0, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  def expired(at: int): bool = CFG.expires >= 0 and at >= CFG.expires
  // The outcome of a run with `retries` retries before it.
  pure def outcome(r: int): St = match CFG.body {
    | Returns => Success
    | RetryOnce => if (r == 0) Retry else Success
    | RetryAlways => if (r < MAX_RETRIES) Retry else Failure
    | Ignores => Ignored
    | Rejects => Rejected
    | Raises => Failure
  }
  // Runs in an eager call: a retry runs again at once.
  pure val EAGER_RUNS: int = match CFG.body {
    | Returns => 1 | RetryOnce => 2 | RetryAlways => MAX_RETRIES + 1
    | Ignores => 1 | Rejects => 1 | Raises => 1
  }
  pure val EAGER_END: St = outcome(EAGER_RUNS - 1)

  // ---- actions ----
  // The client calls apply_async: a message with the countdown's eta, or,
  // eagerly, apply() runs every attempt now and nothing is stored.
  // @cite celery@9c2f238 celery/app/task.py:736-760 "if app.conf.task_always_eager:"
  // @cite celery@9c2f238 celery/app/task.py:949-955 "# if task was executed eagerly using apply(),"
  // @cite celery@9c2f238 celery/app/trace.py:563-576 "I, R = Info(IGNORED, exc), ExceptionInfo(internal=True)"
  action call: bool = all {
    not(called), called' = true,
    if (CFG.eager) all {
      msg' = NoMsg, stored' = stored, seen' = EAGER_END, runs' = EAGER_RUNS,
      early' = (CFG.countdown > 0
                or (EAGER_RUNS > 1 and CFG.retryCountdown > 0)),
      afterExpiry' = expired(t),
      callbacks' = if (CFG.link and EAGER_END == Success) 1 else 0,
      errbacks' = if (CFG.link and EAGER_END == Failure) 1 else 0,
    } else all {
      msg' = Waiting({ eta: t + CFG.countdown, retries: 0 }),
      stored' = stored, seen' = seen, runs' = runs, early' = early,
      afterExpiry' = afterExpiry, callbacks' = callbacks, errbacks' = errbacks,
    },
    t' = t, did(Call),
  }

  // Time passes while the message waits for its eta.
  // @cite celery@9c2f238 celery/worker/strategy.py:179-209 "if req.eta:"
  action tick: bool = all {
    t < T,
    match msg { | NoMsg => false | Waiting(m) => m.eta > t },
    t' = t + 1,
    called' = called, msg' = msg, stored' = stored, seen' = seen,
    runs' = runs, early' = early, afterExpiry' = afterExpiry,
    callbacks' = callbacks, errbacks' = errbacks, did(Tick),
  }

  // The request is due and expired: REVOKED, not run.
  // @cite celery@9c2f238 celery/worker/request.py:415-421 "def maybe_expire(self):"
  action expire: bool = all {
    match msg { | NoMsg => false | Waiting(m) => m.eta <= t }, expired(t),
    msg' = NoMsg, stored' = Revoked, seen' = Revoked,
    t' = t, called' = called, runs' = runs, early' = early,
    afterExpiry' = afterExpiry, callbacks' = callbacks, errbacks' = errbacks,
    did(Expire),
  }

  // The worker runs the due request. A retry publishes a copy with the
  // same expires; Ignore stores nothing; Reject stores FAILURE.
  // @cite celery@9c2f238 celery/app/task.py:195-210 "'expires': self.expires,"
  // @cite celery@9c2f238 celery/worker/request.py:680-703 "if isinstance(exc, Reject):"
  action execute: bool = all {
    match msg { | NoMsg => false | Waiting(m) => m.eta <= t }, not(expired(t)),
    val r = match msg { | NoMsg => 0 | Waiting(m) => m.retries }
    val o = outcome(r)
    // what the backend stores for it
    val s = match o {
      | Pending => stored | Retry => Retry | Success => Success
      | Failure => Failure | Revoked => Revoked | Ignored => stored
      | Rejected => Failure
    }
    all {
      msg' = if (o == Retry) Waiting({ eta: t + CFG.retryCountdown,
                                        retries: r + 1 })
             else NoMsg,
      stored' = s, seen' = s, runs' = runs + 1,
      callbacks' = if (CFG.link and s == Success) callbacks + 1 else callbacks,
      errbacks' = if (CFG.link and s == Failure) errbacks + 1 else errbacks,
      did(Execute({ retries: r, outcome: o })),
    },
    val due = match msg { | NoMsg => 0 | Waiting(m) => m.eta }
    all { early' = (early or due > t), afterExpiry' = (afterExpiry or expired(t)) },
    t' = t, called' = called,
  }

  // ---- step ----
  action step: bool = any { call, tick, expire, execute }

  // ---- properties ----------------------------------------------------------
  // invariants
  // No run starts before its countdown, nor a retry before its countdown.
  val notEarly: bool = not(early)
  // No run starts at or after the task's expiry.
  val noRunAfterExpiry: bool = not(afterExpiry)
  // A link runs only after SUCCESS; at most one link or link_error call.
  val callbacksMatch: bool = (callbacks > 0 implies seen == Success)
    and (errbacks > 0 implies seen == Failure) and callbacks + errbacks <= 1
  // The number of runs stays within max_retries + 1.
  val runsBounded: bool = runs <= MAX_RETRIES + 1

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = called and msg == NoMsg
  action stutter: bool = all {
    quiet, t' = t, called' = called, msg' = msg, stored' = stored,
    seen' = seen, runs' = runs, early' = early, afterExpiry' = afterExpiry,
    callbacks' = callbacks, errbacks' = errbacks, lastAction' = lastAction,
    steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end the caller sees a final state: SUCCESS, FAILURE or REVOKED.
  val quietDone: bool = quiet implies final(seen)
  // At the end the caller sees what the backend stores.
  val callerAgrees: bool = quiet implies seen == stored

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRetried: bool = runs <= 1
}
