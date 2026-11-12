// -*- mode: Bluespec; -*-
// countdown and expires when the client's clock and the worker's clock
// differ: does a task with countdown C start no earlier than C after the
// call, is a task with expires X discarded only after X, and never run
// after X?
//
// Celery 768e476 (the same at 9c2f238). The client turns countdown and a
// numeric expires into absolute times with its own clock
// (amqp.py:386-400, app.now()). The worker compares them with its own:
// the strategy schedules the request at the ETA on the worker's timer
// (strategy.py:178-209, consumer.py:734-737), and Request.maybe_expire
// marks it REVOKED when the worker's now is past expires, at receipt and
// again before the pool runs it (request.py:415-421, 477-483, 1003-1006).
// calling.rst:257-287 promises the countdown as the earliest time,
// and calling.rst:348-366 "expires after one minute from now".
//
// Time is discrete: tt is the true time; the client's clock reads tt +
// clientOff and the worker's tt + workerOff. The call is at tt = 0. The
// message reaches the worker after a delay of up to 2 (a queue, a busy
// worker). The run ends at tt = T. Design `relative`: the message carries
// the countdown and the expiry as durations and the worker adds them to
// its receive time (not Celery's protocol; a comparison).
//
// Not modeled: an ETA given as a datetime (the same as a countdown on the
// client's clock), DST (calling.rst:270-276), the visibility timeout
// (redelivery.qnt), rate limits.
module etatypes {
  type Design = Absolute | Relative
  type Task = Waiting | Received({ eta: int, expires: int }) | Ran(int)
            | Discarded(int)

  type Action =
    | Init
    | Advance
    | Receive({ expired: bool })
    | Execute({ expired: bool })

  type Config = {
    design: Design,
    clientOff: int,
    workerOff: int,
    // countdown and expires in seconds; -1: not given
    countdown: int,
    expires: int,
  }
  pure def config(c: int, x: int): Config =
    { design: Absolute, clientOff: 0, workerOff: 0, countdown: c, expires: x }
  pure val NONE: int = -1
}

module etaskew {
  import etatypes.*

  const CFG: Config

  pure val T: int = 5
  pure val MAXDELAY: int = 2
  pure val FAR: int = 1000

  // ---- state ----
  var tt: int
  var task: Task
  var lastAction: Action
  var steps: int

  action init: bool = all {
    tt' = 0, task' = Waiting, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  def workerNow: int = tt + CFG.workerOff
  // What the message carries, read on the worker's clock.
  def etaAt(recv: int): int =
    if (CFG.countdown < 0) recv + CFG.workerOff
    else match CFG.design {
      | Absolute => CFG.clientOff + CFG.countdown
      | Relative => recv + CFG.workerOff + CFG.countdown
    }
  def expiresAt(recv: int): int =
    if (CFG.expires < 0) FAR
    else match CFG.design {
      | Absolute => CFG.clientOff + CFG.expires
      | Relative => recv + CFG.workerOff + CFG.expires
    }
  pure def isReceived(t: Task): bool = match t {
    | Waiting => false | Received(_) => true | Ran(_) => false
    | Discarded(_) => false
  }
  pure def recvOf(t: Task): { eta: int, expires: int } = match t {
    | Waiting => { eta: 0, expires: 0 } | Received(r) => r
    | Ran(_) => { eta: 0, expires: 0 } | Discarded(_) => { eta: 0, expires: 0 }
  }

  def curEta: int = recvOf(task).eta
  def curExpires: int = recvOf(task).expires

  // ---- actions ----
  // The worker receives the message: discarded as REVOKED if expired on
  // its clock, else scheduled at the ETA.
  // @cite celery celery/app/amqp.py:386-400 "add_seconds_to_datetime(now, countdown), tz=timezone,"
  // @cite celery celery/worker/request.py:415-421 "if now > self.expires:"
  // @cite celery celery/worker/strategy.py:160-163 "if (req.expires or req.id in revoked_tasks) and req.revoked():"
  action receive: bool = all {
    task == Waiting, tt <= MAXDELAY,
    val r = { eta: etaAt(tt), expires: expiresAt(tt) }
    val expired = workerNow > r.expires
    all {
      task' = if (expired) Discarded(tt) else Received(r),
      tt' = tt, did(Receive({ expired: expired })),
    }
  }
  // The ETA is due on the worker's clock: the request is checked again
  // and run, or discarded.
  // @cite celery celery/worker/consumer/consumer.py:734-737 "def apply_eta_task(self, task):"
  // @cite celery celery/worker/request.py:1003-1006 "if self.revoked():"
  action execute: bool = all {
    isReceived(task), workerNow >= curEta,
    val expired = workerNow > curExpires
    all {
      task' = if (expired) Discarded(tt) else Ran(tt),
      tt' = tt, did(Execute({ expired: expired })),
    }
  }
  // Time passes while nothing is due.
  // @env the clock
  action advance: bool = all {
    tt < T, task != Waiting or tt < MAXDELAY,
    not(isReceived(task) and workerNow >= curEta),
    tt' = tt + 1, task' = task, did(Advance),
  }

  // ---- step ----
  action step: bool = any { receive, execute, advance }

  // ---- properties ----------------------------------------------------------
  // invariants
  // A task with countdown C starts no earlier than C after the call.
  val notEarly: bool = match task {
    | Ran(t) => CFG.countdown < 0 or t >= CFG.countdown
    | Waiting => true | Received(_) => true | Discarded(_) => true
  }
  // A task with expires X is discarded only once X has passed.
  val notDiscardedEarly: bool = match task {
    | Discarded(t) => CFG.expires >= 0 and t > CFG.expires
    | Waiting => true | Received(_) => true | Ran(_) => true
  }
  // A task with expires X does not start after X.
  val noRunAfterExpiry: bool = match task {
    | Ran(t) => CFG.expires < 0 or t <= CFG.expires
    | Waiting => true | Received(_) => true | Discarded(_) => true
  }

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state (tt is bounded).
  val quiet: bool = tt == T and not(isReceived(task) and workerNow >= curEta)
    and task != Waiting
  action stutter: bool = all {
    quiet, tt' = tt, task' = task, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end the task ran or was discarded.
  val quietDone: bool = quiet implies match task {
    | Ran(_) => true | Discarded(_) => true
    | Waiting => false | Received(_) => false
  }

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRan: bool = match task {
    | Ran(_) => false | Waiting => true | Received(_) => true
    | Discarded(_) => true
  }
}
