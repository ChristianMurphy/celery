// -*- mode: Bluespec; -*-
// What `inspect` reports against what a worker holds, and a rate limit
// changed at run time. Does inspect active, reserved and scheduled show
// every task the worker has received and not finished, and does a raised
// rate limit apply to the tasks already waiting?
//
// Celery 9c2f238. One prefork worker with one process and enough prefetch
// for every message; three tasks, all received at once; a discrete clock.
//
// * A task with an eta is in scheduled (strategy.py:210-215); one with a
//   rate limit goes into its task's token bucket (strategy.py:216-217) and
//   is marked reserved only when the bucket releases it to the pool
//   (consumer.py:346-372); any other is reserved at once (strategy.py:
//   219). inspect reports active_requests, reserved_requests minus active,
//   and scheduled_requests (control.py:436-462).
// * The bucket releases one task per interval of its rate (6/m: one every
//   10 s, here every R ticks).
// * control.rate_limit replaces every task's bucket for the requests that
//   arrive later (control.py:281-306, consumer.py:317-320); the requests
//   already in the old bucket wait on the old bucket's timer.
//
// * control.revoke of task 2 adds its id to the revoked set and stores
//   REVOKED (control.py:221-260: a bucket request is not in
//   state.requests, so the reply to terminate=True is "tasks unknown").
//   The request is skipped where the worker next looks at it: at receive
//   (strategy.py:160-161), when the bucket or the timer releases it, or
//   when the pool takes it (request.py:348, 477; prefork discards it,
//   findings/revoke-queued-in-pool for the other pools). A request
//   released from the bucket has used a token (consumer.py:350-358), and
//   its message stays unacked until then.
// * query_task looks in state.requests (control.py:104-122), which holds
//   the reserved, active and (since celery PR 10535) scheduled requests,
//   not the bucket's: the same set as inspect, so inspectSeesAll covers it.
//
// Designs: `bucketVisible` (the bucket's requests are listed, as reserved),
// `moveOnRaise` (a rate change moves the waiting requests to the new
// bucket) and `dropRevoked` (a revoke takes the request out of the bucket
// and acks it, without a token).
//
// Not modeled: prefetch limits (priorities.qnt), terminate of a running
// task (revoke.qnt), several workers.
module insptypes {
  type Where = Queued | InBucket | InPool | Scheduled | Running | Done | Skipped
  type Action =
    | Init
    | Receive({ i: int })
    | Release({ i: int })
    | Start({ i: int })
    | Finish({ i: int })
    | Tick
    | RaiseRate
    | Revoke

  type Config = {
    limited: bool,       // the tasks have a rate limit
    eta: bool,           // the tasks have a countdown instead
    raiseRate: bool,     // control.rate_limit raises the rate once
    bucketVisible: bool, // design
    moveOnRaise: bool,   // design
    revoke: bool,        // control.revoke of task 2, once
    dropRevoked: bool,   // design
  }
  pure val PLAIN: Config =
    { limited: false, eta: false, raiseRate: false, bucketVisible: false,
      moveOnRaise: false, revoke: false, dropRevoked: false }
}

module inspect {
  import insptypes.*

  const CFG: Config

  pure val TASKS: Set[int] = Set(1, 2, 3)
  pure val R: int = 2      // the old rate's interval
  pure val ETA: int = 2    // the countdown of eta tasks
  pure val T: int = 8
  pure val REV: int = 2    // the task that may be revoked

  // ---- state ----
  var where: int -> Where
  var t: int
  var nextRelease: int     // when the old bucket releases its next request
  var raisedAt: int        // -1: the rate was not changed
  var revokedAt: int       // -1: task 2 was not revoked
  var lastAction: Action
  var steps: int

  action init: bool = all {
    where' = TASKS.mapBy(_ => Queued), t' = 0, nextRelease' = 0,
    raisedAt' = -1, revokedAt' = -1, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  def running: bool = TASKS.exists(i => where.get(i) == Running)
  def isRevoked(i: int): bool = i == REV and revokedAt >= 0
  // What inspect active, reserved and scheduled list.
  def listed(w: Where): bool = match w {
    | Queued => false | InBucket => CFG.bucketVisible | InPool => true
    | Scheduled => true | Running => true | Done => false | Skipped => false
  }

  // ---- actions ----
  // The worker receives a message: an eta request is scheduled, a rate
  // limited one goes into the bucket (the new one after a rate change:
  // its tokens are there at once), any other is reserved.
  // @cite celery@9c2f238 celery/worker/strategy.py:210-222 "return limit_task(req, bucket, 1)"
  action receive(i: int): bool = all {
    where.get(i) == Queued,
    where' = where.set(i, if (isRevoked(i)) Skipped
                          else if (CFG.eta) Scheduled
                          else if (CFG.limited and raisedAt < 0) InBucket
                          else InPool),
    t' = t, nextRelease' = nextRelease, raisedAt' = raisedAt,
    revokedAt' = revokedAt, did(Receive({ i: i })),
  }
  // The old bucket releases a request when it has a token, or the eta
  // arrives; the request is now reserved and goes to the pool, or, if it
  // was revoked, is skipped and acked (the bucket's token is used).
  // @cite celery@9c2f238 celery/worker/consumer/consumer.py:346-372 "def _schedule_bucket_request(self, bucket):"
  // @cite celery@9c2f238 celery/worker/request.py:477-480 "def revoked(self):"
  action release(i: int): bool = all {
    (where.get(i) == InBucket and t >= nextRelease)
      or (where.get(i) == Scheduled and t >= ETA),
    // the bucket keeps the order
    not(TASKS.exists(j => j < i and where.get(j) == where.get(i))),
    where' = where.set(i, if (isRevoked(i)) Skipped else InPool),
    nextRelease' = if (where.get(i) == InBucket) t + R else nextRelease,
    t' = t, raisedAt' = raisedAt, revokedAt' = revokedAt,
    did(Release({ i: i })),
  }
  // The one process takes a reserved request (prefork skips a revoked
  // one).
  // @cite celery@9c2f238 celery/worker/control.py:436-462 "state.tset(worker_state.reserved_requests) -"
  action start(i: int): bool = all {
    where.get(i) == InPool, not(running),
    not(TASKS.exists(j => j < i and where.get(j) == InPool)),
    where' = where.set(i, if (isRevoked(i)) Skipped else Running),
    t' = t, nextRelease' = nextRelease, raisedAt' = raisedAt,
    revokedAt' = revokedAt, did(Start({ i: i })),
  }
  // A run ends.
  // @env the task body (handlers.qnt has its endings)
  action finish(i: int): bool = all {
    where.get(i) == Running, where' = where.set(i, Done),
    t' = t, nextRelease' = nextRelease, raisedAt' = raisedAt,
    revokedAt' = revokedAt, did(Finish({ i: i })),
  }
  // Time passes while nothing is due: every message is received, no
  // request is ready to leave the bucket or the timer, and none waits for
  // an idle process (the timer and the pool act at once).
  // @env the clock
  action tick: bool = all {
    t < T, t' = t + 1,
    TASKS.forall(i => where.get(i) != Queued
      and not(where.get(i) == InBucket and t >= nextRelease)
      and not(where.get(i) == Scheduled and t >= ETA)
      and not(where.get(i) == InPool and not(running))),
    where' = where, nextRelease' = nextRelease, raisedAt' = raisedAt,
    revokedAt' = revokedAt, did(Tick),
  }
  // control.rate_limit raises the rate: later requests use a new bucket;
  // with moveOnRaise the waiting ones move to it too.
  // @cite celery@9c2f238 celery/worker/control.py:281-306 "state.consumer.reset_rate_limits()"
  // @cite celery@9c2f238 celery/worker/consumer/consumer.py:317-320 "def reset_rate_limits(self):"
  action raiseRate: bool = all {
    CFG.raiseRate, raisedAt < 0, raisedAt' = t,
    where' = if (CFG.moveOnRaise)
               TASKS.mapBy(i => if (where.get(i) == InBucket) InPool
                                else where.get(i))
             else where,
    t' = t, nextRelease' = nextRelease, revokedAt' = revokedAt,
    did(RaiseRate),
  }
  // control.revoke(task 2): the id joins the revoked set; with dropRevoked
  // a request waiting in the bucket leaves it at once, acked.
  // @cite celery@9c2f238 celery/worker/control.py:221-226 "worker_state.revoked.update(task_ids)"
  action revoke: bool = all {
    CFG.revoke, revokedAt < 0, revokedAt' = t,
    where' = if (CFG.dropRevoked and where.get(REV) == InBucket)
               where.set(REV, Skipped)
             else where,
    t' = t, nextRelease' = nextRelease, raisedAt' = raisedAt, did(Revoke),
  }

  // ---- step ----
  action step: bool = any {
    tick, raiseRate, revoke,
    nondet i = TASKS.oneOf()
    any { receive(i), release(i), start(i), finish(i) },
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // inspect lists every task the worker has received and not finished.
  val inspectSeesAll: bool = TASKS.forall(i =>
    (where.get(i) != Queued and where.get(i) != Done and where.get(i) != Skipped)
      implies listed(where.get(i)))
  // After a raise, no request waits for the old rate.
  val newRateApplies: bool = (raisedAt >= 0 and t > raisedAt)
    implies TASKS.forall(i => where.get(i) != InBucket)
  // Once time has passed after a revoke, the revoked request no longer
  // waits in the bucket (holding its message and a later token).
  val revokeFreesBucket: bool = (revokedAt >= 0 and t > revokedAt)
    implies where.get(REV) != InBucket

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  // The run ends at T, with nothing ready to move.
  val quiet: bool = t == T and TASKS.forall(i =>
    where.get(i) != Queued and where.get(i) != InPool
    and where.get(i) != Running
    and not(where.get(i) == InBucket and t >= nextRelease)
    and not(where.get(i) == Scheduled and t >= ETA))
  action stutter: bool = all {
    quiet, where' = where, t' = t, nextRelease' = nextRelease,
    raisedAt' = raisedAt, revokedAt' = revokedAt, lastAction' = lastAction,
    steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end every task ran, or was skipped because it was revoked.
  val quietDone: bool = quiet implies TASKS.forall(i =>
    where.get(i) == Done or (isRevoked(i) and where.get(i) == Skipped))

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverWaited: bool = TASKS.forall(i => where.get(i) != InBucket)
  val neverRevokedWaits: bool = not(isRevoked(REV) and where.get(REV) == InBucket)
}
