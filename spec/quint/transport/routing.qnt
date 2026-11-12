// -*- mode: Bluespec; -*-
// Where a published task goes: a queue no worker consumes, a queue missing
// from task_queues, an exchange with no queue bound, an exchange that was
// never declared, a queue deleted after the client cached its declaration,
// a queue with a message TTL. Does a publish that returns put the task on
// a queue a worker reads, and is an error raised to the call it belongs to?
//
// Celery 768e476 (the same at 9c2f238), kombu 445027e, py-amqp 5.4.2. Two
// publishes from one client on one connection: m1 to the target under
// test, then m2 to the default queue, which a worker consumes. The worker
// runs what reaches a consumed queue.
//
// * A queue name missing from task_queues: created when
//   task_create_missing_queues is on (amqp.py:138-141), QueueNotFound when
//   it is off (routes.py:91-95).
// * Redis: an exchange with no binding for the routing key, declared or
//   not, routes to no queue; with no dead letter queue (the Redis channel
//   has none) the message is dropped and nothing is raised
//   (virtual/base.py:717-744).
// * RabbitMQ: an unroutable message is dropped by the broker (Celery does
//   not set mandatory). A publish to an exchange that does not exist closes
//   the channel with 404 NOT_FOUND. With confirm_publish the publish waits
//   for the broker and raises. Without it the publish returns; py-amqp
//   reads the pending Channel.Close before the next publish on that
//   connection (amqp channel.py:1790-1797, the connection.blocked check) and
//   raises it there, and that next message is not sent.
// * kombu caches a queue's declaration per connection (common.py:133-161);
//   a queue deleted later is not declared again by the client. A worker
//   consuming it gets basic.cancel, reconnects and declares it again
//   (consumer.py:383-400). On Redis LPUSH recreates the list and the
//   binding is permanent (probe on kombu 445027e): no window.
// * A queue with x-message-ttl drops a message nobody consumed in time
//   (kombu entity.py:474-480, RabbitMQ).
//
// Not modeled: x-expires (kombu does not cache the declaration of an
// expiring queue, entity.py:829-834, so the next publish declares it
// again; what is lost is what was in the queue, as with a TTL), alternate
// exchanges, mandatory with a return listener, topic and fanout patterns.
module routingtypes {
  type Broker = Redis | Rabbit
  type Target = Consumed | Unconsumed | Missing | Unbound | Undeclared
              | DeletedCached | TtlUnconsumed
  type Pub = NotYet | Returned | Raised({ own: bool })
  type Msg = Unsent | Queued({ consumed: bool }) | Dropped | Ran | Refused

  type Action =
    | Init
    | Publish1
    | Publish2
    | Redeclare
    | Run({ m: int })
    | Expire

  type Config = {
    broker: Broker,
    target: Target,
    confirm: bool,
    createMissing: bool,
  }
  pure def config(b: Broker, t: Target): Config =
    { broker: b, target: t, confirm: false, createMissing: true }
}

module routing {
  import routingtypes.*

  const CFG: Config

  // ---- state ----
  var pub: int -> Pub
  var msg: int -> Msg
  // RabbitMQ: a Channel.Close (404) the client has not read yet
  var pendingErr: bool
  // RabbitMQ: the queue was deleted after the client cached its declaration
  var queueGone: bool
  var lastAction: Action
  var steps: int

  action init: bool = all {
    pub' = Map(1 -> NotYet, 2 -> NotYet),
    msg' = Map(1 -> Unsent, 2 -> Unsent),
    pendingErr' = false,
    queueGone' = (CFG.target == DeletedCached and CFG.broker == Rabbit),
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // m1's outcome at the client and at the broker
  def first: (Pub, Msg, bool) = match CFG.target {
    | Consumed => (Returned, Queued({ consumed: true }), false)
    | Unconsumed => (Returned, Queued({ consumed: false }), false)
    | TtlUnconsumed => (Returned, Queued({ consumed: false }), false)
    | Missing =>
      if (CFG.createMissing) (Returned, Queued({ consumed: false }), false)
      else (Raised({ own: true }), Refused, false)
    | Unbound => (Returned, Dropped, false)
    | Undeclared =>
      if (CFG.broker == Redis) (Returned, Dropped, false)
      else if (CFG.confirm) (Raised({ own: true }), Refused, false)
      else (Returned, Dropped, true)
    | DeletedCached =>
      if (queueGone) (Returned, Dropped, false)
      else (Returned, Queued({ consumed: true }), false)
  }

  // ---- actions ----
  // m1 to the target.
  // @cite celery celery/app/routes.py:91-95 "raise QueueNotFound("
  // @cite celery celery/app/amqp.py:138-141 "if self.create_missing:"
  // @cite kombu kombu/transport/virtual/base.py:717-744 "if not R and default is not None:"
  // @cite kombu kombu/common.py:148-152 "if ident in declared:"
  action publish1: bool = all {
    pub.get(1) == NotYet,
    val r = first
    all {
      pub' = pub.set(1, r._1), msg' = msg.set(1, r._2),
      pendingErr' = r._3, queueGone' = queueGone, did(Publish1),
    }
  }
  // m2 to the default queue, after m1; an unread Channel.Close is raised
  // here and m2 is not sent.
  // @cite kombu kombu/messaging.py:358-370 "def _publish(self, body, priority, content_type, content_encoding,"
  action publish2: bool = all {
    pub.get(1) != NotYet, pub.get(2) == NotYet,
    if (pendingErr) all {
      pub' = pub.set(2, Raised({ own: false })), msg' = msg.set(2, Refused),
    } else all {
      pub' = pub.set(2, Returned), msg' = msg.set(2, Queued({ consumed: true })),
    },
    pendingErr' = false, queueGone' = queueGone, did(Publish2),
  }
  // The worker consuming the deleted queue gets basic.cancel, reconnects
  // and declares its queues again.
  // @cite celery celery/worker/consumer/consumer.py:383-400 "recoverable_errors = self.connection_errors"
  action redeclare: bool = all {
    queueGone, queueGone' = false,
    pub' = pub, msg' = msg, pendingErr' = pendingErr, did(Redeclare),
  }
  // The worker runs a message on a queue it consumes.
  // @env the worker (worker.qnt has the delivery itself)
  action runMsg(m: int): bool = all {
    msg.get(m) == Queued({ consumed: true }), msg' = msg.set(m, Ran),
    pub' = pub, pendingErr' = pendingErr, queueGone' = queueGone,
    did(Run({ m: m })),
  }
  // A message nobody consumed outlives the queue's x-message-ttl.
  // @env the broker's TTL (kombu entity.py:474-480 documents it)
  action expire: bool = all {
    CFG.target == TtlUnconsumed, msg.get(1) == Queued({ consumed: false }),
    msg' = msg.set(1, Dropped),
    pub' = pub, pendingErr' = pendingErr, queueGone' = queueGone, did(Expire),
  }

  // ---- step ----
  action step: bool = any {
    publish1, publish2, redeclare, expire,
    nondet m = Set(1, 2).oneOf()
    runMsg(m),
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // A message whose publish returned is not dropped.
  val noSilentDrop: bool = Set(1, 2).forall(m => msg.get(m) != Dropped)
  // An error is raised to the call whose message caused it.
  val errorIsOwn: bool = Set(1, 2).forall(m => pub.get(m) != Raised({ own: false }))

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = pub.get(2) != NotYet and not(queueGone)
    and Set(1, 2).forall(m => msg.get(m) != Queued({ consumed: true }))
    and not(CFG.target == TtlUnconsumed and msg.get(1) == Queued({ consumed: false }))
  action stutter: bool = all {
    quiet, pub' = pub, msg' = msg, pendingErr' = pendingErr,
    queueGone' = queueGone, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end every message whose publish returned has run.
  val quietDone: bool = quiet implies Set(1, 2).forall(m =>
    pub.get(m) == Returned implies msg.get(m) == Ran)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRaised: bool = Set(1, 2).forall(m => pub.get(m) == Returned or pub.get(m) == NotYet)
}
