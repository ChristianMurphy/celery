// -*- mode: Bluespec; -*-
// Priorities and several queues: in what order does one worker process
// run the messages it is sent? Does a message start while a more urgent
// one of the same queue waits on the broker, and how many later messages
// can overtake a waiting one?
//
// Celery 768e476 (the same at 9c2f238), kombu 445027e. One worker with one
// process (-c 1), acks_late, prefetch count P = concurrency times
// worker_prefetch_multiplier (consumer.py:330-338): it holds at most P
// unacked messages, runs them in the order it received them, and acks
// each when it ends. Messages are published in a fixed order at times
// the model chooses.
//
// * Redis: each queue is split into lists per priority step; one BRPOP
//   names every queue's list of the most urgent step first, then the next
//   step (kombu/transport/redis.py:1457-1470, 1663-1672), so the most urgent message of
//   any consumed queue comes first (priority 0 is the most urgent,
//   routing.rst:263-270). Among queues of equal urgency the cycle rotates
//   the queue just served to the end (round_robin, the default) or keeps a
//   fixed order (queue_order_strategy 'priority', scheduling.py:94-98;
//   kombu/transport/redis.py:1484-1490).
// * RabbitMQ: a queue declared with x-max-priority delivers its most
//   urgent ready message first (9 is the most urgent, routing.rst:228-233);
//   without the argument it delivers in publish order. A consumer of two
//   queues gets them interleaved.
//
// Urgency is a rank here (higher is more urgent) so that one model covers
// both brokers; the instances name the broker's own numbers.
//
// Not modeled: concurrency above 1 (the same per process), ETA messages
// (held by the worker outside the prefetch count), the visibility timeout
// (redelivery.qnt), quorum queues (no x-max-priority).
module priotypes {
  type Broker = Redis | Rabbit
  type QueueOrder = RoundRobin | FixedOrder
  type MsgSpec = { q: int, rank: int }
  type Where = Unpublished | OnBroker | Held | Running | Done

  type Action =
    | Init
    | Publish({ m: int })
    | Fetch({ m: int })
    | Start({ m: int })
    | Finish({ m: int })

  type Config = {
    broker: Broker,
    prefetch: int,
    // RabbitMQ: the queues have x-max-priority
    maxPriority: bool,
    queueOrder: QueueOrder,
    msgs: List[MsgSpec],
  }
}

module priorities {
  import priotypes.*

  const CFG: Config

  val N: int = CFG.msgs.length()
  val IDS: Set[int] = 0.to(N - 1)

  // ---- state ----
  var where: int -> Where
  // the order in which messages reached the worker (held or running)
  var held: List[int]
  // start order: message -> position
  var started: int -> int
  var nstarted: int
  // starts while a more urgent message of the same queue waited on the broker
  var inversions: int
  // RabbitMQ: queue served last, for the round robin; Redis: the cycle
  var lastQ: int
  var lastAction: Action
  var steps: int

  action init: bool = all {
    where' = IDS.mapBy(_ => Unpublished), held' = List(),
    started' = IDS.mapBy(_ => -1), nstarted' = 0, inversions' = 0, lastQ' = 1,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  def onBroker: Set[int] = IDS.filter(m => where.get(m) == OnBroker)
  // m would start while a more urgent message of its queue waits
  def inverted(m: int): bool = onBroker.exists(o => o != m
    and qOf(CFG, o) == qOf(CFG, m) and rankOf(CFG, o) > rankOf(CFG, m))
  def busy: bool = IDS.exists(m => where.get(m) == Running)
  def unacked: int = IDS.filter(m => where.get(m) == Held or where.get(m) == Running).size()
  pure def rankOf(c: Config, m: int): int = c.msgs[m].rank
  pure def qOf(c: Config, m: int): int = c.msgs[m].q
  // the effective urgency: RabbitMQ without x-max-priority ignores it
  def urg(m: int): int =
    if (CFG.broker == Rabbit and not(CFG.maxPriority)) 0 else rankOf(CFG, m)
  // a queue's head: its most urgent message, oldest first
  def headOf(q: int): Set[int] = {
    val inQ = onBroker.filter(m => qOf(CFG, m) == q)
    val best = inQ.fold(-1, (acc, m) => if (urg(m) > acc) urg(m) else acc)
    val top = inQ.filter(m => urg(m) == best)
    top.filter(m => top.forall(o => o >= m))
  }
  // the queue order this fetch tries: the one not served last first
  def qOrder: List[int] = match CFG.queueOrder {
    | RoundRobin => if (lastQ == 0) [1, 0] else [0, 1]
    | FixedOrder => [0, 1]
  }
  // Which message the broker hands over next.
  def nextMsg: Set[int] = {
    val heads = headOf(0).union(headOf(1))
    val best = heads.fold(-1, (acc, m) => if (urg(m) > acc) urg(m) else acc)
    val top = heads.filter(m => urg(m) == best)
    match CFG.broker {
      // one BRPOP: the most urgent step first, queues in cycle order
      | Redis =>
        val first = qOrder[0]
        if (top.exists(m => qOf(CFG, m) == first)) top.filter(m => qOf(CFG, m) == first)
        else top
      // per queue: its head; the consumer alternates between queues
      | Rabbit =>
        val other = qOrder[0]
        if (headOf(other) != Set()) headOf(other) else heads
    }
  }

  // ---- actions ----
  // The client publishes the next message.
  // @env the client
  action publish: bool = all {
    val m = IDS.filter(i => where.get(i) == Unpublished)
                .fold(N, (acc, i) => if (i < acc) i else acc)
    all {
      m < N, where' = where.set(m, OnBroker), held' = held,
      started' = started, nstarted' = nstarted, inversions' = inversions,
      lastQ' = lastQ,
      did(Publish({ m: m })),
    }
  }
  // The broker delivers one message while the process runs another and
  // the worker holds fewer than P (prefetching ahead).
  // @cite kombu kombu/transport/redis.py:1457-1470 "keys = [self._q_for_pri(queue, pri) for pri in self.priority_steps"
  // @cite kombu kombu/transport/redis.py:1484-1490 "self._queue_cycle.rotate(dest)"
  // @cite celery celery/worker/consumer/consumer.py:330-338 "self.pool.num_processes * self.prefetch_multiplier"
  action fetch: bool = all {
    busy, unacked < CFG.prefetch, nextMsg != Set(),
    nondet m = nextMsg.oneOf()
    all {
      where' = where.set(m, Held), held' = held.append(m),
      started' = started, nstarted' = nstarted, inversions' = inversions,
      lastQ' = if (CFG.queueOrder == RoundRobin) qOf(CFG, m) else lastQ,
      did(Fetch({ m: m })),
    }
  }
  // The process is free and the worker holds nothing: the broker's next
  // message is delivered and starts (with P = 1 this is the only way a
  // message is delivered; the time between the two is left out).
  // @cite kombu kombu/transport/redis.py:1484-1490 "self.connection._deliver(loads(bytes_to_str(item)), dest)"
  action fetchStart: bool = all {
    not(busy), held.length() == 0, nextMsg != Set(),
    nondet m = nextMsg.oneOf()
    all {
      where' = where.set(m, Running), held' = held,
      started' = started.set(m, nstarted), nstarted' = nstarted + 1,
      inversions' = inversions + (if (inverted(m)) 1 else 0),
      lastQ' = if (CFG.queueOrder == RoundRobin) qOf(CFG, m) else lastQ,
      did(Fetch({ m: m })),
    }
  }
  // The process is free: the first message the worker holds starts.
  // @cite celery celery/worker/consumer/consumer.py:330-338 "self.initial_prefetch_count = ("
  action start: bool = all {
    not(busy), held.length() > 0,
    val m = held[0]
    all {
      where' = where.set(m, Running), held' = held.slice(1, held.length()),
      started' = started.set(m, nstarted), nstarted' = nstarted + 1,
      inversions' = inversions + (if (inverted(m)) 1 else 0),
      lastQ' = lastQ, did(Start({ m: m })),
    }
  }
  // The run ends and the message is acked.
  // @env the task body
  action finish: bool = all {
    nondet m = IDS.oneOf()
    all {
      where.get(m) == Running, where' = where.set(m, Done), held' = held,
      started' = started, nstarted' = nstarted, inversions' = inversions,
      lastQ' = lastQ,
      did(Finish({ m: m })),
    }
  }

  // ---- step ----
  action step: bool = any { publish, fetch, fetchStart, start, finish }

  // ---- properties ----------------------------------------------------------
  // invariants
  // A message does not start while a more urgent message of its queue
  // waits on the broker.
  val noInversion: bool = inversions == 0
  // A waiting message is overtaken by at most one message published after
  // it from another queue.
  val boundedOvertake: bool = IDS.forall(m =>
    IDS.filter(o => o > m and qOf(CFG, o) != qOf(CFG, m) and started.get(o) >= 0
      and (started.get(m) < 0 or started.get(o) < started.get(m))
      and rankOf(CFG, o) == rankOf(CFG, m)).size() <= 1)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = IDS.forall(m => where.get(m) == Done)
  action stutter: bool = all {
    quiet, where' = where, held' = held, started' = started,
    nstarted' = nstarted, inversions' = inversions, lastQ' = lastQ, lastAction' = lastAction,
    steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run that keeps going ends in
  // a quiet state, for good, and that state has ended. The spec has no
  // fault budgets (its events are part of the scenario), so the fairness
  // is on every action. Every action raises "steps", so that names the
  // steps that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(step, steps) implies eventually(always(quiet and quietDone))
  // At the end every message ran.
  val quietDone: bool = quiet implies IDS.forall(m => started.get(m) >= 0)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  // a later, more urgent message starts before an earlier one
  val neverReordered: bool = IDS.forall(m => IDS.forall(o =>
    (o > m and started.get(o) >= 0 and started.get(m) >= 0) implies started.get(m) < started.get(o)))
}
