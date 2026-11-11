// -*- mode: Bluespec; -*-
// An abstract at-least-once delivery environment: what a task message can
// do between the broker and the workers, without the transport's details.
//
// The canvas and worker-level specs build on this instead of modeling a
// broker: a message is published (possibly more than once, by whoever
// publishes it), delivered to a run, acked, or put back (its worker or its
// connection was lost, the visibility timeout passed, the task rejected it
// with requeue). A message can be delivered again although it was acked
// (`dups`: a lost ack, a restore by tag, the timeout under a live run), and
// it can disappear only where the layer below can lose it (`losses`: a
// broker that loses acknowledged data). The detailed models of how the
// Redis transport and the pool do this are ../transport/redis_transport.qnt
// and ../worker/worker.qnt; ../worker/worker_projection.qnt checks that
// every step of worker.qnt is a step of this environment.
//
// The environment does not know what a message means: the spec that uses
// it maps its own message ids to tasks.
module deliverytypes {
  type MsgId = { m: int }
  pure def M(i: int): MsgId = { m: i }

  // What may go wrong below the worker. Each budget bounds how often it
  // can happen in one run of the model (the fairness assumption: a fault
  // is finite; a recovery step is never refused forever).
  type DeliveryBudget = { dups: int, losses: int }
  pure val NO_DELIVERY_FAULTS: DeliveryBudget = { dups: 0, losses: 0 }

  // The observable effect of one step on the broker's copies of messages.
  type DeliveryStep =
    | DNone                  // the broker's copies did not change
    | DPublish(MsgId)        // one more copy is ready
    | DDeliver(MsgId)        // a ready copy is handed to a consumer
    | DAck(MsgId)            // a delivered copy is settled
    | DRequeue(MsgId)        // a delivered copy is ready again
    | DTake(MsgId)           // a ready copy is taken and settled at once,
                             // or lost (the two look the same here)
    | DBad(MsgId)            // no step of the environment does this

  // The broker's copies of each message.
  type Copies = { ready: int, taken: int }
  pure val NO_COPIES: Copies = { ready: 0, taken: 0 }

  // Which steps of the environment take `before` to `after`: one per
  // message that changed (a pipeline that restores two messages is two
  // steps). DBad marks a change no step of the environment makes.
  pure def stepsOf(before: MsgId -> Copies, after: MsgId -> Copies,
                   msgs: Set[MsgId]): Set[DeliveryStep] =
    msgs.filter(m => before.get(m) != after.get(m)).map(m => {
      val b = before.get(m)
      val a = after.get(m)
      if (a.ready == b.ready + 1 and a.taken == b.taken) DPublish(m)
      else if (a.ready == b.ready - 1 and a.taken == b.taken + 1) DDeliver(m)
      else if (a.ready == b.ready and a.taken == b.taken - 1) DAck(m)
      else if (a.ready == b.ready + 1 and a.taken == b.taken - 1) DRequeue(m)
      else if (a.ready == b.ready - 1 and a.taken == b.taken) DTake(m)
      else DBad(m)
    })
  pure def isBad(s: DeliveryStep): bool = match s {
    | DNone => false | DPublish(_) => false | DDeliver(_) => false
    | DAck(_) => false | DRequeue(_) => false | DTake(_) => false
    | DBad(_) => true
  }
}

module delivery {
  import deliverytypes.*

  const MSGS: Set[MsgId]
  const DMAX: DeliveryBudget

  var copies: MsgId -> Copies
  // Messages published at least once.
  var sent: Set[MsgId]
  var dused: DeliveryBudget

  action dinit: bool = all {
    copies' = MSGS.mapBy(_ => NO_COPIES), sent' = Set(),
    dused' = NO_DELIVERY_FAULTS,
  }
  action dkeep: bool = all { copies' = copies, sent' = sent, dused' = dused }

  def ready(m: MsgId): bool = copies.get(m).ready > 0
  def taken(m: MsgId): bool = copies.get(m).taken > 0

  // basic.publish, or LPUSH: one more copy, whoever sends it.
  action publish(m: MsgId): bool = all {
    copies' = copies.setBy(m, c => { ...c, ready: c.ready + 1 }),
    sent' = sent.union(Set(m)), dused' = dused,
  }
  // A consumer takes a ready copy.
  action deliver(m: MsgId): bool = all {
    ready(m),
    copies' = copies.setBy(m, c => { ready: c.ready - 1, taken: c.taken + 1 }),
    sent' = sent, dused' = dused,
  }
  // A consumer takes a ready copy and settles it at once: early ack at the
  // accept, a message skipped as a duplicate, a no_ack consumer.
  action take(m: MsgId): bool = all {
    ready(m),
    copies' = copies.setBy(m, c => { ...c, ready: c.ready - 1 }),
    sent' = sent, dused' = dused,
  }
  // The consumer settles its copy.
  action ack(m: MsgId): bool = all {
    taken(m),
    copies' = copies.setBy(m, c => { ...c, taken: c.taken - 1 }),
    sent' = sent, dused' = dused,
  }
  // The copy goes back: the consumer was lost, closed its channel, rejected
  // it with requeue, or held it past the visibility timeout.
  action requeue(m: MsgId): bool = all {
    taken(m),
    copies' = copies.setBy(m, c => { ready: c.ready + 1, taken: c.taken - 1 }),
    sent' = sent, dused' = dused,
  }
  // A message that was settled, or is held, is delivered once more.
  action duplicate(m: MsgId): bool = all {
    sent.contains(m), dused.dups < DMAX.dups,
    copies' = copies.setBy(m, c => { ...c, ready: c.ready + 1 }),
    sent' = sent, dused' = { ...dused, dups: dused.dups + 1 },
  }
  // A ready copy is gone (only where the layer below can lose data).
  action lose(m: MsgId): bool = all {
    ready(m), dused.losses < DMAX.losses,
    copies' = copies.setBy(m, c => { ...c, ready: c.ready - 1 }),
    sent' = sent, dused' = { ...dused, losses: dused.losses + 1 },
  }

  // Nothing is left for a consumer to take, and nothing is held.
  val drained: bool = MSGS.forall(m => copies.get(m) == NO_COPIES)
}
