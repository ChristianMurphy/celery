// -*- mode: Bluespec; -*-
// The publisher side, with RabbitMQ's semantics: when has the broker a
// message whose publish returned?
//
// Without publisher confirms (the default, `confirm_publish` off),
// apply_async() returns once the basic.publish frame is written to the
// socket. With confirms the publisher waits for the broker's basic.ack,
// which a durable queue sends after it wrote the message, and publishes
// again when no confirm arrives (py-amqp raises, kombu's retry policy
// sends the message again).
//
// Two messages, one durable queue. Faults: the broker is killed (it keeps
// what it wrote, and loses what it had only accepted), and the publisher's
// data is dropped with the connection left open.
//
// Not modeled: consumers, mandatory and returned messages, a quorum queue's
// replication (its window between accepting and writing is much smaller,
// not zero), and flow control.
module publishertypes {
  // Ids are one-field records (see chordtypes).
  type MsgId = { m: int }
  pure def M(i: int): MsgId = { m: i }

  type Confirms = ConfirmsOff | ConfirmsOn
  // What the publisher knows about a message.
  type Pub =
    | Unsent
    | Waiting      // sent, waiting for the broker's confirm
    | Returned     // apply_async() returned without an error

  type Action =
    | Init
    | Send(MsgId)
    | Arrive(MsgId)
    | WriteToDisk(MsgId)
    | Confirm(MsgId)
    | Resend(MsgId)
    | Crash
    | BrokerRestart
    | DropData

  type Budget = { crashes: int, drops: int }
  pure val NONE: Budget = { crashes: 0, drops: 0 }
  type Config = { confirms: Confirms, max: Budget }
}

module publisher {
  import publishertypes.*

  const CFG: Config

  pure val MSGS: Set[MsgId] = Set(M(1), M(2))

  var pub: MsgId -> Pub
  // basic.publish frames on their way to the broker.
  var frames: Set[MsgId]
  // Copies the queue accepted and has not written, and copies on disk.
  var memory: MsgId -> int
  var disk: MsgId -> int
  // basic.ack frames on their way to the publisher.
  var confirms: Set[MsgId]
  var brokerUp: bool
  var sent: int      // frames the publisher wrote and read
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    pub' = MSGS.mapBy(_ => Unsent), frames' = Set(),
    memory' = MSGS.mapBy(_ => 0), disk' = MSGS.mapBy(_ => 0),
    confirms' = Set(), brokerUp' = true, sent' = 0, used' = NONE,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action, n: int): bool = all {
    lastAction' = a, steps' = steps + 1, sent' = sent + n,
  }

  // The publisher writes the frame. Without confirms that is all.
  action send(m: MsgId): bool = all {
    pub.get(m) == Unsent, brokerUp,
    frames' = frames.union(Set(m)),
    pub' = pub.set(m, if (CFG.confirms == ConfirmsOn) Waiting else Returned),
    memory' = memory, disk' = disk, confirms' = confirms,
    brokerUp' = brokerUp, used' = used, did(Send(m), 1),
  }
  // The queue accepts the message.
  action arrive(m: MsgId): bool = all {
    frames.contains(m), brokerUp,
    frames' = frames.exclude(Set(m)),
    memory' = memory.setBy(m, n => n + 1),
    pub' = pub, disk' = disk, confirms' = confirms, brokerUp' = brokerUp,
    used' = used, did(Arrive(m), 0),
  }
  // The queue writes it, and confirms it when the publisher asked.
  action writeToDisk(m: MsgId): bool = all {
    memory.get(m) > 0, brokerUp,
    disk' = disk.setBy(m, n => n + memory.get(m)),
    memory' = memory.set(m, 0),
    confirms' = if (CFG.confirms == ConfirmsOn) confirms.union(Set(m))
                else confirms,
    pub' = pub, frames' = frames, brokerUp' = brokerUp, used' = used,
    did(WriteToDisk(m), 0),
  }
  action confirm(m: MsgId): bool = all {
    confirms.contains(m),
    confirms' = confirms.exclude(Set(m)),
    pub' = if (pub.get(m) == Waiting) pub.set(m, Returned) else pub,
    frames' = frames, memory' = memory, disk' = disk, brokerUp' = brokerUp,
    used' = used, did(Confirm(m), 1),
  }
  // No confirm is on its way: the wait times out and the publisher sends
  // the message again.
  def stalled(m: MsgId): bool = and {
    pub.get(m) == Waiting, not(frames.contains(m)), memory.get(m) == 0,
    not(confirms.contains(m)),
  }
  action resend(m: MsgId): bool = all {
    stalled(m), brokerUp,
    frames' = frames.union(Set(m)),
    pub' = pub, memory' = memory, disk' = disk, confirms' = confirms,
    brokerUp' = brokerUp, used' = used, did(Resend(m), 1),
  }

  // ---- faults ----
  // The broker is killed: what it had not written is gone, and so is
  // everything on the wire.
  action crash: bool = all {
    brokerUp, used.crashes < CFG.max.crashes,
    brokerUp' = false, memory' = MSGS.mapBy(_ => 0), frames' = Set(),
    confirms' = Set(), pub' = pub, disk' = disk,
    used' = { ...used, crashes: used.crashes + 1 }, did(Crash, 0),
  }
  action brokerRestart: bool = all {
    not(brokerUp), brokerUp' = true,
    pub' = pub, frames' = frames, memory' = memory, disk' = disk,
    confirms' = confirms, used' = used, did(BrokerRestart, 0),
  }
  // The network drops the publisher's data in both directions.
  action dropData: bool = all {
    frames != Set() or confirms != Set(), used.drops < CFG.max.drops,
    frames' = Set(), confirms' = Set(),
    pub' = pub, memory' = memory, disk' = disk, brokerUp' = brokerUp,
    used' = { ...used, drops: used.drops + 1 }, did(DropData, 0),
  }

  action step: bool = any {
    {
      nondet m = MSGS.oneOf()
      any { send(m), arrive(m), writeToDisk(m), confirm(m), resend(m) }
    },
    crash, brokerRestart, dropData,
  }

  // ---- properties ----
  // A message whose publish returned is on its way, accepted or written.
  val neverLost: bool = MSGS.forall(m => pub.get(m) == Returned implies (
    frames.contains(m) or memory.get(m) > 0 or disk.get(m) > 0))
  // The queue never has a message twice.
  val publishedOnce: bool = MSGS.forall(m => memory.get(m) + disk.get(m) <= 1)
  // Exactly once while no fault was used.
  val exactlyOnce: bool = used == NONE implies (
    publishedOnce and (quiet implies MSGS.forall(m => disk.get(m) == 1)))
  // A quiet system has every message on disk and every publish returned.
  val quietDone: bool = quiet implies MSGS.forall(m =>
    disk.get(m) > 0 and pub.get(m) == Returned)
  // One frame per publish without confirms, two with.
  val normalCost: bool = (used == NONE and quiet) implies
    sent == MSGS.size() * (if (CFG.confirms == ConfirmsOn) 2 else 1)
  // Witness (violated means reachable).
  val neverQuiet: bool = not(quiet)

  val quiet: bool = and {
    brokerUp, frames == Set(), confirms == Set(),
    MSGS.forall(m => pub.get(m) != Unsent and memory.get(m) == 0
                     and not(stalled(m))),
  }
  action stutter: bool = all {
    quiet,
    pub' = pub, frames' = frames, memory' = memory, disk' = disk,
    confirms' = confirms, brokerUp' = brokerUp, sent' = sent, used' = used,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
}
