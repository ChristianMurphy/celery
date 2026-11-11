// -*- mode: Bluespec; -*-
// kombu's Redis transport: how a consumer takes a message off a queue.
//
// Two designs, picked by CFG.design. Rules cite kombu/transport/redis.py.
//
//   BrpopThenRecord  main (445027e). The consumer sends BRPOP
//                    (_brpop_start :1457-1472); Redis pops the message; the
//                    consumer reads the reply (_brpop_read :1474-1496) and
//                    only then records the message in the unacked hash and
//                    index (QoS.append :474-495). Between the pop and the
//                    record the message is in no key (kombu issue 305).
//   PopScript        kombu PR 2831 (c61b525). One Lua script pops the
//                    message and records it (POP_UNACKED :223-305, sent at
//                    :1647-1672). A script cannot block, so a consumer that
//                    finds the queue empty waits on a wake list with BLPOP;
//                    publishers, restores and put-backs push a token
//                    (:1442-1445, 1977-2007), and a pop that leaves messages
//                    behind puts one back (:279-292). A reply that arrives
//                    without credit, or as old as the visibility timeout, is
//                    put back by a script that checks the record's score
//                    (PUT_BACK :313-326, _brpop_read :1689-1698). A message
//                    the script recorded under another tag than its own is
//                    recorded again in one transaction (QoS.append
//                    :601-624, _deliver_popped :1748-1760).
//   PopScriptPaused  the PR before eb41008: the script, but a reply without
//                    credit waits, as on main, and is delivered whatever
//                    its age.
//
// One queue, MSGS messages, two consumers with prefetch PREFETCH. Faults:
// a consumer is killed (max.crashes), a delivered message outlasts the
// visibility timeout while its consumer lives (max.heldTimeouts), a reply
// is delayed past the visibility timeout (max.replyTimeouts), a consumer
// loses its credit while a reply is on its way (max.creditLosses: celery
// lowers the prefetch count when an ETA task finishes), and the script reads
// another delivery tag than kombu does (max.mismatches).
//
// CFG.renewal adds the renewal of unacked messages proposed for kombu issue
// 337 on top of the PR: a consumer moves the index score of the tags it
// holds. RenewBlind is the prototype's `ZADD XX`, which also moves the
// score of a record another consumer wrote under the same tag after a
// restore; RenewOwn moves a score only if it is still the one this consumer
// knows. PUT_BACK compares that score.
//
// Not modeled: priorities and several queues, fanout, no_ack consumers,
// basic_get, a warm shutdown (restore_unacked), the broker losing data, and
// a connection that drops and heals. They belong to the multi-worker model.
module transporttypes {
  type Design = BrpopThenRecord | PopScriptPaused | PopScript
  pure def scripted(d: Design): bool = match d {
    | BrpopThenRecord => false | PopScriptPaused => true | PopScript => true
  }

  // Ids are one-field records with different field names, so they do not
  // typecheck in each other's place (see chordtypes).
  type MsgId = { m: int }
  type ConsumerId = { c: int }
  pure def M(i: int): MsgId = { m: i }
  pure def C(i: int): ConsumerId = { c: i }
  // The score a record has in the unacked index: when it was written.
  type Score = { at: int }

  // The key of a record in the unacked hash: the message's own delivery
  // tag, or another tag the pop script read from the message.
  type Tag = OwnTag | ScriptTag
  // A record in the unacked hash and index. `by` is who popped it; Redis
  // does not store that, the spec uses it to tell a restore after a crash
  // from a visibility timeout on a live consumer.
  // `moved`: a renewal changed the score since the pop wrote it.
  type Entry = {
    m: MsgId, tag: Tag, score: Score, by: ConsumerId, moved: bool,
  }
  type Renewal = NoRenewal | RenewBlind | RenewOwn
  // How PUT_BACK knows the record is still its pop's: by the index score
  // (the PR), or by a token the pop stores with the record, which no
  // renewal changes.
  type PutBackBy = ByScore | ByToken
  // A popped message on its way to the consumer.
  type Popped = {
    m: MsgId,
    score: Score,     // PopScript: the score the script recorded it with
    tag: Tag,         // PopScript: the tag the script recorded it under
    old: bool,        // the reply is as old as the visibility timeout
  }
  // The command a consumer has in flight on its polling connection.
  type Poll =
    | Idle              // none
    | Popping(Score)    // BRPOP or the pop script sent, not run yet
    | Reply(Popped)     // Redis popped a message; its reply is on the way
    | Waiting           // PopScript: BLPOP on the wake list
  // `emptied`: its last pop left every queue empty, so its next command is
  // the wait on the wake list, not a pop (_brpop_read :1700-1702).
  type Live = { poll: Poll, held: Set[MsgId], prefetch: int, emptied: bool }
  type Consumer = Dead | Alive(Live)

  pure def isAlive(c: Consumer): bool = match c {
    | Dead => false | Alive(_) => true
  }
  pure val NOT_LIVE: Live =
    { poll: Idle, held: Set(), prefetch: 0, emptied: false }
  // Only read under the guard isAlive(c).
  pure def liveOf(c: Consumer): Live = match c {
    | Dead => NOT_LIVE | Alive(s) => s
  }
  pure def isPopping(p: Poll): bool = match p {
    | Idle => false | Popping(_) => true | Reply(_) => false | Waiting => false
  }
  pure def isReply(p: Poll): bool = match p {
    | Idle => false | Popping(_) => false | Reply(_) => true | Waiting => false
  }
  pure val NO_SCORE: Score = { at: -1 }
  pure val NO_REPLY: Popped =
    { m: M(0), score: NO_SCORE, tag: OwnTag, old: false }
  pure def scoreOf(p: Poll): Score = match p {
    | Idle => NO_SCORE | Popping(s) => s | Reply(r) => r.score
    | Waiting => NO_SCORE
  }
  pure def replyOf(p: Poll): Popped = match p {
    | Idle => NO_REPLY | Popping(_) => NO_REPLY | Reply(r) => r
    | Waiting => NO_REPLY
  }

  type ReceiveOutcome =
    | Delivered           // to the application; counted against the prefetch
    | Rerecorded          // delivered, and recorded again under its own tag
    | PutBack             // back on the queue, its record removed
    | PutBackTooLate      // the record is gone or another pop's: left alone
  type RestoreReason =
    | HolderDead          // the consumer that popped it was killed
    | HeldTooLong         // a live consumer holds it past the timeout
    | ReplyTooLong        // its reply is still on the way, or was put back
    | Stranded            // its consumer dropped the reply, nobody renews it
  type Action =
    | Init
    | Publish(MsgId)
    | SendPop(ConsumerId)
    | ServerPop({ c: ConsumerId, m: MsgId, tag: Tag })
    | PopEmpty(ConsumerId)
    | WaitAfterEmpty(ConsumerId)
    | WakeUp(ConsumerId)
    | WaitTimeout(ConsumerId)
    | Receive({ c: ConsumerId, m: MsgId, outcome: ReceiveOutcome })
    | Ack({ c: ConsumerId, m: MsgId })
    | Crash(ConsumerId)
    | Restore({ c: ConsumerId, m: MsgId, reason: RestoreReason })
    | LoseCredit(ConsumerId)
    | RegainCredit(ConsumerId)
    | RenewOthers({ c: ConsumerId, m: MsgId })

  type Faults = {
    crashes: int, heldTimeouts: int, replyTimeouts: int, creditLosses: int,
    mismatches: int,
  }
  pure val NO_FAULTS: Faults = {
    crashes: 0, heldTimeouts: 0, replyTimeouts: 0, creditLosses: 0,
    mismatches: 0,
  }
  type Config = {
    design: Design, renewal: Renewal, putBack: PutBackBy, max: Faults,
  }
}

module redis_transport {
  import transporttypes.*

  const CFG: Config

  pure val MSGS: Set[MsgId] = Set(M(1), M(2))
  pure val CONSUMERS: Set[ConsumerId] = Set(C(1), C(2))
  pure val PREFETCH: int = 1

  // ---- Redis ----
  var queue: Set[MsgId]         // the queue's list (order is not modeled)
  var unacked: Set[Entry]       // the unacked hash and its index
  var wake: bool                // the wake list holds a token (PopScript)
  // ---- clients ----
  var unpublished: Set[MsgId]
  var consumers: ConsumerId -> Consumer
  var clock: int                // for scores
  // ---- observations ----
  var acked: Set[MsgId]         // acked by a consumer
  var deliveries: MsgId -> int  // times handed to an application callback
  var ops: int                  // round trips to Redis so far
  var used: Faults
  var lastAction: Action
  var steps: int

  action init: bool = all {
    queue' = Set(), unacked' = Set(), wake' = false,
    unpublished' = MSGS,
    consumers' = CONSUMERS.mapBy(_ => Alive({
      poll: Idle, held: Set(), prefetch: PREFETCH, emptied: false })),
    clock' = 0, acked' = Set(), deliveries' = MSGS.mapBy(_ => 0), ops' = 0,
    used' = NO_FAULTS, lastAction' = Init, steps' = 0,
  }

  // Every action records itself, counts one step and `n` round trips.
  action did(a: Action, n: int): bool = all {
    lastAction' = a, steps' = steps + 1, ops' = ops + n,
  }
  action keepRedis: bool = all {
    queue' = queue, unacked' = unacked, wake' = wake,
  }
  action keepObs: bool = all { acked' = acked, deliveries' = deliveries }

  def live(c: ConsumerId): Live = liveOf(consumers.get(c))
  def alive(c: ConsumerId): bool = isAlive(consumers.get(c))
  def credit(c: ConsumerId): bool = live(c).held.size() < live(c).prefetch
  pure def entriesOf(u: Set[Entry], m: MsgId): Set[Entry] =
    u.filter(e => e.m == m)
  // A record whose consumer is alive, does not hold the message and has no
  // reply for it on the way: that consumer will neither ack it nor put it
  // back.
  def stranded(e: Entry): bool = and {
    alive(e.by), not(live(e.by).held.contains(e.m)),
    not(isReply(live(e.by).poll) and replyOf(live(e.by).poll).m == e.m
        and replyOf(live(e.by).poll).score == e.score),
  }
  // Another consumer holds the same tag and keeps renewing it.
  def renewedByAnother(e: Entry): bool =
    CFG.renewal == RenewBlind and CONSUMERS.exists(c =>
      c != e.by and alive(c) and live(c).held.contains(e.m))

  // A publisher puts a message on the queue (_put :1974-2007); with the PR
  // it also pushes a wake token in the same pipeline.
  action publish(m: MsgId): bool = all {
    unpublished.contains(m),
    unpublished' = unpublished.exclude(Set(m)),
    queue' = queue.union(Set(m)),
    wake' = (wake or scripted(CFG.design)),
    unacked' = unacked, consumers' = consumers, clock' = clock, keepObs,
    used' = used, did(Publish(m), 1),
  }

  // A consumer with credit sends its pop (_brpop_start; QoS.can_consume).
  // The script's score is the time the command was built (:1668, 1645).
  action sendPop(c: ConsumerId): bool = all {
    alive(c), live(c).poll == Idle, credit(c), not(live(c).emptied),
    consumers' = consumers.set(c, Alive({ ...live(c),
                                          poll: Popping({ at: clock }) })),
    clock' = clock + 1,
    keepRedis, unpublished' = unpublished, keepObs, used' = used,
    did(SendPop(c), 1),
  }

  // Redis runs the pop.
  //   main: BRPOP removes the message; nothing else is written.
  //   PR:   POP_UNACKED removes it, records it in unacked with the score
  //         sent, and leaves a wake token if the queue is not empty
  //         (:270-304). `mismatch`: the tag the script read is not the one
  //         kombu reads from the message.
  action serverPop(c: ConsumerId, m: MsgId, mismatch: bool): bool = {
    val tag = if (mismatch) ScriptTag else OwnTag
    val score = scoreOf(live(c).poll)
    val rest = queue.exclude(Set(m))
    all {
      alive(c), isPopping(live(c).poll), queue.contains(m),
      not(mismatch) or (scripted(CFG.design)
                        and used.mismatches < CFG.max.mismatches),
      queue' = rest,
      unacked' = if (scripted(CFG.design))
                   unacked.union(Set({ m: m, tag: tag, score: score, by: c,
                                       moved: false }))
                 else unacked,
      wake' = (wake or (scripted(CFG.design) and rest != Set())),
      consumers' = consumers.set(c, Alive({
        ...live(c), emptied: scripted(CFG.design) and rest == Set(),
        poll: Reply({ m: m, score: score, tag: tag, old: false }) })),
      used' = if (mismatch) { ...used, mismatches: used.mismatches + 1 }
              else used,
      unpublished' = unpublished, clock' = clock, keepObs,
      // the round trip was counted when the command was sent
      did(ServerPop({ c: c, m: m, tag: tag }), 0),
    }
  }

  // PR: the pop found every queue empty; the consumer waits on the wake
  // list next (_brpop_read :1700-1722). On main BRPOP itself blocks.
  action popEmpty(c: ConsumerId): bool = all {
    scripted(CFG.design), alive(c), isPopping(live(c).poll), queue == Set(),
    consumers' = consumers.set(c, Alive({ ...live(c), poll: Waiting })),
    keepRedis, unpublished' = unpublished, clock' = clock, keepObs,
    used' = used, did(PopEmpty(c), 1),
  }

  // PR: the last pop said every queue was empty, so the consumer waits on
  // the wake list without popping first, whatever was published since.
  action waitAfterEmpty(c: ConsumerId): bool = all {
    alive(c), live(c).poll == Idle, credit(c), live(c).emptied,
    consumers' = consumers.set(c, Alive({
      ...live(c), poll: Waiting, emptied: false })),
    keepRedis, unpublished' = unpublished, clock' = clock, keepObs,
    used' = used, did(WaitAfterEmpty(c), 1),
  }

  // PR: BLPOP takes the token; the consumer sends its pop (:1713-1722).
  action wakeUp(c: ConsumerId): bool = all {
    alive(c), live(c).poll == Waiting, wake, credit(c),
    wake' = false,
    consumers' = consumers.set(c, Alive({ ...live(c),
                                          poll: Popping({ at: clock }) })),
    clock' = clock + 1,
    queue' = queue, unacked' = unacked, unpublished' = unpublished, keepObs,
    used' = used, did(WakeUp(c), 1),
  }

  // PR: the wait times out (brpop_timeout) and the consumer pops anyway.
  // Only with a message queued: a timeout over an empty queue leads back
  // to the same wait.
  action waitTimeout(c: ConsumerId): bool = all {
    alive(c), live(c).poll == Waiting, queue != Set(), credit(c),
    consumers' = consumers.set(c, Alive({ ...live(c),
                                          poll: Popping({ at: clock }) })),
    clock' = clock + 1,
    keepRedis, unpublished' = unpublished, keepObs, used' = used,
    did(WaitTimeout(c), 1),
  }

  // The consumer reads the reply of its pop.
  //   main  only with credit (a reply without credit stays unread,
  //         :687-694 at 445027e); QoS.append records the message, then it
  //         is delivered.
  //   PR    without credit, or with a reply as old as the visibility
  //         timeout: PUT_BACK. It removes the record and pushes the message
  //         back only if the record still has the pop's score; a restore
  //         has put the message back otherwise. Else deliver; a message
  //         recorded under another tag is recorded again under its own, in
  //         the transaction that drops the script's record.
  action receive(c: ConsumerId): bool = {
    val s = live(c)
    val p = replyOf(s.poll)
    val putsBack = CFG.design == PopScript and (not(credit(c)) or p.old)
    // PUT_BACK: the record still has the score this pop wrote
    val mine = unacked.filter(e =>
      e.m == p.m and e.tag == p.tag and e.score == p.score
      and (CFG.putBack == ByToken or not(e.moved)))
    val outcome: ReceiveOutcome =
      if (putsBack) (if (mine != Set()) PutBack else PutBackTooLate)
      else if (scripted(CFG.design) and p.tag == ScriptTag) Rerecorded
      else Delivered
    val delivers = not(putsBack)
    val own: Entry =
      { m: p.m, tag: OwnTag, score: { at: clock }, by: c, moved: false }
    all {
      alive(c), isReply(s.poll), putsBack or credit(c),
      unacked' = if (outcome == PutBack) unacked.exclude(mine)
                 else if (outcome == Rerecorded)
                   unacked.exclude(mine).union(Set(own))
                 else if (outcome == Delivered and not(scripted(CFG.design)))
                   unacked.union(Set(own))
                 else unacked,
      queue' = if (outcome == PutBack) queue.union(Set(p.m)) else queue,
      wake' = (wake or outcome == PutBack),
      consumers' = consumers.set(c, Alive({
        ...s, poll: Idle,
        held: if (delivers) s.held.union(Set(p.m)) else s.held })),
      deliveries' = if (delivers) deliveries.setBy(p.m, n => n + 1)
                    else deliveries,
      clock' = clock + 1, acked' = acked, unpublished' = unpublished,
      used' = used,
      // main: the record; PR: the put-back or the second record
      did(Receive({ c: c, m: p.m, outcome: outcome }),
          if (outcome == Delivered and scripted(CFG.design)) 0 else 1),
    }
  }

  // QoS.ack: remove the record under the message's own tag (:636-638).
  action ack(c: ConsumerId, m: MsgId): bool = all {
    alive(c), live(c).held.contains(m),
    unacked' = unacked.filter(e => not(e.m == m and e.tag == OwnTag)),
    acked' = acked.union(Set(m)),
    consumers' = consumers.set(c, Alive({
      ...live(c), held: live(c).held.exclude(Set(m)) })),
    queue' = queue, wake' = wake, deliveries' = deliveries,
    unpublished' = unpublished, clock' = clock, used' = used,
    did(Ack({ c: c, m: m }), 1),
  }

  // ---- faults ----
  // The consumer is killed: nothing it held in memory survives.
  action crash(c: ConsumerId): bool = all {
    alive(c), used.crashes < CFG.max.crashes,
    consumers' = consumers.set(c, Dead),
    used' = { ...used, crashes: used.crashes + 1 },
    keepRedis, unpublished' = unpublished, clock' = clock, keepObs,
    did(Crash(c), 0),
  }

  // restore_visible (:660-686) by any live consumer: a record older than
  // the visibility timeout goes back to the queue (restore_by_tag
  // :688-699), with a wake token on the PR. Free of charge when the
  // consumer that popped the message is dead; a fault when it is alive.
  action restore(c: ConsumerId, e: Entry): bool = {
    val holder = consumers.get(e.by)
    val h = liveOf(holder)
    val reason: RestoreReason =
      if (not(isAlive(holder))) HolderDead
      else if (h.held.contains(e.m)) HeldTooLong
      else if (stranded(e)) Stranded else ReplyTooLong
    // the holder's reply for this very pop is now as old as the timeout
    val late = isAlive(holder) and isReply(h.poll)
               and replyOf(h.poll).m == e.m and replyOf(h.poll).score == e.score
    val marked = if (late)
                   consumers.set(e.by, Alive({
                     ...h, poll: Reply({ ...replyOf(h.poll), old: true }) }))
                 else consumers
    all {
      alive(c), unacked.contains(e),
      reason != Stranded or not(renewedByAnother(e)),
      reason == HolderDead or reason == Stranded
        or (reason == HeldTooLong and used.heldTimeouts < CFG.max.heldTimeouts)
        or (reason == ReplyTooLong
            and used.replyTimeouts < CFG.max.replyTimeouts),
      unacked' = unacked.exclude(Set(e)),
      queue' = queue.union(Set(e.m)),
      wake' = (wake or scripted(CFG.design)),
      consumers' = marked,
      used' = if (reason == HeldTooLong)
                { ...used, heldTimeouts: used.heldTimeouts + 1 }
              else if (reason == ReplyTooLong)
                { ...used, replyTimeouts: used.replyTimeouts + 1 }
              else used,
      unpublished' = unpublished, clock' = clock, keepObs,
      did(Restore({ c: c, m: e.m, reason: reason }), 1),
    }
  }

  // A consumer that holds a message renews its tag. The record under that
  // tag is another consumer's, written after a restore: RenewBlind moves
  // its score all the same.
  action renewOthers(c: ConsumerId, e: Entry): bool = all {
    CFG.renewal == RenewBlind, alive(c), live(c).held.contains(e.m),
    unacked.contains(e), e.by != c, e.tag == OwnTag, not(e.moved),
    unacked' = unacked.exclude(Set(e)).union(Set({ ...e, moved: true })),
    queue' = queue, wake' = wake, consumers' = consumers,
    unpublished' = unpublished, clock' = clock, keepObs, used' = used,
    did(RenewOthers({ c: c, m: e.m }), 1),
  }

  // The consumer's credit goes while a reply is on its way, and comes back.
  action loseCredit(c: ConsumerId): bool = all {
    alive(c), isReply(live(c).poll), live(c).prefetch == PREFETCH,
    used.creditLosses < CFG.max.creditLosses,
    consumers' = consumers.set(c, Alive({ ...live(c), prefetch: 0 })),
    used' = { ...used, creditLosses: used.creditLosses + 1 },
    keepRedis, unpublished' = unpublished, clock' = clock, keepObs,
    did(LoseCredit(c), 0),
  }
  action regainCredit(c: ConsumerId): bool = all {
    alive(c), live(c).prefetch < PREFETCH,
    consumers' = consumers.set(c, Alive({ ...live(c), prefetch: PREFETCH })),
    keepRedis, unpublished' = unpublished, clock' = clock, keepObs,
    used' = used, did(RegainCredit(c), 0),
  }

  // The choices are made next to the actions that use them: the simulator
  // and TLC try every combination of the choices in scope for every action.
  action step: bool = any {
    {
      nondet c = CONSUMERS.oneOf()
      any {
        sendPop(c), popEmpty(c), waitAfterEmpty(c), wakeUp(c), waitTimeout(c),
        receive(c),
        crash(c), loseCredit(c), regainCredit(c),
      }
    },
    {
      nondet m = MSGS.oneOf()
      publish(m)
    },
    {
      nondet c = CONSUMERS.oneOf()
      nondet m = MSGS.oneOf()
      any {
        ack(c, m),
        {
          nondet mismatch = Set(true, false).oneOf()
          serverPop(c, m, mismatch)
        },
      }
    },
    {
      nondet c = CONSUMERS.oneOf()
      nondet e = unacked.oneOf()
      any { restore(c, e), renewOthers(c, e) }
    },
  }

  // ---- properties ----------------------------------------------------------
  def published: Set[MsgId] = MSGS.exclude(unpublished)
  // In how many places Redis or an acknowledgement accounts for a message.
  def places(m: MsgId): int =
    (if (queue.contains(m)) 1 else 0) + entriesOf(unacked, m).size()
    + (if (acked.contains(m)) 1 else 0)
  def inFlight(m: MsgId): bool = CONSUMERS.exists(c =>
    alive(c) and isReply(live(c).poll) and replyOf(live(c).poll).m == m)

  // Conservation, strict: at every moment a published message is on the
  // queue, in unacked, or acked. (kombu PR 2831's state machine test.)
  val inSomePlace: bool = published.forall(m => places(m) >= 1)
  // Conservation, weak: or a live consumer is about to read it. A message
  // that breaks this is lost for good.
  val neverLost: bool = published.forall(m => places(m) >= 1 or inFlight(m))
  // And never in two places, unless a consumer was killed or held a
  // message past the visibility timeout.
  val notTwice: bool =
    (used.crashes == 0 and used.heldTimeouts == 0)
      implies published.forall(m => places(m) <= 1)
  // No message is delivered twice without one of those two.
  val noDuplicate: bool =
    (used.crashes == 0 and used.heldTimeouts == 0)
      implies MSGS.forall(m => deliveries.get(m) <= 1)
  // A consumer never holds more messages than its prefetch count allows.
  val prefetchRespected: bool = CONSUMERS.forall(c =>
    alive(c) implies live(c).held.size() <= live(c).prefetch)
  // A put-back that did nothing found its record gone: no record is left
  // with a consumer that dropped the message.
  val noStranded: bool = unacked.forall(e => not(stranded(e)))
  // PR: a message does not sit on the queue while a consumer with credit
  // waits and no token or pop is on the way, unless a consumer was killed
  // (a token it took dies with it; the wait's timeout covers that).
  val noMissedWake: bool =
    (scripted(CFG.design) and used.crashes == 0 and queue != Set()
     and CONSUMERS.exists(c => alive(c) and live(c).poll == Waiting
                               and credit(c)))
      implies (wake or CONSUMERS.exists(c => alive(c)
                                             and isPopping(live(c).poll)))

  // ---- liveness, in layers (see chord.qnt) --------------------------------
  // Quiet: no action other than a fault can run.
  def restorable: bool = unacked.exists(e =>
    not(alive(e.by)) or (stranded(e) and not(renewedByAnother(e))))
  def busy(c: ConsumerId): bool = {
    val s = live(c)
    alive(c) and (
      s.held != Set() or s.prefetch < PREFETCH
      or (s.poll == Idle and credit(c))
      or (isPopping(s.poll) and (queue != Set() or scripted(CFG.design)))
      or (s.poll == Waiting and credit(c) and (wake or queue != Set()))
      or (isReply(s.poll) and (credit(c) or CFG.design == PopScript))
      or restorable
      or unacked.exists(e => CFG.renewal == RenewBlind and e.by != c
           and e.tag == OwnTag and not(e.moved) and s.held.contains(e.m)))
  }
  val quiet: bool = unpublished == Set() and CONSUMERS.forall(c => not(busy(c)))
  // A quiet system has acked every message and left no record behind.
  val quietDone: bool =
    quiet implies (MSGS.forall(m => acked.contains(m)) and unacked == Set())
  // Exactly once, when no fault was used: never delivered twice, and when
  // quiet, delivered once and acked.
  val exactlyOnce: bool = used == NO_FAULTS implies (
    MSGS.forall(m => deliveries.get(m) <= 1)
    and (quiet implies MSGS.forall(m => deliveries.get(m) == 1
                                        and acked.contains(m))))
  // Witnesses (violated means reachable).
  val neverQuiet: bool = not(quiet)
  val neverDuplicated: bool = MSGS.forall(m => deliveries.get(m) <= 1)
  val neverPutBack: bool = CONSUMERS.forall(c => MSGS.forall(m =>
    lastAction != Receive({ c: c, m: m, outcome: PutBack })))

  action stutter: bool = all {
    quiet,
    keepRedis, unpublished' = unpublished, consumers' = consumers,
    clock' = clock, keepObs, ops' = ops, used' = used,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
}
