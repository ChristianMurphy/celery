// -*- mode: Bluespec; -*-
// Remote control with reply=True: a client sends two broadcasts, one
// after the other, to one worker, and collects the replies. Does each call
// return the reply the worker gave it while it was still waiting?
//
// Celery 9c2f238 (app/control.py), kombu 445027e (pidbox.py).
//
// * control.broadcast binds a copy of the Mailbox to a connection for each
//   call (kombu pidbox.py:217-220). The copy's reply queue is named after
//   its oid, a uuid3 of the node, the process, the thread and id() of the
//   copy (pidbox.py:250-262, common.py:42-58): copies freed between calls
//   reuse addresses, so two calls can use the same reply queue name.
// * _publish declares the reply queue and sends the request with a fresh
//   ticket (pidbox.py:308-329). _collect consumes the reply queue: a reply
//   with another ticket is put in `unclaimed`, one with this ticket is
//   returned; the loop runs `limit` drains (with a destination, limit is
//   the number of destinations, pidbox.py:351-353), each waits up to
//   `timeout` (pidbox.py:370-411). A drain that takes another call's reply
//   uses one of the `limit` iterations.
// * After a call the reply queue is deleted (virtual transports,
//   virtual/base.py:561-562) or auto-deleted (AMQP, the last consumer is
//   cancelled): a reply published while no queue of that name exists is
//   dropped. A reply that arrives after its call returned is late; it
//   lands in the next call's queue if that call uses the same name and
//   has declared it.
//
// Design `ticketedLimit`: only replies with this call's ticket count
// toward `limit`.
//
// Not modeled: several workers (a reply per destination), callbacks, the
// clock header, two workers with the same node name (both answer; a
// DuplicateNodenameWarning is documented).
module pidboxtypes {
  // What a call returned.
  type Outcome = Pending | Own | NoReply
  type Action =
    | Init
    | Publish({ c: int })
    | Answer({ c: int })
    | Drain({ c: int })
    | Timeout({ c: int })

  type Config = {
    sameQueue: bool,      // the two calls use the same reply queue name
    limitOne: bool,       // destination=[worker]: limit 1
    ticketedLimit: bool,  // design
  }
}

module pidbox {
  import pidboxtypes.*

  const CFG: Config

  pure val CALLS: Set[int] = Set(1, 2)
  pure def qname(c: int): int = if (CFG.sameQueue) 1 else c

  // ---- state ----
  var calling: int               // the call being collected; 0: none yet, 3: done
  var published: Set[int]        // requests sent
  var answered: Set[int]         // requests the worker answered (in order)
  var open: Set[int]             // reply queue names that exist
  var inbox: int -> List[int]    // reply queue name -> tickets in it
  var drains: int                // drains of the current call that count
  var outcome: int -> Outcome
  var answeredWhileWaiting: int -> bool  // the reply was sent while its call waited
  var stale: int                 // replies of another call drained
  var lastAction: Action
  var steps: int

  action init: bool = all {
    calling' = 0, published' = Set(), answered' = Set(), open' = Set(),
    inbox' = Set(1, 2).mapBy(_ => List()), drains' = 0,
    outcome' = CALLS.mapBy(_ => Pending),
    answeredWhileWaiting' = CALLS.mapBy(_ => false), stale' = 0,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // ---- actions ----
  // The client starts call c (call 2 after call 1 returned): it declares
  // the reply queue and sends the request with a fresh ticket.
  // @cite kombu kombu/pidbox.py:308-329 "maybe_declare(self.reply_queue(chan))"
  // @cite kombu kombu/pidbox.py:217-220 "bound = copy(self)"
  action publish(c: int): bool = all {
    calling == c - 1, not(c.in(published)),
    c == 1 or outcome.get(1) != Pending,
    calling' = c, published' = published.union(Set(c)),
    open' = open.union(Set(qname(c))), drains' = 0,
    answered' = answered, inbox' = inbox, outcome' = outcome,
    answeredWhileWaiting' = answeredWhileWaiting, stale' = stale,
    did(Publish({ c: c })),
  }
  // The worker answers request c (in order): the reply goes to c's reply
  // queue if a queue of that name exists, and is dropped otherwise.
  // @cite kombu kombu/pidbox.py:104-123 "self.reply({self.hostname: reply},"
  // @cite kombu kombu/pidbox.py:288-304 "except InconsistencyError:"
  action answer(c: int): bool = all {
    c.in(published), not(c.in(answered)),
    CALLS.forall(d => d < c implies d.in(answered)),
    answered' = answered.union(Set(c)),
    inbox' = if (qname(c).in(open))
               inbox.set(qname(c), inbox.get(qname(c)).append(c))
             else inbox,
    answeredWhileWaiting' = answeredWhileWaiting.set(c,
      calling == c and outcome.get(c) == Pending),
    calling' = calling, published' = published, open' = open,
    drains' = drains, outcome' = outcome, stale' = stale,
    did(Answer({ c: c })),
  }
  // The collector drains one reply: its own ends the call; another call's
  // goes to `unclaimed`. A drain counts toward limit (with ticketedLimit
  // only its own does); at the limit the call returns.
  // @cite kombu kombu/pidbox.py:387-411 "unclaimed[this_id].append(body)"
  action drain(c: int): bool = all {
    calling == c, outcome.get(c) == Pending,
    inbox.get(qname(c)).length() > 0,
    val h = inbox.get(qname(c)).head()
    val counted = if (CFG.ticketedLimit and h != c) drains else drains + 1
    val done = h == c or (CFG.limitOne and counted >= 1)
    all {
      inbox' = if (done) inbox.set(qname(c), List())
               else inbox.set(qname(c), inbox.get(qname(c)).tail()),
      open' = if (done) open.exclude(Set(qname(c))) else open,
      outcome' = if (h == c) outcome.set(c, Own)
                 else if (done) outcome.set(c, NoReply) else outcome,
      calling' = if (done and c == 2) 3 else calling,
      drains' = counted, stale' = if (h != c) stale + 1 else stale,
    },
    published' = published, answered' = answered,
    answeredWhileWaiting' = answeredWhileWaiting, did(Drain({ c: c })),
  }
  // No reply within `timeout`: the call returns without one, and its
  // reply queue is deleted (a later reply to it is late).
  // @cite kombu kombu/pidbox.py:405-411 "except socket.timeout:"
  // @cite kombu kombu/transport/virtual/base.py:561-562 "def after_reply_message_received(self, queue):"
  action timeout(c: int): bool = all {
    calling == c, outcome.get(c) == Pending,
    inbox.get(qname(c)).length() == 0,
    outcome' = outcome.set(c, NoReply),
    open' = open.exclude(Set(qname(c))),
    calling' = if (c == 2) 3 else calling,
    inbox' = inbox, published' = published, answered' = answered,
    drains' = drains, answeredWhileWaiting' = answeredWhileWaiting,
    stale' = stale, did(Timeout({ c: c })),
  }

  // ---- step ----
  action step: bool = {
    nondet c = CALLS.oneOf()
    any { publish(c), answer(c), drain(c), timeout(c) }
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // A call whose worker answered while it waited returns that reply.
  val noLostReply: bool = CALLS.forall(c =>
    (answeredWhileWaiting.get(c) and outcome.get(c) != Pending)
      implies outcome.get(c) == Own)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = calling == 3 and answered == CALLS
  action stutter: bool = all {
    quiet, calling' = calling, published' = published, answered' = answered,
    open' = open, inbox' = inbox, drains' = drains, outcome' = outcome,
    answeredWhileWaiting' = answeredWhileWaiting, stale' = stale,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end both calls returned.
  val quietDone: bool = quiet implies CALLS.forall(c => outcome.get(c) != Pending)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverStale: bool = stale == 0
}
