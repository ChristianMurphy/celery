// -*- mode: Bluespec; -*-
// The ack settings matrix: for each way a run ends and each combination
// of acks_late, acks_on_failure and reject_on_worker_lost, is the message
// acked, rejected or requeued, what is stored, does it run again, and on
// which broker does a rejected message reach a dead letter exchange?
//
// Celery 768e476 (the same at 9c2f238). Early ack: on_accepted acks when
// the child accepts the job (request.py:524-533). The end of the run:
// Request.on_success acks a late-ack message (request.py:1026-1040);
// Request.on_failure (request.py:642-830): Reject(requeue=False) stores
// FAILURE and rejects, Reject(requeue=True) rejects with requeue and
// stores nothing, Ignore acks, Retry acks (on_retry), a lost process
// requeues under reject_on_worker_lost and otherwise stores FAILURE; a
// failure acks under acks_on_failure and otherwise rejects without
// requeue. ack and reject are no-ops once the message was acked
// (request.py:901-911). A raise stores FAILURE in the child
// (trace.py:221-251), a retry stores RETRY and publishes a new message.
//
// Brokers: Redis (a reject without requeue removes the message, as an
// ack does; kombu redis.py has no dead lettering) and RabbitMQ with a
// dead letter exchange on the queue (a reject without requeue goes there).
// A requeued message runs again and returns; a retry's new message runs
// and returns.
//
// Not modeled: hard time limits and acks_on_timeout (handlers.qnt),
// retry_on_worker_lost, quorum queue delivery limits (redelivery.qnt),
// a lost connection.
module ackmatrixtypes {
  type Ending = Returns | Raises | Retries | RejectRequeue | RejectDrop
              | Ignores | Lost
  type Broker = Redis | RabbitDlx

  type Msg = Queued | Delivered | Gone | DeadLettered
  type Stored = NoState | Success | Failure | RetryState

  type Action =
    | Init
    | Deliver
    | Accept({ acked: bool })
    | End(Ending)
    | Settle({ ack: bool, requeue: bool, store: bool })

  type Config = {
    ending: Ending,
    acksLate: bool,
    acksOnFailure: bool,
    rejectOnLost: bool,
    broker: Broker,
  }
  pure def config(e: Ending, late: bool): Config =
    { ending: e, acksLate: late, acksOnFailure: true, rejectOnLost: false,
      broker: Redis }
}

module ackmatrix {
  import ackmatrixtypes.*

  const CFG: Config

  // ---- state ----
  var msg: Msg
  var acked: bool          // Request.acknowledged for the current delivery
  var running: bool
  var ended: bool          // the run ended; the main process settles next
  var ending: Ending       // how the current run ended
  var runs: int
  var stored: Stored
  var extra: bool          // a retry's new message waits
  var requeueAsked: bool   // a run asked for its message to be requeued
  var lastAction: Action
  var steps: int

  action init: bool = all {
    msg' = Queued, acked' = false, running' = false, ended' = false,
    ending' = Returns, runs' = 0, stored' = NoState, extra' = false,
    requeueAsked' = false, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepRun: bool = all {
    ending' = ending, runs' = runs, stored' = stored, extra' = extra,
    requeueAsked' = requeueAsked,
  }

  // ---- actions ----
  // The worker gets the message (or the retry's new one) and hands it to
  // the pool.
  // @cite celery celery/worker/request.py:1000-1020 "accept_callback=self.on_accepted,"
  action deliver: bool = all {
    not(running), not(ended), runs < 2, msg == Queued or extra,
    msg' = if (msg == Queued) Delivered else msg,
    acked' = false, running' = true, ended' = false,
    ending' = ending, runs' = runs + 1, stored' = stored, extra' = false,
    requeueAsked' = requeueAsked,
    did(Deliver),
  }
  // The child accepts the job: early ack.
  // @cite celery celery/worker/request.py:524-533 "if not self.task.acks_late:"
  action accept: bool = all {
    running, not(ended), not(acked), not(CFG.acksLate),
    acked' = true, msg' = if (msg == Delivered) Gone else msg,
    running' = running, ended' = ended, keepRun,
    did(Accept({ acked: true })),
  }
  // The run ends. A raise stores FAILURE and a retry RETRY (and publishes
  // a new message) in the child; a second run returns.
  // @cite celery celery/app/trace.py:221-251 "task.backend.mark_as_failure("
  // @cite celery celery/app/trace.py:576-582 "except Retry as exc:"
  action end: bool = all {
    running, not(ended), CFG.acksLate or acked,
    val e = if (runs == 1) CFG.ending else Returns
    all {
      ended' = true, running' = false, ending' = e,
      stored' = match e {
        | Returns => Success | Raises => Failure | Retries => RetryState
        | RejectRequeue => stored | RejectDrop => stored | Ignores => stored
        | Lost => stored
      },
      extra' = e == Retries,
      msg' = msg, acked' = acked, runs' = runs, requeueAsked' = requeueAsked,
      did(End(e)),
    }
  }
  // The main process settles the message (on_success, on_failure,
  // on_retry), guarded by acknowledged.
  // @cite celery celery/worker/request.py:681-707 "return self.reject(requeue=exc.requeue)"
  // @cite celery celery/worker/request.py:745-776 "self.reject(requeue=False)"
  // @cite celery celery/worker/request.py:901-911 "if not self.acknowledged:"
  // @cite celery celery/worker/request.py:616-631 "if self.task.acks_late:"
  action settle: bool = all {
    ended,
    val e = ending
    val failure = e == Raises or (e == Lost and not(CFG.acksLate and CFG.rejectOnLost))
    // what Request asks for
    val requeue = e == RejectRequeue
                  or (e == Lost and CFG.acksLate and CFG.rejectOnLost)
    val drop = e == RejectDrop
               or (failure and CFG.acksLate and not(CFG.acksOnFailure))
    // what reaches the broker: nothing once the message was acked
    val effective = not(acked) and msg == Delivered
    val store = e == RejectDrop or (e == Lost and not(requeue))
    all {
      ended' = false, running' = false, acked' = true,
      msg' = if (not(effective)) msg
             else if (requeue) Queued
             else if (drop and CFG.broker == RabbitDlx) DeadLettered
             else Gone,
      stored' = if (store) Failure else stored,
      requeueAsked' = (requeueAsked or requeue),
      ending' = ending, runs' = runs, extra' = extra,
      did(Settle({ ack: not(requeue or drop), requeue: requeue, store: store })),
    }
  }

  // ---- step ----
  action step: bool = any { deliver, accept, end, settle }

  // ---- properties ----------------------------------------------------------
  def settledMsg: bool = msg == Gone or msg == DeadLettered
  val quiet: bool = not(running) and not(ended) and not(extra)
    and (settledMsg or (msg == Queued and runs >= 2))
  // invariants
  // At the end a final state (SUCCESS or FAILURE) is stored.
  val outcomeVisible: bool = quiet implies (stored == Success or stored == Failure)
  // A run that asked for its message to be requeued runs again.
  val requeueRuns: bool = quiet implies (requeueAsked implies runs >= 2)
  // A message rejected without requeue reaches the dead letter exchange.
  val rejectDeadLetters: bool =
    quiet implies (CFG.ending != RejectDrop or msg == DeadLettered)
  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state (at most two
  // runs).
  action stutter: bool = all {
    quiet, msg' = msg, acked' = acked, running' = running, ended' = ended,
    keepRun, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run that keeps going ends in
  // a quiet state, for good, and that state has ended. The spec has no
  // fault budgets (its events are part of the scenario), so the fairness
  // is on every action. Every action raises "steps", so that names the
  // steps that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(step, steps) implies eventually(always(quiet and quietDone))
  // At the end the message is settled and no new message waits.
  val quietDone: bool = quiet implies settledMsg

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRanTwice: bool = runs < 2
}
