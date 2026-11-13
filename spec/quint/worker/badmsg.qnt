// -*- mode: Bluespec; -*-
// Messages a worker cannot handle: a task name it does not know, a body
// it cannot decode, a content type it does not accept, an ETA it cannot
// convert. Is the message settled, does the worker stay up, is a state
// stored, and does the failure reach the errback, the next step of a
// chain and the chord the task belongs to?
//
// Celery 768e476 (the same at 9c2f238). Consumer.create_task_handler
// (consumer.py:798-860): a name with no strategy goes to on_unknown_task,
// which rejects the message and stores FAILURE (NotRegistered) with a
// request of its own: chord None, errbacks None, no chain
// (consumer.py:752-783). The default strategy reads message.payload,
// which decodes the body in the main process (strategy.py:132-142): a
// DecodeError goes to on_decode_error, which acks (consumer.py:565-579);
// a content type outside accept_content raises ContentDisallowed, and
// on_invalid_task rejects (consumer.py:785-789). Neither stores a state.
// An ETA that cannot be converted stores FAILURE with the request's own
// context and rejects (strategy.py:181-195). A task that raises is the
// control: handle_failure stores FAILURE with the request
// (trace.py:221-251), and mark_as_failure fails the chord part, the rest
// of the chain and calls the errbacks (base.py:298-317).
//
// One message of a kind, in a context: alone, with a link_error errback,
// as the first step of a chain of two, or as one of two chord header
// tasks whose sibling succeeds. Redis broker: a reject without requeue
// removes the message as an ack does.
//
// Not modeled: protocol 1 messages, a backend error while the failure is
// stored (worker/redelivery.qnt and revoke.qnt have that class),
// Reject and Ignore from the body (ackmatrix.qnt).
module badmsgtypes {
  type Kind = Known | Unknown | Undecodable | Disallowed | BadEta
  type Context = Alone | WithErrback | ChainFirst | ChordMember

  type Msg = Queued | Settled
  type Stored = NoState | Failure
  type Next = NoNext | NextPending | NextFailed
  type Body = NoBody | BodyPending | BodyFailed

  type Action =
    | Init
    | Receive(Kind)
    | SiblingEnds
    | ChordJoin

  type Config = { kind: Kind, ctx: Context }
}

module badmsg {
  import badmsgtypes.*

  const CFG: Config

  // ---- state ----
  var msg: Msg
  var workerUp: bool
  var stored: Stored
  var errbacks: int
  var nextStep: Next
  var parts: int          // chord parts returned (header of two)
  var partFailed: bool    // a returned part failed
  var siblingDone: bool
  var body: Body
  var lastAction: Action
  var steps: int

  action init: bool = all {
    msg' = Queued, workerUp' = true, stored' = NoState, errbacks' = 0,
    nextStep' = if (CFG.ctx == ChainFirst) NextPending else NoNext,
    parts' = 0, partFailed' = false, siblingDone' = false,
    body' = if (CFG.ctx == ChordMember) BodyPending else NoBody,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // mark_as_failure with a request that carries the context: the chord
  // part, the rest of the chain and the errbacks (base.py:298-317).
  // @cite celery celery/backends/base.py:298-317 "self._call_task_errbacks(request, exc, traceback)"
  action failWithContext: bool = all {
    stored' = Failure,
    errbacks' = if (CFG.ctx == WithErrback) errbacks + 1 else errbacks,
    nextStep' = if (nextStep == NextPending) NextFailed else nextStep,
    parts' = if (CFG.ctx == ChordMember) parts + 1 else parts,
    partFailed' = (partFailed or CFG.ctx == ChordMember),
  }
  // mark_as_failure with on_unknown_task's Bunch: only the store.
  // @cite celery celery/worker/consumer/consumer.py:765-775 "name=name, chord=None, root_id=root_id,"
  action failWithoutContext: bool = all {
    stored' = Failure, errbacks' = errbacks, nextStep' = nextStep, parts' = parts,
    partFailed' = partFailed,
  }
  // on_decode_error and on_invalid_task store nothing.
  // @cite celery celery/worker/consumer/consumer.py:565-579 "def on_decode_error(self, message, exc):"
  action storeNothing: bool = all {
    stored' = stored, errbacks' = errbacks, nextStep' = nextStep, parts' = parts,
    partFailed' = partFailed,
  }

  // ---- actions ----
  // The consumer receives the message and settles it by its kind.
  // @cite celery celery/worker/consumer/consumer.py:798-860 "return on_unknown_task(None, message, exc)"
  // @cite celery celery/worker/consumer/consumer.py:752-783 "errbacks=None,"
  // @cite celery celery/worker/consumer/consumer.py:565-579 "message.ack()"
  // @cite celery celery/worker/consumer/consumer.py:785-789 "message.reject_log_error(logger, self.connection_errors)"
  // @cite celery celery/worker/strategy.py:132-142 "if body is None and 'args' not in message.payload:"
  // @cite celery celery/worker/strategy.py:181-195 "req.reject(requeue=False)"
  // @cite celery celery/app/trace.py:221-251 "task.backend.mark_as_failure("
  // @cite celery celery/backends/base.py:298-317 "self._fail_chain(getattr(request, 'chain', None), exc, traceback,"
  action receive: bool = all {
    msg == Queued, workerUp,
    msg' = Settled, workerUp' = true,
    match CFG.kind {
      | Known => failWithContext
      | BadEta => failWithContext
      | Unknown => failWithoutContext
      | Undecodable => storeNothing
      | Disallowed => storeNothing
    },
    body' = body, siblingDone' = siblingDone,
    did(Receive(CFG.kind)),
  }
  // The chord's other header task succeeds and returns its part.
  // @cite celery celery/backends/base.py:298-317 "self.on_chord_part_return(request, state, exc)"
  action siblingEnds: bool = all {
    CFG.ctx == ChordMember, not(siblingDone),
    parts' = parts + 1, siblingDone' = true,
    msg' = msg, workerUp' = workerUp, stored' = stored, errbacks' = errbacks,
    nextStep' = nextStep, partFailed' = partFailed, body' = body,
    did(SiblingEnds),
  }
  // When both parts returned the chord completes: the body runs, or fails
  // when a part failed. Only the failed outcome matters here.
  // @cite celery celery/backends/base.py:1016-1020 "def on_chord_part_return(self, request, state, result, **kwargs):"
  action chordJoin: bool = all {
    body == BodyPending, parts == 2,
    body' = if (partFailed) BodyFailed else NoBody,
    msg' = msg, workerUp' = workerUp, stored' = stored, errbacks' = errbacks,
    nextStep' = nextStep, parts' = parts, partFailed' = partFailed,
    siblingDone' = siblingDone,
    did(ChordJoin),
  }

  // ---- step ----
  action step: bool = any { receive, siblingEnds, chordJoin }

  // ---- properties ----------------------------------------------------------
  // invariants
  // The worker stays up.
  val workerSurvives: bool = workerUp
  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state (one receive,
  // one sibling, one join).
  val quiet: bool = msg == Settled and not(siblingEnabled) and not(joinEnabled)
  def siblingEnabled: bool = CFG.ctx == ChordMember and not(siblingDone)
  def joinEnabled: bool = body == BodyPending and parts == 2
  action stutter: bool = all {
    quiet, msg' = msg, workerUp' = workerUp, stored' = stored,
    errbacks' = errbacks, nextStep' = nextStep, parts' = parts,
    partFailed' = partFailed, siblingDone' = siblingDone, body' = body,
    lastAction' = lastAction,
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
  // When the message is settled a final state is stored for its id.
  val outcomeVisible: bool = quiet implies stored != NoState
  // A failure stored for the task reaches what depends on it: the
  // errback ran, the chain's next step is failed, the chord's body is
  // failed.
  val failurePropagates: bool = (quiet and stored == Failure) implies all {
    CFG.ctx != WithErrback or errbacks == 1,
    nextStep != NextPending,
    body != BodyPending,
  }
  // At the end the message is settled and nothing waits for it.
  val quietDone: bool = quiet implies (msg == Settled and nextStep != NextPending
                                        and body != BodyPending)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverFailed: bool = stored != Failure
}
