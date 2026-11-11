// -*- mode: Bluespec; -*-
// The delivery properties, defined once over an observation record.
//
// Quint has no interfaces and cannot pass state or a module as a
// parameter, so each spec builds one `MsgObs` per message from its own
// state (its `obs` definition) and states its property as one of these
// functions. What each field means in a spec is in that spec's `obs`.
//
//   published  the message was handed to the broker (its publish returned)
//   runs       how many times it was delivered or started: what must not
//              happen twice
//   done       a run of it has finished, or it was acked or written, so
//              that losing the broker's copy loses nothing
//   held       the broker or someone who will finish it still has it
//   settled    the end state: done once, and nothing of it is left behind
module observe {
  type MsgObs = {
    published: bool, runs: int, done: bool, held: bool, settled: bool,
  }

  // Exactly once while no fault was used: no message runs twice, and a
  // quiet system has run each one once and settled it.
  pure def exactlyOnceOf(faultFree: bool, quiet: bool, obs: Set[MsgObs]): bool =
    faultFree implies obs.forall(o =>
      o.runs <= 1 and (quiet implies (o.runs == 1 and o.settled)))

  // At least once, as a safety property: a published message is held
  // somewhere until it is done.
  pure def neverLostOf(obs: Set[MsgObs]): bool =
    obs.forall(o => o.published implies (o.done or o.held))

  // And as an outcome (lemma 3 of the liveness argument): a quiet system
  // has settled every message.
  pure def quietDoneOf(quiet: bool, obs: Set[MsgObs]): bool =
    quiet implies obs.forall(o => o.settled)

  // No message runs twice (a witness where it is reachable).
  pure def neverTwiceOf(obs: Set[MsgObs]): bool = obs.forall(o => o.runs <= 1)
}
