// -*- mode: Bluespec; -*-
// A chord whose header holds a task, a nested chord or a task that
// replaces itself, with an errback given by on_error() or as the
// link_error option of apply_async. Does the chord end (its body runs or
// is stored FAILURE), and is the errback called once when a task fails?
//
// Celery 9c2f238 (main, after celery PR 10778), Redis result backend.
// outer = chord([X, Y], body). X is one of
//
// * Flat: a task (leaf 1).
// * Nested: chord([leaf 1, leaf 2], inner body). The inner body is a
//   member of the outer chord: when it runs, its result is the outer
//   chord's part X. When an inner header task fails, the Redis backend
//   fails the inner body through chord_error_from_stack (redis.py:1052-
//   1056), which calls the inner body's errbacks and stores FAILURE for
//   it (base.py:464-503) but never returns a part to the outer chord.
// * Replaced: a task that calls self.replace(leaf 1): the replacement
//   takes the task's id and its chord (task.py:1171-1180); the replaced
//   task raises Ignore and returns no part (task.py:1323-1327).
//
// Y is leaf 3. With the default task_allow_error_cb_on_chord_header
// (False) a header task has no errback of its own. The errback:
//
// * OnError: chord.link_error() links it to the body and, for a chord
//   nested in the header, to that chord too (canvas.py:2744-2751): the
//   inner body gets it.
// * AsOption: apply_async(link_error=eb) gives it to the outer body only
//   (canvas.py:2583-2600, celery PR 10778); on 5.5.3 the call raised
//   TypeError.
//
// The parts arrive in any order; a part return and the completion it
// triggers are one step (the Redis Lua scripts make them atomic; chord.qnt
// has the races).
//
// Not modeled: duplicates and lost workers (chord.qnt), a group or chain
// in the header (documented: canvas.rst:736-772 for group callbacks),
// task_allow_error_cb_on_chord_header True, a body that fails.
module nesttypes {
  type Shape = Flat | Nested | Replaced
  type Attach = OnError | AsOption
  // A result as the backend stores it; Due: every part is in and the
  // body message is sent, not run yet.
  type St = Pending | Due | Ok | Failed
  type Action =
    | Init
    | Replace
    | RunLeaf({ leaf: int, ok: bool })
    | RunInnerBody
    | RunBody

  type Config = { shape: Shape, attach: Attach, fails: Set[int] }
  pure def config(s: Shape, a: Attach, f: Set[int]): Config =
    { shape: s, attach: a, fails: f }
  pure def done(s: St): bool = match s {
    | Pending => false | Due => false | Ok => true | Failed => true
  }
}

module nested {
  import nesttypes.*

  const CFG: Config

  // The leaves in play: leaf 2 only in a nested chord's header.
  pure val LEAVES: Set[int] =
    if (CFG.shape == Nested) Set(1, 2, 3) else Set(1, 3)

  // ---- state ----
  var leaf: int -> St
  var replaced: bool        // the replacing task ran (Replaced only)
  var innerBody: St         // Nested only
  var parts: int -> St      // the outer chord's parts: 1 is X, 3 is Y
  var body: St              // the outer body
  var errbacks: int         // calls of the errback
  var lastAction: Action
  var steps: int

  action init: bool = all {
    leaf' = LEAVES.mapBy(_ => Pending), replaced' = false,
    innerBody' = Pending, parts' = Set(1, 3).mapBy(_ => Pending),
    body' = Pending, errbacks' = 0, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // The outer chord gets part p; with every part in, the body is sent,
  // or, if a part failed, the body is stored FAILURE and its errbacks run.
  def outerAfter(p: int, s: St): int -> St = parts.set(p, s)
  def allIn(ps: int -> St): bool = Set(1, 3).forall(q => done(ps.get(q)))
  def anyFailed(ps: int -> St): bool = Set(1, 3).exists(q => ps.get(q) == Failed)
  // The outer body's errbacks: the errback, whichever way it was given.
  def bodyAfter(ps: int -> St): St =
    if (not(allIn(ps))) body else if (anyFailed(ps)) Failed else Due
  def ebAfter(ps: int -> St): int =
    if (allIn(ps) and anyFailed(ps)) errbacks + 1 else errbacks

  // ---- actions ----
  // A task that replaces itself: the replacement keeps its id and chord;
  // the task itself raises Ignore and returns no part.
  // @cite celery@9c2f238 celery/app/task.py:1171-1180 "sig.freeze(self.request.id)"
  // @cite celery@9c2f238 celery/app/task.py:1323-1327 "raise Ignore('Replaced by new task')"
  action replace: bool = all {
    CFG.shape == Replaced, not(replaced), replaced' = true,
    leaf' = leaf, innerBody' = innerBody, parts' = parts, body' = body,
    errbacks' = errbacks, did(Replace),
  }

  // A header task runs and returns or raises; its part goes to its chord.
  // A failed inner header fails the inner body and calls its errbacks; no
  // part reaches the outer chord.
  // @cite celery@9c2f238 celery/backends/redis.py:1052-1056 "return self.chord_error_from_stack(callback, exc)"
  // @cite celery@9c2f238 celery/backends/base.py:464-503 "def chord_error_from_stack(self, callback, exc=None):"
  action runLeaf(l: int): bool = all {
    leaf.get(l) == Pending,
    not(CFG.shape == Replaced and l == 1 and not(replaced)),
    val ok = not(CFG.fails.contains(l))
    val s = if (ok) Ok else Failed
    val leaf2 = leaf.set(l, s)
    if (CFG.shape == Nested and l != 3) {
      val innerIn = leaf2.get(1) != Pending and leaf2.get(2) != Pending
      val innerFail = leaf2.get(1) == Failed or leaf2.get(2) == Failed
      all {
        leaf' = leaf2, replaced' = replaced,
        innerBody' = if (not(innerIn)) innerBody
                     else if (innerFail) Failed else Due,
        errbacks' = if (innerIn and innerFail and CFG.attach == OnError)
                      errbacks + 1 else errbacks,
        parts' = parts, body' = body,
        did(RunLeaf({ leaf: l, ok: ok })),
      }
    } else {
      val p = if (l == 3) 3 else 1
      val ps = outerAfter(p, s)
      all {
        leaf' = leaf2, replaced' = replaced, innerBody' = innerBody,
        parts' = ps, body' = bodyAfter(ps), errbacks' = ebAfter(ps),
        did(RunLeaf({ leaf: l, ok: ok })),
      }
    }
  }

  // The inner body runs; it is the outer chord's part X.
  // @cite celery@9c2f238 celery/canvas.py:2744-2751 "# Edge case for nested chords in the header"
  action runInnerBody: bool = all {
    innerBody == Due, innerBody' = Ok,
    val ps = outerAfter(1, Ok)
    all {
      parts' = ps, body' = bodyAfter(ps), errbacks' = ebAfter(ps),
      leaf' = leaf, replaced' = replaced, did(RunInnerBody),
    }
  }

  // The outer body runs. An errback passed as apply_async's link_error
  // is on the outer body only.
  // @cite celery@9c2f238 celery/canvas.py:2583-2600 "errbacks = maybe_list(options.pop('link_error', None)) or []"
  action runBody: bool = all {
    body == Due, body' = Ok,
    leaf' = leaf, replaced' = replaced, innerBody' = innerBody,
    parts' = parts, errbacks' = errbacks, did(RunBody),
  }

  // ---- step ----
  action step: bool = any {
    replace,
    nondet l = LEAVES.oneOf()
    runLeaf(l),
    runInnerBody, runBody,
  }

  // ---- properties ----------------------------------------------------------
  def someFailed: bool = LEAVES.exists(l => leaf.get(l) == Failed)
  // invariants
  // The body runs only if no header task failed.
  val bodyOnlyOnSuccess: bool = body == Ok implies not(someFailed)
  // The errback runs at most once (canvas.rst: "errbacks just once").
  val errbackOnce: bool = errbacks <= 1

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = LEAVES.forall(l => done(leaf.get(l)))
    and innerBody != Due and body != Due
    and not(CFG.shape == Replaced and not(replaced))
  action stutter: bool = all {
    quiet, leaf' = leaf, replaced' = replaced, innerBody' = innerBody,
    parts' = parts, body' = body, errbacks' = errbacks,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }

  // Liveness in one formula (../README.md): a run that keeps going ends in
  // a quiet state, for good, and that state has ended. The spec has no
  // fault budgets (its events are part of the scenario), so the fairness
  // is on every action. Every action raises "steps", so that names the
  // steps that count. Checked by TLC with stepOrQuiet as the step.
  temporal settles: bool =
    weakFair(step, steps) implies eventually(always(quiet and quietDone))
  // At the end the outer body ran or is stored FAILURE.
  val quietDone: bool = quiet implies done(body)
  // At the end, if a header task failed, the errback was called.
  val errbackOnFailure: bool = (quiet and someFailed) implies errbacks >= 1

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverErrback: bool = errbacks == 0
}
