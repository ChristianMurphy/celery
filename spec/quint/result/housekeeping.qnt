// -*- mode: Bluespec; -*-
// Results housekeeping: forget() while the task runs, ignore_result with
// store_errors_even_if_ignored and a retry, and a saved group that is
// forgotten or deleted and then restored. Does a forgotten result stay
// forgotten, does a finished task leave a state that says it finished
// (or nothing), and does a restored group become ready once its tasks
// ran?
//
// Celery 768e476 (the same at 9c2f238). One task id, at most one retry
// (Task.retry with countdown 0); one client that may forget the task, or
// save a group of that one task, then forget or delete the group, then
// restore it.
//
// * Task.retry stores RETRY when errors are stored: always, or with
//   ignore_result only under store_errors_even_if_ignored
//   (trace.py:166-205). SUCCESS is stored only when the result is not
//   ignored (trace.py:479-485, 606-608). FAILURE follows store_errors
//   (trace.py:221-242).
// * AsyncResult.forget deletes what is stored now (result.py:136-143,
//   backends/base.py:883-885); nothing records the id as forgotten, so a
//   write by the running task puts a state back.
// * GroupResult.forget forgets the tasks, not the saved group
//   (result.py:712-715); delete removes the saved group
//   (result.py:989-991, backends/base.py:1000-1002); restore reads it
//   (backends/base.py:990-994).
//
// Not modeled: track_started (the tracer does not store STARTED when the
// result is ignored, trace.py:480), result_expires and
// celery.backend_cleanup (Redis expires keys itself; the database backends
// need beat to run the cleanup, documented in configuration.rst),
// result_extended (the same writes with more fields), the chord counter.
module housetypes {
  type Status = Retry | Success | Failure
  type Stored = Absent | St(Status)
  type Ending = Returns | RetryThenReturns | Raises
  type Op = NoOp | ForgetTask | GroupForget | GroupDelete
  type Run = Queued(int) | Running(int) | Finished
  type Restored = NotYet | RestoredNone | RestoredGroup

  type Action =
    | Init
    | Start
    | Finish({ stored: bool })
    | Forget
    | SaveGroup
    | GroupOp
    | Restore

  type Config = {
    ending: Ending,
    ignoreResult: bool,
    storeErrors: bool,
    op: Op,
  }
  pure def config(e: Ending, o: Op): Config =
    { ending: e, ignoreResult: false, storeErrors: false, op: o }
}

module housekeeping {
  import housetypes.*

  const CFG: Config

  // ---- state ----
  var stored: Stored
  var job: Run
  var forgotten: bool
  var groupSaved: bool
  var groupOpDone: bool
  var restored: Restored
  var lastAction: Action
  var steps: int

  action init: bool = all {
    stored' = Absent, job' = Queued(0), forgotten' = false,
    groupSaved' = false, groupOpDone' = false, restored' = NotYet,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepClient: bool = all {
    forgotten' = forgotten, groupSaved' = groupSaved,
    groupOpDone' = groupOpDone, restored' = restored,
  }

  pure def isQueued(r: Run): bool = match r {
    | Queued(_) => true | Running(_) => false | Finished => false
  }
  pure def isRunning(r: Run): bool = match r {
    | Queued(_) => false | Running(_) => true | Finished => false
  }
  pure def attemptOf(r: Run): int = match r {
    | Queued(a) => a | Running(a) => a | Finished => 0
  }
  // Errors (RETRY, FAILURE) are stored unless the result is ignored
  // without store_errors_even_if_ignored; SUCCESS unless it is ignored.
  val storeErrors: bool = not(CFG.ignoreResult) or CFG.storeErrors
  val storeSuccess: bool = not(CFG.ignoreResult)

  // ---- actions ----
  // A worker takes the message.
  // @env the broker and the worker (worker.qnt has the delivery)
  action start: bool = all {
    isQueued(job), job' = Running(attemptOf(job)), stored' = stored,
    keepClient, did(Start),
  }
  // The run ends: a retry stores RETRY and sends the task again, a return
  // stores SUCCESS, a raise stores FAILURE, each when it is stored at all.
  // @cite celery celery/app/trace.py:166-182 "store_errors = task.store_errors_even_if_ignored"
  // @cite celery celery/app/trace.py:191-205 "task.backend.mark_as_retry("
  // @cite celery celery/app/trace.py:479-485 "publish_result = not eager and not ignore_result"
  action finish: bool = all {
    isRunning(job),
    val a = attemptOf(job)
    val retrying = CFG.ending == RetryThenReturns and a == 0
    val st = if (retrying) Retry
             else if (CFG.ending == Raises) Failure else Success
    val doStore = if (st == Success) storeSuccess else storeErrors
    all {
      stored' = if (doStore) St(st) else stored,
      job' = if (retrying) Queued(a + 1) else Finished,
      keepClient, did(Finish({ stored: doStore })),
    }
  }
  // The client forgets the task, at any time.
  // @cite celery celery/result.py:136-143 "self.backend.forget(self.id)"
  action forget: bool = all {
    CFG.op == ForgetTask, not(forgotten),
    stored' = Absent, forgotten' = true, job' = job,
    groupSaved' = groupSaved, groupOpDone' = groupOpDone,
    restored' = restored, did(Forget),
  }
  // The client saves a group of the one task once it finished.
  // @cite celery celery/result.py:979-987 "return (backend or self.app.backend).save_group(self.id, self)"
  action saveGroup: bool = all {
    CFG.op == GroupForget or CFG.op == GroupDelete,
    job == Finished, not(groupSaved),
    groupSaved' = true, stored' = stored, job' = job, forgotten' = forgotten,
    groupOpDone' = groupOpDone, restored' = restored, did(SaveGroup),
  }
  // GroupResult.forget forgets the tasks; delete removes the saved group.
  // @cite celery celery/result.py:712-715 "for result in self.results:"
  // @cite celery celery/result.py:989-991 "(backend or self.app.backend).delete_group(self.id)"
  action groupOp: bool = all {
    groupSaved, not(groupOpDone), groupOpDone' = true,
    if (CFG.op == GroupForget) all { stored' = Absent, groupSaved' = true }
    else all { stored' = stored, groupSaved' = false },
    job' = job, forgotten' = forgotten, restored' = restored, did(GroupOp),
  }
  // GroupResult.restore reads the saved group: None when there is none.
  // @cite celery celery/backends/base.py:990-994 "meta = self.get_group_meta(group_id, cache=cache)"
  action restore: bool = all {
    groupOpDone, restored == NotYet,
    restored' = if (groupSaved) RestoredGroup else RestoredNone,
    stored' = stored, job' = job, forgotten' = forgotten,
    groupSaved' = groupSaved, groupOpDone' = groupOpDone, did(Restore),
  }

  // ---- step ----
  action step: bool = any { start, finish, forget, saveGroup, groupOp, restore }

  // ---- properties ----------------------------------------------------------
  // invariants
  // Once the task finished, what is stored is a final state or nothing:
  // never RETRY, which says the task will run again.
  val finalNotUnready: bool = job == Finished implies stored != St(Retry)
  // A forgotten result stays forgotten.
  val forgetSticks: bool = forgotten implies stored == Absent
  // A group restored after its tasks finished is ready: every task has a
  // final state stored.
  val restoredReady: bool = (restored == RestoredGroup and job == Finished)
    implies (stored == St(Success) or stored == St(Failure))

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = job == Finished
    and (CFG.op == ForgetTask implies forgotten)
    and ((CFG.op == GroupForget or CFG.op == GroupDelete) implies restored != NotYet)
  action stutter: bool = all {
    quiet, stored' = stored, job' = job, keepClient,
    lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end a task whose result is kept reads its final state, unless
  // the client removed it.
  val quietDone: bool = quiet implies (
    (storeSuccess and CFG.op == NoOp) implies
      (stored == St(Success) or stored == St(Failure)))

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverStoredRetry: bool = stored != St(Retry)
}
