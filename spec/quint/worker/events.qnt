// -*- mode: Bluespec; -*-
// Task events against stored states: for each way a run ends, what a
// monitor built on events.State (Flower) shows, and what the result
// backend stores. Does the monitor end in the state the backend stores,
// does it stop showing a task as started once the run ended, and does a
// run send one final event?
//
// Celery 768e476 (the same at 9c2f238). One task, one prefork worker
// started with -E; the monitor applies every task event in order, as
// events.State does (later events of a run replace earlier ones; the
// task-rejected after a task-failed leaves FAILURE, as observed).
//
// Endings (handlers.qnt has the hooks for the same endings):
// * Returns: SUCCESS, task-succeeded (request.py:620-631).
// * Raises, Lost (a lost pool process): FAILURE, task-failed
//   (request.py:745-799).
// * HardLimit: FAILURE and two task-failed (finding hard-limit-twice:
//   the error callback and the timeout callback each send one).
// * RejectDrop: Reject(requeue=False): FAILURE (celery issue 4222's fix),
//   task-failed, then task-rejected (request.py:680-703, 905-911).
// * Ignores: raise Ignore: nothing stored, the message acked, no event
//   (request.py:704-705; tasks.rst:1489-1494 "no state will be recorded").
// * UpdateThenIgnore: update_state(SUCCESS) then raise Ignore, the
//   documented way to store a result by hand (tasks.rst:1510-1522).
// * Unknown: a task name the worker lacks: FAILURE and task-failed from
//   the consumer, no task-received (consumer.py:752-783).
//
// Not modeled: task-retried (a retry is a new run of the same id; the
// monitor shows it), task-revoked and expiry (checked on the real code in
// the probe: REVOKED both sides), worker heartbeats and "offline", the
// monitor's own event loss (events are not persistent; a monitor started
// late misses them).
module eventtypes {
  type Ending = Returns | Raises | Lost | HardLimit | RejectDrop | Ignores
              | UpdateThenIgnore | Unknown
  type Status = Success | Failure
  type Stored = Absent | St(Status)
  type Mon = Unseen | MReceived | MStarted | MSucceeded | MFailed
  type Phase = Waiting | Received | Running | Ended

  type Action =
    | Init
    | Receive
    | Start
    | End({ events: int })
    | SecondFailed

  type Config = { ending: Ending }
}

module events {
  import eventtypes.*

  const CFG: Config

  // ---- state ----
  var phase: Phase
  var stored: Stored
  var mon: Mon
  // final events (task-succeeded, task-failed) sent for the run
  var finals: int
  // a hard limit's second task-failed is still to come
  var secondDue: bool
  var lastAction: Action
  var steps: int

  action init: bool = all {
    phase' = Waiting, stored' = Absent, mon' = Unseen, finals' = 0,
    secondDue' = false, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // ---- actions ----
  // The worker receives the message: task-received; a task name it does
  // not know is failed at once, with task-failed only.
  // @cite celery celery/worker/strategy.py:166-172 "'task-received',"
  // @cite celery celery/worker/consumer/consumer.py:752-783 "def on_unknown_task(self, body, message, exc):"
  action receive: bool = all {
    phase == Waiting,
    if (CFG.ending == Unknown) all {
      phase' = Ended, stored' = St(Failure), mon' = MFailed, finals' = 1,
    } else all {
      phase' = Received, stored' = stored, mon' = MReceived, finals' = finals,
    },
    secondDue' = secondDue, did(Receive),
  }
  // A pool process accepts it: task-started.
  // @cite celery celery/worker/request.py:525-533 "self.send_event('task-started')"
  action start: bool = all {
    phase == Received, phase' = Running, mon' = MStarted,
    stored' = stored, finals' = finals, secondDue' = secondDue, did(Start),
  }
  // The run ends.
  // @cite celery celery/worker/request.py:620-631 "self.send_event('task-succeeded', result=retval, runtime=runtime)"
  // @cite celery celery/worker/request.py:680-705 "elif isinstance(exc, Ignore):"
  // @cite celery celery/worker/request.py:790-799 "'task-failed',"
  // @cite celery celery/worker/request.py:905-911 "self.send_event('task-rejected', requeue=requeue)"
  action endRun: bool = all {
    phase == Running, phase' = Ended,
    val r: (Stored, Mon, int) = match CFG.ending {
      | Returns => (St(Success), MSucceeded, 1)
      | Raises => (St(Failure), MFailed, 1)
      | Lost => (St(Failure), MFailed, 1)
      | HardLimit => (St(Failure), MFailed, 1)
      | RejectDrop => (St(Failure), MFailed, 1)
      | Ignores => (Absent, MStarted, 0)
      | UpdateThenIgnore => (St(Success), MStarted, 0)
      | Unknown => (St(Failure), MFailed, 1)
    }
    all {
      stored' = r._1, mon' = r._2, finals' = finals + r._3,
      secondDue' = CFG.ending == HardLimit, did(End({ events: r._3 })),
    }
  }
  // A hard limit: billiard's timeout callback fails the request again.
  // @cite celery celery/worker/request.py:539-614 "def on_timeout(self, soft, timeout):"
  action secondFailed: bool = all {
    secondDue, secondDue' = false, finals' = finals + 1, mon' = MFailed,
    phase' = phase, stored' = stored, did(SecondFailed),
  }

  // ---- step ----
  action step: bool = any { receive, start, endRun, secondFailed }

  // ---- properties ----------------------------------------------------------
  // invariants
  // A run sends at most one final event.
  val oneFinalEvent: bool = finals <= 1
  // Once the run ended, the monitor shows what the backend stores.
  val monitorAgrees: bool = (phase == Ended and not(secondDue)) implies match stored {
    | St(s) => (s == Success and mon == MSucceeded) or (s == Failure and mon == MFailed)
    | Absent => true
  }

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = phase == Ended and not(secondDue)
  action stutter: bool = all {
    quiet, phase' = phase, stored' = stored, mon' = mon, finals' = finals,
    secondDue' = secondDue, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end the monitor no longer shows the task as received or
  // started.
  val quietDone: bool = quiet implies (mon != MStarted and mon != MReceived)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverFailedEvent: bool = mon != MFailed
}
