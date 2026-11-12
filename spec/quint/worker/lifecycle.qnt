// -*- mode: Bluespec; -*-
// The worker's lifecycle around one running task: a connection loss with
// and without worker_cancel_long_running_tasks_on_connection_loss, an
// autoscale down, a pool restart, a child recycled by
// max_tasks_per_child. Does the task run once per delivery, never twice
// at the same time, and end with SUCCESS?
//
// Celery 768e476 (the same at 9c2f238), kombu 445027e. One acks_late task
// on a Redis broker, one prefork worker with two processes.
//
// * Connection loss: the worker closes the connection and kombu restores
//   every unacked message to its queue (redis.py:491-495); the run goes
//   on unless the setting is on, then it is terminated
//   (consumer.py:480-500). The reconnected worker receives the restored
//   message and runs it, on a free process.
// * Autoscale down: billiard shrinks idle processes only; with every
//   process busy the autoscaler logs and tries later
//   (autoscale.py:142-149).
// * pool_restart: the controller reloads the pool; a running task ends on
//   its process (control.py:596-602; probe: the run ended SUCCESS on the
//   same pid, 1 of 1).
// * max_tasks_per_child: the child exits after the task and a new one
//   takes its place (components.py:157; probe: four 1 s tasks each on a
//   new pid, all SUCCESS, 1 of 1).
//
// Not modeled: max_memory_per_child (the same recycle after the task),
// a run that ignores SIGTERM, RabbitMQ (redelivery.qnt has the channel
// close), the startup with an unreachable broker (it retries,
// broker_connection_retry_on_startup) or backend (dedup-read-error and
// revoke-backend-error cover a backend error during a run).
module lifetypes {
  type Action =
    | Init
    | Deliver
    | Finish
    | LoseConnection({ cancelled: int })
    | Shrink
    | PoolRestart
    | Recycle

  type Config = {
    cancelOnLoss: bool,
    losses: int,
    shrink: bool,
    poolRestart: bool,
    recycle: bool,
  }
  pure def config(cancel: bool, losses: int): Config =
    { cancelOnLoss: cancel, losses: losses, shrink: false,
      poolRestart: false, recycle: false }
}

module lifecycle {
  import lifetypes.*

  const CFG: Config

  // ---- state ----
  var queued: bool
  var unacked: bool
  var running: int
  var procs: int
  var runs: int
  var done: bool
  var lossesLeft: int
  var controlDone: bool
  var lastAction: Action
  var steps: int

  action init: bool = all {
    queued' = true, unacked' = false, running' = 0, procs' = 2, runs' = 0,
    done' = false, lossesLeft' = CFG.losses, controlDone' = false,
    lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepControl: bool = all {
    lossesLeft' = lossesLeft, controlDone' = controlDone, procs' = procs,
  }

  // ---- actions ----
  // The worker receives the message and a free process runs it.
  // @env the broker and the pool (worker.qnt has the delivery)
  action deliver: bool = all {
    queued, running < procs,
    queued' = false, unacked' = true, running' = running + 1, runs' = runs + 1,
    done' = done, keepControl, did(Deliver),
  }
  // A run ends: SUCCESS is stored and the delivery is acked.
  // @cite celery celery/worker/request.py:620-631 "self.send_event('task-succeeded', result=retval, runtime=runtime)"
  action finish: bool = all {
    running > 0, running' = running - 1, done' = true,
    unacked' = (unacked and running - 1 > 0), queued' = queued, runs' = runs,
    keepControl, did(Finish),
  }
  // The broker connection is lost: unacked messages go back to the queue;
  // with the setting on, the running tasks are terminated.
  // @cite celery celery/worker/consumer/consumer.py:480-500 "if self.app.conf.worker_cancel_long_running_tasks_on_connection_loss:"
  // @cite kombu kombu/transport/redis.py:491-495 "def restore_unacked(self, client=None):"
  action loseConnection: bool = all {
    lossesLeft > 0, lossesLeft' = lossesLeft - 1,
    queued' = (queued or unacked), unacked' = false,
    val c = if (CFG.cancelOnLoss) running else 0
    all { running' = running - c, did(LoseConnection({ cancelled: c })) },
    runs' = runs, done' = done, procs' = procs, controlDone' = controlDone,
  }
  // Autoscale down by one: only an idle process goes.
  // @cite celery celery/worker/autoscale.py:142-149 "Autoscaler won't scale down: all processes busy."
  action shrink: bool = all {
    CFG.shrink, not(controlDone), procs > 1, running < procs,
    procs' = procs - 1, controlDone' = true,
    queued' = queued, unacked' = unacked, running' = running, runs' = runs,
    done' = done, lossesLeft' = lossesLeft, did(Shrink),
  }
  // pool_restart: the pool is reloaded; a running task ends where it runs.
  // @cite celery celery/worker/control.py:596-602 "state.consumer.controller.reload(modules, reload, reloader=reloader)"
  action poolRestart: bool = all {
    CFG.poolRestart, not(controlDone), controlDone' = true,
    queued' = queued, unacked' = unacked, running' = running, runs' = runs,
    done' = done, lossesLeft' = lossesLeft, procs' = procs, did(PoolRestart),
  }
  // max_tasks_per_child: the child that ran the task is replaced.
  // @cite celery celery/worker/components.py:157-157 "maxtasksperchild=w.max_tasks_per_child,"
  action recycle: bool = all {
    CFG.recycle, not(controlDone), done, controlDone' = true,
    queued' = queued, unacked' = unacked, running' = running, runs' = runs,
    done' = done, lossesLeft' = lossesLeft, procs' = procs, did(Recycle),
  }

  // ---- step ----
  action step: bool = any {
    deliver, finish, loseConnection, shrink, poolRestart, recycle,
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // The task never runs twice at the same time.
  val noOverlap: bool = running <= 1
  // A run starts again only after a connection loss: at most one run per
  // loss, plus the first.
  val noRerunWithoutLoss: bool = runs <= 1 + (CFG.losses - lossesLeft)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = not(queued) and running == 0
  action stutter: bool = all {
    quiet, queued' = queued, unacked' = unacked, running' = running,
    procs' = procs, runs' = runs, done' = done, lossesLeft' = lossesLeft,
    controlDone' = controlDone, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end SUCCESS is stored and the message is acked.
  val quietDone: bool = quiet implies (done and not(unacked))

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverRerun: bool = runs <= 1
}
