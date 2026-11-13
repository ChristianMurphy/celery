// -*- mode: Bluespec; -*-
// The signals that stop or restart a worker while a task runs: SIGTERM
// (once or twice), SIGINT twice, SIGQUIT, SIGTERM then SIGQUIT, SIGHUP.
// Is worker_shutdown dispatched at every shutdown, after the running task
// stored its result, is no task lost, and does a SIGHUP restart consume
// again?
//
// Celery 9c2f238, kombu 445027e. One acks_late task A runs in a prefork
// worker on Redis; a second task B waits in the queue. Signal handlers
// (apps/worker.py):
//
// * SIGTERM: a warm shutdown: should_stop (apps/worker.py:494-497, 324-
//   361); a second SIGTERM sets the same flag again and nothing else.
// * SIGINT: a warm shutdown, and the next SIGINT becomes a cold one
//   (on_SIGINT, apps/worker.py:509-512).
// * SIGQUIT: a cold shutdown: should_terminate (apps/worker.py:500-503).
// * SIGHUP: should_stop and an atexit hook that execs sys.executable with
//   sys.argv (apps/worker.py:524-542). Started as `python -m celery`,
//   sys.argv[0] is celery/__main__.py, which then runs as a script and
//   fails on its relative import; the console script `celery` restarts.
//
// A warm shutdown leaves WorkController.start through WorkerShutdown:
// stop() closes the consumer, waits for the pool and dispatches
// worker_shutdown (worker.py:269-280, 312-320). A cold one raises
// WorkerTerminate: terminate() cancels the running task, whose message
// goes back to the queue (acks_late on Redis), and dispatches nothing
// (worker.py:272-273, 322-334). A cold shutdown during a warm one runs
// inside stop(), which still dispatches worker_shutdown.
//
// Not modeled: the soft shutdown (worker_soft_shutdown_timeout), other
// pools, early ack (cold-early-retry), REMAP_SIGTERM (its SIGTERM is
// SIGQUIT), `celery multi restart` (a TERM, then a new process: the
// warm case), the broker or backend unreachable at the start (probed:
// documented retries, and dedup-read-error's ack).
module sdtypes {
  type Sig = Term | Intr | Quit | Hup
  type Launch = ConsoleScript | PythonM
  type Worker = Up | Warm | Down
  // Lost: a run cancelled with its message already acked (early ack:
  // cold-early-retry); these instances use acks_late, so nothing is lost.
  type Task = Queued | Running | Done | Lost
  type Action =
    | Init
    | Signal(Sig)
    | Finish({ t: int })
    | Start({ t: int })
    | Exit({ exec: bool })

  type Config = { sigs: List[Sig], launch: Launch }
  pure def config(s: List[Sig]): Config = { sigs: s, launch: ConsoleScript }
}

module shutdown {
  import sdtypes.*

  const CFG: Config

  // ---- state ----
  var worker: Worker
  var tasks: int -> Task       // 1 is A (running at the start), 2 is B
  var sigIdx: int                // the index of the next signal to deliver
  var intSeen: bool            // SIGINT's handler is now the cold one
  var hupSeen: bool            // a SIGHUP registered the exec hook
  var hooks: int               // worker_shutdown dispatches
  var hookWhileRunning: bool   // worker_shutdown while a task ran
  var lastAction: Action
  var steps: int

  action init: bool = all {
    worker' = Up, tasks' = Map(1 -> Running, 2 -> Queued), sigIdx' = 0,
    intSeen' = false, hupSeen' = false, hooks' = 0,
    hookWhileRunning' = false, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  def running: bool = tasks.keys().exists(t => tasks.get(t) == Running)
  // A cancelled run goes back to the queue (acks_late on Redis).
  def requeued: int -> Task =
    tasks.keys().mapBy(t => if (tasks.get(t) == Running) Queued else tasks.get(t))

  // ---- actions ----
  // A signal arrives (the instance's signals, in order).
  // @cite celery@9c2f238 celery/apps/worker.py:324-361 "def _shutdown_handler("
  // @cite celery@9c2f238 celery/apps/worker.py:494-503 "_shutdown_handler, sig='SIGQUIT', how='Cold', callback=on_cold_shutdown, exitcode=EX_FAILURE,"
  // @cite celery@9c2f238 celery/apps/worker.py:509-512 "def on_SIGINT(worker):"
  // @cite celery@9c2f238 celery/apps/worker.py:532-542 "atexit.register(_reload_current_worker)"
  // @cite celery@9c2f238 celery/worker/worker.py:269-280 "except WorkerTerminate:"
  // @cite celery@9c2f238 celery/worker/worker.py:322-334 "def terminate(self, in_sighandler=False):"
  action signal: bool = all {
    sigIdx < CFG.sigs.length(), worker != Down,
    val s = CFG.sigs[sigIdx]
    // a cold shutdown: SIGQUIT, or SIGINT after SIGINT
    val cold = s == Quit or (s == Intr and intSeen)
    all {
      sigIdx' = sigIdx + 1,
      intSeen' = (intSeen or s == Intr), hupSeen' = (hupSeen or s == Hup),
      worker' = if (cold) Down else Warm,
      tasks' = if (cold) requeued else tasks,
      // a cold shutdown inside a warm one's stop() still dispatches it
      hooks' = if (cold and worker == Warm) hooks + 1 else hooks,
      hookWhileRunning' = hookWhileRunning,
      did(Signal(s)),
    }
  }

  // A run ends: SUCCESS is stored and the message acked.
  // @cite celery@9c2f238 celery/worker/request.py:620-631 "self.send_event('task-succeeded', result=retval, runtime=runtime)"
  action finish(t: int): bool = all {
    tasks.get(t) == Running, worker != Down,
    tasks' = tasks.set(t, Done),
    worker' = worker, sigIdx' = sigIdx, intSeen' = intSeen, hupSeen' = hupSeen,
    hooks' = hooks, hookWhileRunning' = hookWhileRunning,
    did(Finish({ t: t })),
  }
  // A running worker takes a queued task.
  // @env the broker delivers B (worker.qnt has the delivery)
  action startTask(t: int): bool = all {
    worker == Up, tasks.get(t) == Queued, not(running),
    tasks' = tasks.set(t, Running),
    worker' = worker, sigIdx' = sigIdx, intSeen' = intSeen, hupSeen' = hupSeen,
    hooks' = hooks, hookWhileRunning' = hookWhileRunning,
    did(Start({ t: t })),
  }
  // The warm shutdown ends: the pool is joined, worker_shutdown is
  // dispatched; after a SIGHUP the process execs itself.
  // @cite celery@9c2f238 celery/worker/worker.py:312-320 "self._send_worker_shutdown()"
  // @cite celery@9c2f238 celery/apps/worker.py:524-529 "os.execv(sys.executable, [sys.executable] + sys.argv)"
  action exit: bool = all {
    worker == Warm, not(running),
    hooks' = hooks + 1, hookWhileRunning' = (hookWhileRunning or running),
    val restarts = hupSeen and CFG.launch == ConsoleScript
    all {
      worker' = if (restarts) Up else Down,
      hupSeen' = if (restarts) false else hupSeen,
      did(Exit({ exec: hupSeen })),
    },
    tasks' = tasks, sigIdx' = sigIdx, intSeen' = intSeen,
  }

  // ---- step ----
  action step: bool = any {
    signal, exit,
    nondet t = Set(1, 2).oneOf()
    any { finish(t), startTask(t) },
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // worker_shutdown is never dispatched while a task still runs.
  val hookAfterRuns: bool = not(hookWhileRunning)
  // A worker that has stopped dispatched worker_shutdown.
  val hookOnShutdown: bool = worker == Down implies hooks >= 1

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  // A signal to a stopped worker has no effect: quiet once it is down.
  val quiet: bool = (sigIdx == CFG.sigs.length() or worker == Down)
    and not(running)
    and worker != Warm
    and not(worker == Up and tasks.keys().exists(t => tasks.get(t) == Queued))
  action stutter: bool = all {
    quiet, worker' = worker, tasks' = tasks, sigIdx' = sigIdx,
    intSeen' = intSeen, hupSeen' = hupSeen, hooks' = hooks,
    hookWhileRunning' = hookWhileRunning, lastAction' = lastAction,
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
  // At the end every task ran or its message waits in the queue.
  val quietDone: bool = quiet implies tasks.keys().forall(t =>
    tasks.get(t) == Done or tasks.get(t) == Queued)
  // After a SIGHUP the worker runs again and has taken every task.
  val restartResumes: bool =
    (quiet and CFG.sigs.indices().exists(i => CFG.sigs[i] == Hup))
      implies (worker == Up and tasks.keys().forall(t => tasks.get(t) == Done))

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverHook: bool = hooks == 0
}
