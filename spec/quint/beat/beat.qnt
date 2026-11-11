// -*- mode: Bluespec; -*-
// celery beat: is each scheduled run sent once, and none missed, across a
// restart, a killed scheduler, a clock jump and a second scheduler?
//
// Celery 768e476. One periodic entry, either an interval (run every R) or a
// crontab (at every wall-clock multiple of R). Scheduler.tick
// (beat.py:343-399) sends a due entry: apply_entry -> apply_async, which
// first reserves the entry: last_run_at becomes now (reserve, beat.py:424-
// 426; ScheduleEntry._next_instance, beat.py:140-146), then publishes. An
// interval is due when now - last_run_at >= run_every (schedules.py:151-
// 157); a crontab when its first occurrence after last_run_at is not later
// than now (schedules.py:675-722). After a send apply_async syncs the store
// if it never synced since the start, or more than sync_every (3 minutes)
// ago, or after beat_sync_every sends (beat.py:416-422, 452-455, 467-473).
// PersistentScheduler keeps last_run_at in a shelve file
// (beat.py:592-628, 658-667) and reads it back at the start; close()
// syncs (beat.py:669-683). A new entry starts with last_run_at = now.
//
// Time is discrete: tt is the true time, wall the machine's clock (the
// scheduler reads wall). Time advances only while no running scheduler has
// a due entry: the scheduler wakes when its entry is due (tick returns the
// delay, at most beat_max_loop_interval). The run ends at tt = T.
//
// Not modeled: beat_cron_starting_deadline, timezones and DST, several
// entries, the publish failing (apply_entry logs it and the run is
// skipped), a schedule edited at run time, the broker.
module beattypes {
  type BeatId = { b: int }
  pure def Bt(i: int): BeatId = { b: i }

  type Kind = Interval | Cron
  type Jump = Forward | Backward

  // A scheduler's process. last: last_run_at in memory; lastSync: the
  // true time of its last sync, -1 for none since it started.
  type Proc = Down | Up({ last: int, lastSync: int })
  pure def isUp(p: Proc): bool = match p { | Down => false | Up(_) => true }
  pure val NO_PROC: { last: int, lastSync: int } = { last: 0, lastSync: -1 }
  pure def procOf(p: Proc): { last: int, lastSync: int } = match p {
    | Down => NO_PROC | Up(u) => u
  }

  type Action =
    | Init
    | Start(BeatId)
    | Tick({ b: BeatId, wall: int, synced: bool })
    | Advance
    | Kill(BeatId)
    | Stop(BeatId)
    | ClockJump(Jump)

  type Budget = { kills: int, stops: int, jumps: int }
  pure val NONE: Budget = { kills: 0, stops: 0, jumps: 0 }

  type Config = {
    kind: Kind,
    // two schedulers for the same schedule (each with its own file)
    beats: int,
    // beat_sync_every = 1: sync after every send (default 0: by time)
    syncEachSend: bool,
    // the clock jumps by this much (a step of NTP, a manual change)
    jump: Jump,
    max: Budget,
  }
  pure def config(k: Kind): Config =
    { kind: k, beats: 1, syncEachSend: false, jump: Backward, max: NONE }
}

module beat {
  import beattypes.*

  const CFG: Config

  pure val R: int = 2      // run_every, or the crontab's period
  pure val S: int = 5      // sync_every
  pure val J: int = 3      // the size of a clock jump
  pure val T: int = 9      // the end of the run
  pure val BEATS: Set[BeatId] = 1.to(CFG.beats).map(i => Bt(i))

  // ---- state ----
  var tt: int
  var wall: int
  var procs: BeatId -> Proc
  var saved: BeatId -> int        // last_run_at in the file; -1: no file
  // ---- observations ----
  var sends: int
  var lastSendTT: int             // true time of the last send, -1 none
  var lastOcc: int                // crontab occurrence of the last send
  var occs: Set[int]              // crontab occurrences sent
  var watch: int                  // true time since which a send is due
                                  // within R (a scheduler ran throughout); -1
  var double: bool                // a run sent twice, or an interval run early
  var missed: bool                // a run not sent in time, or skipped
  var used: Budget
  var lastAction: Action
  var steps: int

  action init: bool = all {
    tt' = 0, wall' = 0, procs' = BEATS.mapBy(_ => Down),
    saved' = BEATS.mapBy(_ => -1),
    sends' = 0, lastSendTT' = -1, lastOcc' = -1, occs' = Set(), watch' = -1,
    double' = false, missed' = false, used' = NONE, lastAction' = Init,
    steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }
  action keepObs: bool = all {
    sends' = sends, lastSendTT' = lastSendTT, lastOcc' = lastOcc,
    occs' = occs, double' = double, missed' = missed,
  }
  def anyUp: bool = BEATS.exists(b => isUp(procs.get(b)))
  def others(b: BeatId): bool = BEATS.exists(c => c != b and isUp(procs.get(c)))

  // The crontab's first occurrence after x (a multiple of R).
  pure def nextOcc(x: int): int = (x / R + 1) * R
  def due(b: BeatId): bool = {
    val p = procs.get(b)
    isUp(p) and match CFG.kind {
      | Interval => wall - procOf(p).last >= R
      | Cron => nextOcc(procOf(p).last) <= wall
    }
  }

  // ---- actions ----
  // A scheduler starts: last_run_at from its file, or now for a new entry
  // (beat.py:592-628, 123-133).
  // @cite celery celery/beat.py:592-628 "entries = self._store.setdefault('entries', {})"
  // @cite celery celery/beat.py:123-133 "self.last_run_at = last_run_at or self.default_now()"
  action start(b: BeatId): bool = all {
    not(isUp(procs.get(b))), tt < T,
    procs' = procs.set(b, Up({
      last: if (saved.get(b) < 0) wall else saved.get(b), lastSync: -1 })),
    watch' = if (anyUp) watch else tt,
    tt' = tt, wall' = wall, saved' = saved, keepObs, used' = used,
    did(Start(b)),
  }
  // A due entry is sent: reserve (last_run_at = now), publish, then sync
  // if due (beat.py:424-458, 416-422).
  // @cite celery celery/beat.py:366-380 "next_entry = self.reserve(entry)"
  // @cite celery celery/beat.py:424-426 "new_entry = self.schedule[entry.name] = next(entry)"
  // @cite celery celery/beat.py:416-422 "(time.monotonic() - self._last_sync) > self.sync_every) or"
  // @cite celery celery/schedules.py:151-157 "return remaining("
  // @cite celery celery/schedules.py:686-722 "due = rem == 0"
  action tick(b: BeatId): bool = {
    val u = procOf(procs.get(b))
    val sync = CFG.syncEachSend or u.lastSync < 0 or tt - u.lastSync > S
    val occ = wall / R
    all {
      due(b),
      procs' = procs.set(b, Up({ last: wall,
                                  lastSync: if (sync) tt else u.lastSync })),
      saved' = if (sync) saved.set(b, wall) else saved,
      sends' = sends + 1, lastSendTT' = tt, lastOcc' = occ,
      occs' = occs.union(Set(occ)),
      double' = (double or match CFG.kind {
        | Interval => lastSendTT >= 0 and tt - lastSendTT < R
        | Cron => occs.contains(occ)
      }),
      missed' = (missed or (CFG.kind == Cron and watch >= 0 and lastOcc >= 0
                            and occ > lastOcc + 1)),
      watch' = tt,
      tt' = tt, wall' = wall, used' = used,
      did(Tick({ b: b, wall: wall, synced: sync })),
    }
  }
  // Time passes while nothing is due (the scheduler sleeps until then).
  // @cite celery celery/beat.py:366-369 "return min(event[0] - now, max_interval)"
  action advance: bool = all {
    tt < T, BEATS.forall(b => not(due(b))),
    tt' = tt + 1, wall' = wall + 1,
    missed' = (missed or (watch >= 0 and tt + 1 - watch > R)),
    procs' = procs, saved' = saved, watch' = watch,
    sends' = sends, lastSendTT' = lastSendTT, lastOcc' = lastOcc,
    occs' = occs, double' = double, used' = used,
    did(Advance),
  }

  // ---- faults ----
  // SIGKILL (or a crash, an OOM kill): nothing is synced.
  // @env the scheduler's process is killed
  action kill(b: BeatId): bool = all {
    isUp(procs.get(b)), used.kills < CFG.max.kills,
    procs' = procs.set(b, Down), saved' = saved,
    watch' = if (others(b)) watch else -1,
    lastOcc' = if (others(b)) lastOcc else -1,
    tt' = tt, wall' = wall,
    sends' = sends, lastSendTT' = lastSendTT, occs' = occs, double' = double,
    missed' = missed,
    used' = { ...used, kills: used.kills + 1 }, did(Kill(b)),
  }
  // A clean stop: close() syncs (beat.py:669-683, 478-500).
  // @cite celery celery/beat.py:669-683 "super().close()"
  action stop(b: BeatId): bool = all {
    isUp(procs.get(b)), used.stops < CFG.max.stops,
    procs' = procs.set(b, Down),
    saved' = saved.set(b, procOf(procs.get(b)).last),
    watch' = if (others(b)) watch else -1,
    lastOcc' = if (others(b)) lastOcc else -1,
    tt' = tt, wall' = wall,
    sends' = sends, lastSendTT' = lastSendTT, occs' = occs, double' = double,
    missed' = missed,
    used' = { ...used, stops: used.stops + 1 }, did(Stop(b)),
  }
  // The machine's clock is stepped (beat reads it through app.now()).
  // @env the wall clock jumps
  action clockJump: bool = all {
    used.jumps < CFG.max.jumps,
    wall' = match CFG.jump { | Forward => wall + J | Backward => wall - J },
    CFG.jump == Forward or wall >= J,
    tt' = tt, procs' = procs, saved' = saved, watch' = watch, keepObs,
    used' = { ...used, jumps: used.jumps + 1 }, did(ClockJump(CFG.jump)),
  }

  // ---- step ----
  action step: bool = any {
    advance, clockJump,
    { nondet b = BEATS.oneOf() any { start(b), tick(b), kill(b), stop(b) } },
  }

  // ---- properties ----------------------------------------------------------
  // invariants
  // No crontab occurrence is sent twice, and no interval run is sent less
  // than run_every after the one before.
  val noDoubleSend: bool = not(double)
  // While a scheduler runs, a run is sent at least every run_every, and no
  // crontab occurrence is skipped.
  val noMissedRun: bool = not(missed)

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state (tt is bounded by
  // T, and every other action uses a budget or needs one of them).
  val quiet: bool = tt == T and BEATS.forall(b => not(due(b)))
  action stutter: bool = all {
    quiet, tt' = tt, wall' = wall, procs' = procs, saved' = saved,
    watch' = watch, keepObs, used' = used, lastAction' = lastAction,
    steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end a running scheduler has sent within the last run_every.
  val quietDone: bool = quiet implies (watch < 0 or tt - watch <= R)

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverSentTwice: bool = sends < 2
}
