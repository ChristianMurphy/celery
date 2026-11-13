# Beat

`beat.qnt`, `beat_instances.qnt`, celery 768e476 (beat.py and schedules.py
are the same at 9c2f238).

## Model

One periodic entry: an interval (run every R) or a crontab (at every
wall-clock multiple of R). A tick sends a due entry: it reserves the entry
first (last_run_at becomes now, beat.py:424-426), publishes, and then syncs
the schedule file if it has not synced since it started, or if sync_every
(3 minutes) has passed, or after `beat_sync_every` sends (beat.py:416-422,
452-455). A start reads last_run_at from the file; a new entry starts at
now. A clean stop syncs (close, beat.py:669-683). Time is discrete: the true
time and the machine's wall clock, which beat reads. A scheduler wakes when
its entry is due. Instances `<schedule>_<fault class>`: `kill` (SIGKILL: no
sync), `stop` (a clean stop), `back` and `fwd` (the wall clock steps back or
forward), `two` (a second scheduler), `shared` (a second scheduler on the
same schedule file), `pause` (the process stopped, by SIGSTOP or a laptop's
sleep, for longer than run_every). `sync1` sets `beat_sync_every = 1`.
`noBurst` checks that a scheduler sends the entry at most once at any one
time: after a pause or a restart the missed runs are sent once.

<!-- generated: properties beat.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noDoubleSend` | safety | No crontab occurrence is sent twice, and no interval run is sent less than run_every after the one before. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Absence (a run sent twice, or an interval run sent early), global | docs/userguide/periodic-tasks.rst:19-21 (celery 768e476): one scheduler, "otherwise you'd end up with duplicate tasks" |
| `noMissedRun` | safety | While a scheduler runs, a run is sent at least every run_every of true time, and no crontab occurrence is skipped. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Absence (a run late or skipped while a scheduler runs), global | docs/userguide/periodic-tasks.rst (celery 768e476), Entries: a task is sent at the interval or crontab given |
| `noBurst` | safety | A scheduler sends the entry at most once at any one time: after a pause, a restart or a clock jump the runs it missed are sent once, not one by one. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Bounded existence (at most one send per scheduler at a time), global | docs/userguide/configuration.rst:4714-4718 (celery 768e476): only the most recent missed run time is considered; past due cron jobs run immediately |
| `quietDone` | liveness | At the end of the run a running scheduler has sent within the last run_every. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Response (lemma 3 of 3: at the end a running scheduler has sent within run_every) | docs/userguide/periodic-tasks.rst (celery 768e476) |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run in which the code keeps working ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep); weak fairness of `work` (every action that uses no fault budget), none for the faults. | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Existence | sanity |
| `neverSentTwice` | witness | The entry is sent twice. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file or both on one file); a scheduler wakes when its entry is due; faults per instance: a kill, a clean stop, a clock jump of 3, a pause of 3 (SIGSTOP or a sleep). | Existence | sanity |
<!-- end generated -->

## Results

`python3 tools/check.py --tier full --only beat`.

<!-- generated: results beat.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| cron_back | 15 | noMissedRun (8), quietDone (12) | neverQuiet (10), neverSentTwice (8) |
| cron_fwd | 17 | noMissedRun (8), noBurst (7) | neverQuiet (10), neverSentTwice (6) |
| cron_kill | 17 | noDoubleSend (11), noBurst (11) | neverQuiet (10), neverSentTwice (8) |
| cron_none | 14 | none | neverQuiet (10), neverSentTwice (8) |
| cron_pause | 14 | none | neverQuiet (10), neverSentTwice (6) |
| cron_stop | 16 | none | neverQuiet (10), neverSentTwice (8) |
| interval_back | 15 | noMissedRun (7), quietDone (12) | neverQuiet (10), neverSentTwice (8) |
| interval_fwd | 16 | noDoubleSend (7), noBurst (7) | neverQuiet (10), neverSentTwice (7) |
| interval_kill | 17 | noDoubleSend (11), noBurst (11) | neverQuiet (10), neverSentTwice (8) |
| interval_none | 14 | none | neverQuiet (10), neverSentTwice (8) |
| interval_pause | 14 | none | neverQuiet (10), neverSentTwice (7) |
| sync1_cron_kill | 16 | none | neverQuiet (10), neverSentTwice (8) |
| sync1_interval_kill | 16 | none | neverQuiet (10), neverSentTwice (8) |
| two_cron_none | 19 | noDoubleSend (7) | neverQuiet (10), neverSentTwice (7) |
| two_cron_shared | 19 | noDoubleSend (7) | neverQuiet (10), neverSentTwice (7) |
| two_interval_shared | 19 | noDoubleSend (7) | neverQuiet (10), neverSentTwice (7) |
<!-- end generated -->

What the model says:

* A scheduler killed between two syncs starts again from the older
  last_run_at in its file and sends again what it sent since
  (`interval_kill`, `cron_kill`): a crontab occurrence sent twice, an
  interval run early. A clean stop syncs (`cron_stop` passes), and so does
  `beat_sync_every = 1` (`sync1_*` pass).
* The wall clock stepping back delays every run by the step: nothing is
  sent until the clock is past last_run_at again (`*_back`; celery issue
  6742). Stepping forward sends an interval run early and skips the crontab
  occurrences in between (`*_fwd`).
* Two schedulers send every run twice (`two_cron_none`; documented).

Why each violation is expected:

<!-- generated: reasons beat.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noDoubleSend` | cron_kill (11), interval_kill (11) | open | finding beat-kill-double (not in the tracker yet; no upstream thread found). killed between two syncs, beat starts again from the file's older last_run_at and sends the last occurrence again |
| `noDoubleSend` | interval_fwd (7) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps forward: the interval run is due early |
| `noDoubleSend` | two_cron_none (7), two_cron_shared (7), two_interval_shared (7) | documented | docs/userguide/periodic-tasks.rst:19-21 (celery 768e476): you have to ensure only a single scheduler is running for a schedule at a time, otherwise you'd end up with duplicate tasks |
| `noMissedRun` | cron_back (8), interval_back (7) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps back: last_run_at is in the future and nothing is due until the clock catches up |
| `noMissedRun` | cron_fwd (8) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps forward over an occurrence: it is never sent |
| `noBurst` | cron_kill (11), interval_kill (11) | open | finding beat-kill-double (a docs change on branch docs/beat-sync-after-kill). the restarted scheduler reads the older last_run_at and sends at once what it sent just before the kill |
| `noBurst` | cron_fwd (7), interval_fwd (7) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps forward right after a send: the entry is due again at the same true time |
| `quietDone` | cron_back (12), interval_back (12) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the run ends inside the delay |
<!-- end generated -->

## Findings

[beat-kill-double](../findings/beat-kill-double/) (duplicate): beat
killed with SIGKILL between two syncs sends the last crontab occurrence
again at its restart; 3 of 3 on the real code, 0 of 2 after SIGTERM, 0 of
1 with `beat_sync_every = 1`.

## Leftovers probed on the real code

The scheduler's decisions (reserve, is_due, the heap, the shelve file,
sync, setup_schedule's merge and timezone reset) were run on 9c2f238 in
one process with the clock injected (app.now and time.monotonic) and
send_task recording instead of publishing (scratchpad `beatsim.py`, 1 of 1
each). Two schedulers ran as real `celery beat` processes on Redis.

| Case | What the real code did | Status |
|---|---|---|
| a pause of 10 minutes (interval 60 s, crontab every minute, solar sunrise over a 4 hour pause) | one send per entry at the resume, none for the runs in between | documented: configuration.rst:4714-4718, only the most recent missed run is considered (`*_pause` pass `noBurst`) |
| two schedulers on one schedule file (Python 3.14, and 5.5.3 on Python 3.12) | both sent every run (13 and 12 sends in 12 s for 6 runs), no lock, no error | documented: periodic-tasks.rst:19-21 (`two_*_shared`) |
| the timezone setting changed between a stop at 09:59:30 and a start at 10:00:30, crontab minute=0 | the file is reset (beat.py:606-611, a warning is logged) and the 10:00 run is not sent; with the timezone unchanged it is sent at the restart | the reset is logged; not modeled |
| crontab(minute=0) edited to minute=30 between a stop and a start at 10:45 | sent at 10:45 and then at 11:30: the entry keeps last_run_at (beat.py:155-165, 519-532), so the new schedule's 10:30 counts as missed | the documented catch-up of a missed run |
| an entry removed and added back after its due time | not sent until its next occurrence (a new entry starts at now) | as for a new entry |
| Europe/Berlin, the spring and autumn DST changes, crontab(hour=2, minute=30), crontab(minute=0), every hour | spring: 02:30 ran once at 03:30 CEST (01:30 UTC), hourly runs one real hour apart; autumn: 02:30 ran once, but crontab(minute=0) skipped the second 02:00 (CET): 6 runs in 7 real hours | open celery issue 10107 (Hourly crontab skips execution during fall-back DST transition; closed fix PRs 10131 and 10440, open PR 10608) |
| schedule(10 minutes, relative=True) started at 10:07:13 | runs at 10:17:00, 10:27:00, 10:37:00 (relative=False: 10:17:13 and so on) | documented: schedules.py:135-136 (9c2f238), "the run time will be rounded to the resolution of the interval" |

