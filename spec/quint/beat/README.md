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
forward), `two` (a second scheduler). `sync1` sets `beat_sync_every = 1`.

<!-- generated: properties beat.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noDoubleSend` | safety | No crontab occurrence is sent twice, and no interval run is sent less than run_every after the one before. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Absence (a run sent twice, or an interval run sent early), global | docs/userguide/periodic-tasks.rst:19-21 (celery 768e476): one scheduler, "otherwise you'd end up with duplicate tasks" |
| `noMissedRun` | safety | While a scheduler runs, a run is sent at least every run_every of true time, and no crontab occurrence is skipped. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Absence (a run late or skipped while a scheduler runs), global | docs/userguide/periodic-tasks.rst (celery 768e476), Entries: a task is sent at the interval or crontab given |
| `quietDone` | liveness | At the end of the run a running scheduler has sent within the last run_every. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Response (lemma 3 of 3: at the end a running scheduler has sent within run_every) | docs/userguide/periodic-tasks.rst (celery 768e476) |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `neverQuiet` | witness | Some run ends. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Existence | sanity |
| `neverSentTwice` | witness | The entry is sent twice. Assumes: one periodic entry, an interval (run every R) or a crontab (every wall-clock multiple of R), R = 2, sync_every = 5, the run ends at true time 9; one or two schedulers (PersistentScheduler, each with its own file); a scheduler wakes when its entry is due. | Existence | sanity |
<!-- end generated -->

## Results

`python3 tools/check.py --tier full --only beat`.

<!-- generated: results beat.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| cron_back | 15 | noMissedRun (8), quietDone (12) | neverQuiet (10), neverSentTwice (8) |
| cron_fwd | 17 | noMissedRun (8) | neverQuiet (10), neverSentTwice (6) |
| cron_kill | 17 | noDoubleSend (11) | neverQuiet (10), neverSentTwice (8) |
| cron_none | 14 | none | neverQuiet (10), neverSentTwice (8) |
| cron_stop | 16 | none | neverQuiet (10), neverSentTwice (8) |
| interval_back | 15 | noMissedRun (7), quietDone (12) | neverQuiet (10), neverSentTwice (8) |
| interval_fwd | 16 | noDoubleSend (7) | neverQuiet (10), neverSentTwice (7) |
| interval_kill | 17 | noDoubleSend (11) | neverQuiet (10), neverSentTwice (8) |
| interval_none | 14 | none | neverQuiet (10), neverSentTwice (8) |
| sync1_cron_kill | 16 | none | neverQuiet (10), neverSentTwice (8) |
| sync1_interval_kill | 16 | none | neverQuiet (10), neverSentTwice (8) |
| two_cron_none | 19 | noDoubleSend (7) | neverQuiet (10), neverSentTwice (7) |
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
| `noDoubleSend` | two_cron_none (7) | documented | docs/userguide/periodic-tasks.rst:19-21 (celery 768e476): you have to ensure only a single scheduler is running for a schedule at a time, otherwise you'd end up with duplicate tasks |
| `noMissedRun` | cron_back (8), interval_back (7) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps back: last_run_at is in the future and nothing is due until the clock catches up |
| `noMissedRun` | cron_fwd (8) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the clock steps forward over an occurrence: it is never sent |
| `quietDone` | cron_back (12), interval_back (12) | open | open celery issue 6742 (System datetime change effects, beat service stops activities): beat reads the wall clock (schedules.py:151-157, 686-722). the run ends inside the delay |
<!-- end generated -->

## Findings

[beat-kill-double](../findings/beat-kill-double/) (duplicate): beat
killed with SIGKILL between two syncs sends the last crontab occurrence
again at its restart; 3 of 3 on the real code, 0 of 2 after SIGTERM, 0 of
1 with `beat_sync_every = 1`.
