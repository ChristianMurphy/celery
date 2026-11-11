# Baselines: changes that must not change a result

## The retyping

This directory holds the matrix of the untyped specs at 355c8b7 and of the
retyped ones: 187 cells (29 instances, 6 or 7 properties), each checked by the
simulator (2,000 samples of 40 steps, seed 17) and by an exhaustive search
with TLC. `python3 compare.py untyped-355c8b7.json
typed.json`:

* verdicts: 133 hold and 54 are violated, the same cells before and after, in
  both engines;
* shortest counterexamples (TLC, breadth first): the same length in all 54;
* the simulator and TLC disagree on one cell, before and after:
  `redis_wlost_late.quietDone`, which 2,000 samples miss and TLC finds in 14
  states (it took Apalache 35 minutes at 16 steps);
* 33 cells, all on `chord_unlock` instances, have a state graph one level
  shallower. `Finish`, `Publish`, `LoseWorker` and `LoseChild` no longer carry
  a header index in `lastAction` (the untyped record had an `h` field for
  every action, 0 for `chord_unlock`), so pairs of states that differed only
  there are one state. With `h` put back in a scratch copy, the depths are
  the old ones (15 and 23 for `unlock_none` and `unlock_dup`).

The types found no bug in the specs. The time went up: TLC 1,080 s to 1,875 s
summed over the cells, the simulator 1,095 s to 1,668 s, on a machine other
jobs were using.
