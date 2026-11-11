// -*- mode: Bluespec; -*-
// Instances of beat.qnt: <schedule>_<fault class>. `interval`: run every R;
// `cron`: a crontab at every multiple of R. Faults: `kill` (SIGKILL, no
// sync), `stop` (a clean stop), `back` and `fwd` (the clock jumps), `two`
// (a second scheduler). `sync1`: beat_sync_every = 1.
//
// ---- instances: a schedule and a fault class --------------------------------
module interval_none {
  import beattypes.* from "./beat"
  import beat(CFG = config(Interval)).* from "./beat"
}
module cron_none {
  import beattypes.* from "./beat"
  import beat(CFG = config(Cron)).* from "./beat"
}
module interval_kill {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Interval), max: { ...NONE, kills: 1 } }).* from "./beat"
}
module cron_kill {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), max: { ...NONE, kills: 1 } }).* from "./beat"
}
module cron_stop {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), max: { ...NONE, stops: 1 } }).* from "./beat"
}
module sync1_cron_kill {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), syncEachSend: true, max: { ...NONE, kills: 1 } }).* from "./beat"
}
module sync1_interval_kill {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Interval), syncEachSend: true, max: { ...NONE, kills: 1 } }).* from "./beat"
}
module interval_back {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Interval), max: { ...NONE, jumps: 1 } }).* from "./beat"
}
module cron_back {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), max: { ...NONE, jumps: 1 } }).* from "./beat"
}
module interval_fwd {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Interval), jump: Forward, max: { ...NONE, jumps: 1 } }).* from "./beat"
}
module cron_fwd {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), jump: Forward, max: { ...NONE, jumps: 1 } }).* from "./beat"
}
module two_cron_none {
  import beattypes.* from "./beat"
  import beat(CFG = { ...config(Cron), beats: 2 }).* from "./beat"
}
