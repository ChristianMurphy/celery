// -*- mode: Bluespec; -*-
// Instances of revoke.qnt: <setting>_<case>. `late`/`early`: the ack mode
// (acks_late unless named); `revoke`: one revoke by id; `stamp`: by
// stamped header; `expire`: task 1 carries expires; `started`:
// task_track_started; `threads`: a pool without the semaphore
// (threads, gevent, eventlet), with task 2 ahead; `s2`: two pool processes per worker; `nomingle`:
// --without-mingle; `statedb`: --statedb; `cancel`:
// worker_cancel_long_running_tasks_on_connection_loss; `retry`:
// result_backend_always_retry; `db`: a backend whose error is not one of
// the broker's. Faults: `cut` (a broker connection lost), `kill`
// (SIGKILL), `halt` (a warm, soft or cold shutdown), `ctldup` (the
// broadcast sent twice), `backenderr` (the REVOKED write of a discard
// fails). Designs: `syncrevoke` (save the state db when a revoke
// arrives), `checkaccept` (on_accepted reads the revoked set), `sticky`
// (the store refuses to replace any final state).
//
// ---- instances: a setting and a case ---------------------------------------
module late_revoke {
  import revoketypes.* from "./revoke"
  import revoke(CFG = config(Late)).* from "./revoke"
}
module threads_revoke {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), semaphore: false, other: true }).* from "./revoke"
}
module late_revoke_started {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), trackStarted: true }).* from "./revoke"
}
module late_expire {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true }).* from "./revoke"
}
module late_revoke_cut {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), max: { ...NONE, cuts: 1 } }).* from "./revoke"
}
module late_revoke_cut_nomingle {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), mingle: false, max: { ...NONE, cuts: 1 } }).* from "./revoke"
}
module late_cut_s2 {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, slots: 2, max: { ...NONE, cuts: 1 } }).* from "./revoke"
}
module cancel_cut_s2 {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, slots: 2, cancelOnLoss: true, max: { ...NONE, cuts: 1 } }).* from "./revoke"
}
module statedb_kill {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), statedb: true, mingle: false, max: { ...NONE, kills: 1 } }).* from "./revoke"
}
module statedb_halt {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), statedb: true, mingle: false, max: { ...NONE, halts: 1 } }).* from "./revoke"
}
module stamp_statedb_halt {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), byStamp: true, statedb: true, mingle: false, max: { ...NONE, halts: 1 } }).* from "./revoke"
}
module syncrevoke_statedb_kill {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), statedb: true, mingle: false, syncOnRevoke: true, max: { ...NONE, kills: 1 } }).* from "./revoke"
}
module late_halt {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, max: { ...NONE, halts: 1 } }).* from "./revoke"
}
module early_halt {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Early), revoke: false, max: { ...NONE, halts: 1 } }).* from "./revoke"
}
module late_ctldup {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), max: { ...NONE, ctlDups: 1 } }).* from "./revoke"
}
module expire_backenderr {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true, other: true, max: { ...NONE, backendErrors: 1 } }).* from "./revoke"
}
module expire_backenderr_db {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true, other: true, brokerError: false, max: { ...NONE, backendErrors: 1 } }).* from "./revoke"
}
module expire_backenderr_retry {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true, other: true, alwaysRetry: true, max: { ...NONE, backendErrors: 1 } }).* from "./revoke"
}
module checkaccept_revoke {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), checkOnAccept: true }).* from "./revoke"
}
module sticky_checkaccept_revoke {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), checkOnAccept: true, finalSticky: true }).* from "./revoke"
}
module checkaccept_expire {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true, checkOnAccept: true }).* from "./revoke"
}
module expire_dup {
  import revoketypes.* from "./revoke"
  import revoke(CFG = { ...config(Late), revoke: false, expires: true, max: { ...NONE, dups: 1 } }).* from "./revoke"
}
