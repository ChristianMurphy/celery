// -*- mode: Bluespec; -*-
// Instances of redelivery.qnt: <broker>_<setting>_<case>.
//
// Short runs take 1 unit, long ones 3; the timeout (visibility timeout or
// consumer_timeout) is 2 units. p1 and p2 are the prefetch count of a
// worker with one pool process (worker_prefetch_multiplier 1 or 2); w1 is
// one worker instead of two.
//
// ---- instances: a broker, a setting and a case ------------------------------
module redis_late_short {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = config(Redis, Late)).* from "./redelivery"
}
module redis_late_long {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), runLen: 3 }).* from "./redelivery"
}
module redis_late_long_w1 {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), runLen: 3, workers: 1 }).* from "./redelivery"
}
module redis_late_long_w1p2 {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), runLen: 3, workers: 1, prefetch: 2 }).* from "./redelivery"
}
module redis_nopf_long_w1p2 {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), runLen: 3, workers: 1, prefetch: 2, disablePrefetch: true }).* from "./redelivery"
}
module redis_dedup_long_w1 {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), runLen: 3, workers: 1, dedup: true }).* from "./redelivery"
}
module redis_late_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), max: { ...NONE, kills: 1 } }).* from "./redelivery"
}
module redis_early_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Early), max: { ...NONE, kills: 1 } }).* from "./redelivery"
}
module redis_late_loss {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), max: { ...NONE, losses: 1 } }).* from "./redelivery"
}
module classic_late_short {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = config(Classic, Late)).* from "./redelivery"
}
module classic_late_long {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), runLen: 3 }).* from "./redelivery"
}
module classic_retry_long {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), runLen: 3, channelRetry: true }).* from "./redelivery"
}
module classic_dedup_long {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), runLen: 3, dedup: true }).* from "./redelivery"
}
module classic_early_cut {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Early), max: { ...NONE, cuts: 1 } }).* from "./redelivery"
}
module classic_late_cut {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), max: { ...NONE, cuts: 1 } }).* from "./redelivery"
}
module classic_late_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), max: { ...NONE, kills: 1 } }).* from "./redelivery"
}
module classic_noconfirm_loss {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), confirm: false, max: { ...NONE, losses: 1 } }).* from "./redelivery"
}
module classic_confirm_loss {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), max: { ...NONE, losses: 1 } }).* from "./redelivery"
}
module quorum_late_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Quorum, Late), limit: 1, max: { ...NONE, kills: 2 } }).* from "./redelivery"
}
module quorum_dlx_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Quorum, Late), limit: 1, dlx: true, max: { ...NONE, kills: 2 } }).* from "./redelivery"
}
module quorum_late_long {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Quorum, Late), runLen: 3 }).* from "./redelivery"
}
// worker_deduplicate_successful_tasks: a short task, a worker killed, and
// what the check can get wrong: a failed read, an expired result, a copy
// the publisher sent again
module redis_dedup_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), dedup: true, max: { ...NONE, kills: 1 } }).* from "./redelivery"
}
module classic_dedup_kill {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Classic, Late), dedup: true, max: { ...NONE, kills: 1 } }).* from "./redelivery"
}
module redis_dedup_readerr {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), dedup: true, max: { ...NONE, kills: 1, readErrors: 1 } }).* from "./redelivery"
}
module redis_dedupretry_readerr {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), dedup: true, alwaysRetry: true, max: { ...NONE, kills: 1, readErrors: 1 } }).* from "./redelivery"
}
module redis_dedup_expire {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), dedup: true, max: { ...NONE, kills: 1, expiries: 1 } }).* from "./redelivery"
}
module redis_dedup_republish {
  import redeliverytypes.* from "./redelivery"
  import redelivery(CFG = { ...config(Redis, Late), dedup: true, max: { ...NONE, republishes: 1 } }).* from "./redelivery"
}
