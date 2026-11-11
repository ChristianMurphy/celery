// -*- mode: Bluespec; -*-
// Instances of reader.qnt: <how a reader waits>_<fault class>. `poll`:
// get_task_meta until a ready state; `pubsub`: the Redis result consumer;
// `rpc`: rpc://. Designs: `keepready` (no store replaces a ready state),
// `subfirst` (subscribe again before the MGET after a lost connection).
// `ids` instances build AsyncResults from the ids instead of a saved group.
//
// ---- instances: a mode and a fault class ------------------------------------
module poll_none {
  import readertypes.* from "./reader"
  import reader(CFG = config(Poll)).* from "./reader"
}
module poll_late {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(Poll), max: { ...NONE, late: 1 } }).* from "./reader"
}
module poll_expire {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(Poll), max: { ...NONE, expiries: 1 } }).* from "./reader"
}
module poll_error {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(Poll), max: { ...NONE, errors: 1 } }).* from "./reader"
}
module pubsub_none {
  import readertypes.* from "./reader"
  import reader(CFG = config(PubSub)).* from "./reader"
}
module pubsub_cut {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), max: { ...NONE, cuts: 1 } }).* from "./reader"
}
module pubsub_late {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), max: { ...NONE, late: 1 } }).* from "./reader"
}
module pubsub_expire {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), max: { ...NONE, expiries: 1 } }).* from "./reader"
}
module pubsub_error {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), max: { ...NONE, errors: 1 } }).* from "./reader"
}
module ids_pubsub_cut {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), group: false, max: { ...NONE, cuts: 1 } }).* from "./reader"
}
module rpc_none {
  import readertypes.* from "./reader"
  import reader(CFG = config(Rpc)).* from "./reader"
}
module rpc_late {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(Rpc), max: { ...NONE, late: 1 } }).* from "./reader"
}
module keepready_late {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(Poll), keep: KeepReady, max: { ...NONE, late: 1 } }).* from "./reader"
}
module keepready_pubsub_late {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), keep: KeepReady, max: { ...NONE, late: 1 } }).* from "./reader"
}
module subfirst_cut {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), subscribeFirst: true, max: { ...NONE, cuts: 1 } }).* from "./reader"
}
module subfirst_cut2 {
  import readertypes.* from "./reader"
  import reader(CFG = { ...config(PubSub), subscribeFirst: true, max: { ...NONE, cuts: 2 } }).* from "./reader"
}
