// -*- mode: Bluespec; -*-
// Instances of retry.qnt: <body>_<fault class>. `ok`: a run's body may
// return or call retry(); `fail`: every run calls retry().
//
// ---- instances: a body and a fault class -----------------------------------
module ok_none {
  import retrytypes.* from "./retry"
  import retry(CFG = config(true)).* from "./retry"
}
module fail_none {
  import retrytypes.* from "./retry"
  import retry(CFG = config(false)).* from "./retry"
}
module ok_node {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), max: { ...NONE, nodes: 1 } }).* from "./retry"
}
module fail_node {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(false), max: { ...NONE, nodes: 1 } }).* from "./retry"
}
module ok_dup {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), max: { ...NONE, dups: 1 } }).* from "./retry"
}
module ok_refuse {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), max: { ...NONE, refusals: 1 } }).* from "./retry"
}
module retryfirst_none {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), retryFirst: true }).* from "./retry"
}
module retryfirst_fail {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(false), retryFirst: true }).* from "./retry"
}
module retryfirst_node {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), retryFirst: true, max: { ...NONE, nodes: 1 } }).* from "./retry"
}
// ---- fenced writes and late copies (item 7) ----------------------------------
// `fenced`: the per-attempt epoch design; `late`: one late copy of a write
// whose reply was lost (result_backend_always_retry sent it again).
module fenced_ok_none {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), fenced: true }).* from "./retry"
}
module fenced_fail_none {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(false), fenced: true }).* from "./retry"
}
module fenced_ok_node {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), fenced: true, max: { ...NONE, nodes: 1 } }).* from "./retry"
}
module ok_late {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), max: { ...NONE, lateCopies: 1 } }).* from "./retry"
}
module retryfirst_late {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), retryFirst: true, max: { ...NONE, lateCopies: 1 } }).* from "./retry"
}
module fenced_ok_late {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(true), fenced: true, max: { ...NONE, lateCopies: 1 } }).* from "./retry"
}
module fenced_fail_late {
  import retrytypes.* from "./retry"
  import retry(CFG = { ...config(false), fenced: true, max: { ...NONE, lateCopies: 1 } }).* from "./retry"
}
