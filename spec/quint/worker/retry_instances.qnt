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
