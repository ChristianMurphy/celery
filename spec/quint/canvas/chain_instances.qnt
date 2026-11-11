// -*- mode: Bluespec; -*-
// Instances of chain.qnt: <ack mode or design>_<fault class>.
//
// ---- instances: an ack mode or design, and a fault class -------------------
module late_none {
  import chaintypes.* from "./chain"
  import chain(CFG = config(Late)).* from "./chain"
}
module early_none {
  import chaintypes.* from "./chain"
  import chain(CFG = config(Early)).* from "./chain"
}
module late_raises {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), aRaises: true }).* from "./chain"
}
module late_node {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), max: { ...NONE, nodes: 1 } }).* from "./chain"
}
module early_node {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Early), max: { ...NONE, nodes: 1 } }).* from "./chain"
}
module dedup_node {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), dedup: true, max: { ...NONE, nodes: 1 } }).* from "./chain"
}
module late_child {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), max: { ...NONE, children: 1 } }).* from "./chain"
}
module early_child {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Early), max: { ...NONE, children: 1 } }).* from "./chain"
}
module reject_child {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), rejectOnLost: true, max: { ...NONE, children: 1 } }).* from "./chain"
}
module late_refuse {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), max: { ...NONE, refusals: 1 } }).* from "./chain"
}
module late_storeerr {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), max: { ...NONE, storeErrors: 1 } }).* from "./chain"
}
module late_dup {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), max: { ...NONE, dups: 1 } }).* from "./chain"
}
module dedup_dup {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), dedup: true, max: { ...NONE, dups: 1 } }).* from "./chain"
}
module late_unencodable {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), unencodable: true }).* from "./chain"
}
module early_unencodable {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Early), unencodable: true }).* from "./chain"
}
module encfirst_unencodable {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), unencodable: true, encodeFirst: true }).* from "./chain"
}
module encfirst_storeerr {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), encodeFirst: true, max: { ...NONE, storeErrors: 1 } }).* from "./chain"
}
module encfirst_none {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), encodeFirst: true }).* from "./chain"
}
module quiet_storeerr {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), quietAfterSend: true, max: { ...NONE, storeErrors: 1 } }).* from "./chain"
}
module quiet_unencodable {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), quietAfterSend: true, unencodable: true }).* from "./chain"
}
module quiet_child {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), quietAfterSend: true, max: { ...NONE, children: 1 } }).* from "./chain"
}
module quiet_refuse {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), quietAfterSend: true, max: { ...NONE, refusals: 1 } }).* from "./chain"
}
module quiet_none {
  import chaintypes.* from "./chain"
  import chain(CFG = { ...config(Late), quietAfterSend: true }).* from "./chain"
}
