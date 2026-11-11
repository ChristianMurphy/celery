// -*- mode: Bluespec; -*-
// Instances of redis_transport.qnt: one module per configuration checked.
//
//   quint run --main=<instance> redis_transport_instances.qnt --invariant=<property>
//
// ---- instances: a design and a fault class ---------------------------------
module main_none {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = { design: BrpopThenRecord, max: NO_FAULTS }).* from "./redis_transport"
}
module main_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord,
    max: { ...NO_FAULTS, crashes: 1 },
  }).* from "./redis_transport"
}
module main_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord,
    max: { ...NO_FAULTS, creditLosses: 1 },
  }).* from "./redis_transport"
}
module main_credit_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord,
    max: { ...NO_FAULTS, creditLosses: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module main_held_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord,
    max: { ...NO_FAULTS, heldTimeouts: 1 },
  }).* from "./redis_transport"
}
module paused_credit_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScriptPaused,
    max: { ...NO_FAULTS, creditLosses: 1, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_none {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = { design: PopScript, max: NO_FAULTS }).* from "./redis_transport"
}
module pr_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, creditLosses: 1 },
  }).* from "./redis_transport"
}
module pr_reply_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_credit_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, creditLosses: 1, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_mismatch {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, mismatches: 1 },
  }).* from "./redis_transport"
}
module pr_mismatch_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, mismatches: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_credit_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, creditLosses: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_held_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript,
    max: { ...NO_FAULTS, heldTimeouts: 1 },
  }).* from "./redis_transport"
}
