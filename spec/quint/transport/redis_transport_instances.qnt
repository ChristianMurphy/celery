// -*- mode: Bluespec; -*-
// Instances of redis_transport.qnt: one module per configuration checked.
//
//   quint run --main=<instance> redis_transport_instances.qnt --invariant=<property>
//
// ---- instances: a design and a fault class ---------------------------------
module main_none {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord, renewal: NoRenewal, putBack: ByScore,
    max: NO_FAULTS,
  }).* from "./redis_transport"
}
module main_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, crashes: 1 },
  }).* from "./redis_transport"
}
module main_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1 },
  }).* from "./redis_transport"
}
module main_credit_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module main_held_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: BrpopThenRecord, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, heldTimeouts: 1 },
  }).* from "./redis_transport"
}
module paused_credit_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScriptPaused, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_none {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: NO_FAULTS,
  }).* from "./redis_transport"
}
module pr_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1 },
  }).* from "./redis_transport"
}
module pr_reply_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_credit_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module pr_mismatch {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, mismatches: 1 },
  }).* from "./redis_transport"
}
module pr_mismatch_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, mismatches: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_credit_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, creditLosses: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module pr_held_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByScore,
    max: { ...NO_FAULTS, heldTimeouts: 1 },
  }).* from "./redis_transport"
}
// Renewal of unacked messages on top of the PR (kombu issue 337): a held
// message outlasts the timeout while its consumer is blocked, and the next
// consumer loses its credit before it reads the reply.
module renewblind_held_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: RenewBlind, putBack: ByScore,
    max: { ...NO_FAULTS, heldTimeouts: 1, creditLosses: 1 },
  }).* from "./redis_transport"
}
module renewown_held_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: RenewOwn, putBack: ByScore,
    max: { ...NO_FAULTS, heldTimeouts: 1, creditLosses: 1 },
  }).* from "./redis_transport"
}
// The pop token: PUT_BACK compares a token stored with the record. Every
// fault class of the PR, and the blind renewal.
module token_none {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: NO_FAULTS,
  }).* from "./redis_transport"
}
module token_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, crashes: 1 },
  }).* from "./redis_transport"
}
module token_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, creditLosses: 1 },
  }).* from "./redis_transport"
}
module token_reply_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module token_credit_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, creditLosses: 1, replyTimeouts: 1 },
  }).* from "./redis_transport"
}
module token_mismatch {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, mismatches: 1 },
  }).* from "./redis_transport"
}
module token_mismatch_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, mismatches: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module token_credit_crash {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, creditLosses: 1, crashes: 1 },
  }).* from "./redis_transport"
}
module token_held_timeout {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: NoRenewal, putBack: ByToken,
    max: { ...NO_FAULTS, heldTimeouts: 1 },
  }).* from "./redis_transport"
}
module token_renewblind_held_credit {
  import transporttypes.* from "./redis_transport"
  import redis_transport(CFG = {
    design: PopScript, renewal: RenewBlind, putBack: ByToken,
    max: { ...NO_FAULTS, heldTimeouts: 1, creditLosses: 1 },
  }).* from "./redis_transport"
}
