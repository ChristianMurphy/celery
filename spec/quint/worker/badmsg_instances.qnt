// -*- mode: Bluespec; -*-
// Instances of badmsg.qnt: <kind>_<context>. Kinds: `known` (a registered
// task that raises; the control), `unknown` (no task of that name on the
// worker), `undecodable` (a body the serializer cannot read), `disallowed`
// (a content type outside accept_content), `badeta` (an ETA that cannot be
// converted). Contexts: `alone`, `errback` (link_error), `chain` (the first
// of two steps), `chord` (one of two header tasks).
//
// ---- instances: a kind of message and its context ----------------------------
module known_alone {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Known, ctx: Alone }).* from "./badmsg"
}
module known_errback {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Known, ctx: WithErrback }).* from "./badmsg"
}
module known_chain {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Known, ctx: ChainFirst }).* from "./badmsg"
}
module known_chord {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Known, ctx: ChordMember }).* from "./badmsg"
}
module unknown_alone {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Unknown, ctx: Alone }).* from "./badmsg"
}
module unknown_errback {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Unknown, ctx: WithErrback }).* from "./badmsg"
}
module unknown_chain {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Unknown, ctx: ChainFirst }).* from "./badmsg"
}
module unknown_chord {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Unknown, ctx: ChordMember }).* from "./badmsg"
}
module undecodable_alone {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Undecodable, ctx: Alone }).* from "./badmsg"
}
module undecodable_chord {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Undecodable, ctx: ChordMember }).* from "./badmsg"
}
module disallowed_alone {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: Disallowed, ctx: Alone }).* from "./badmsg"
}
module badeta_chord {
  import badmsgtypes.* from "./badmsg"
  import badmsg(CFG = { kind: BadEta, ctx: ChordMember }).* from "./badmsg"
}
