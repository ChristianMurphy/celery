// -*- mode: Bluespec; -*-
// Instances of pidbox.qnt: fresh (each call its own reply queue name),
// reused (the second call reuses the first's name) with a destination
// (limit 1) or without (no limit), and the design ticketedLimit.
//
// ---- instances: the reply queue names, the limit and the design ------------
module fresh_limit {
  import pidboxtypes.* from "./pidbox"
  import pidbox(CFG = { sameQueue: false, limitOne: true, ticketedLimit: false }).* from "./pidbox"
}
module reused_limit {
  import pidboxtypes.* from "./pidbox"
  import pidbox(CFG = { sameQueue: true, limitOne: true, ticketedLimit: false }).* from "./pidbox"
}
module reused_nolimit {
  import pidboxtypes.* from "./pidbox"
  import pidbox(CFG = { sameQueue: true, limitOne: false, ticketedLimit: false }).* from "./pidbox"
}
module reused_limit_ticketed {
  import pidboxtypes.* from "./pidbox"
  import pidbox(CFG = { sameQueue: true, limitOne: true, ticketedLimit: true }).* from "./pidbox"
}
