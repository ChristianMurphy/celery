// -*- mode: Bluespec; -*-
// Instances of events.qnt: one per ending, named after it.
//
// ---- instances: the ending ------------------------------------------------
module ev_returns {
  import eventtypes.* from "./events"
  import events(CFG = { ending: Returns }).* from "./events"
}
module ev_raises {
  import eventtypes.* from "./events"
  import events(CFG = { ending: Raises }).* from "./events"
}
module ev_lost {
  import eventtypes.* from "./events"
  import events(CFG = { ending: Lost }).* from "./events"
}
module ev_hardlimit {
  import eventtypes.* from "./events"
  import events(CFG = { ending: HardLimit }).* from "./events"
}
module ev_rejectdrop {
  import eventtypes.* from "./events"
  import events(CFG = { ending: RejectDrop }).* from "./events"
}
module ev_ignores {
  import eventtypes.* from "./events"
  import events(CFG = { ending: Ignores }).* from "./events"
}
module ev_updatethenignore {
  import eventtypes.* from "./events"
  import events(CFG = { ending: UpdateThenIgnore }).* from "./events"
}
module ev_unknown {
  import eventtypes.* from "./events"
  import events(CFG = { ending: Unknown }).* from "./events"
}
