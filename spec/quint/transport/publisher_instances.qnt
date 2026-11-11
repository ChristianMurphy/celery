// -*- mode: Bluespec; -*-
// Instances of publisher.qnt: confirms off or on, and a fault class.
//
// ---- instances: confirms and a fault class --------------------------------
module off_none {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOff, max: NONE }).* from "./publisher"
}
module off_crash {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOff, max: { ...NONE, crashes: 1 } }).* from "./publisher"
}
module off_drop {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOff, max: { ...NONE, drops: 1 } }).* from "./publisher"
}
module on_none {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOn, max: NONE }).* from "./publisher"
}
module on_crash {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOn, max: { ...NONE, crashes: 1 } }).* from "./publisher"
}
module on_drop {
  import publishertypes.* from "./publisher"
  import publisher(CFG = { confirms: ConfirmsOn, max: { ...NONE, drops: 1 } }).* from "./publisher"
}
