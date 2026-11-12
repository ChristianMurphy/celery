// -*- mode: Bluespec; -*-
// Instances of lifecycle.qnt: [no]cancel_loss[2] (the setting off or on,
// one or two connection losses), shrink (autoscale down), poolrestart,
// recycle (max_tasks_per_child).
//
// ---- instances: the setting and the event ------------------------------------
module nocancel_loss {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = config(false, 1)).* from "./lifecycle"
}
module nocancel_loss2 {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = config(false, 2)).* from "./lifecycle"
}
module cancel_loss {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = config(true, 1)).* from "./lifecycle"
}
module cancel_loss2 {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = config(true, 2)).* from "./lifecycle"
}
module shrink {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = { ...config(false, 0), shrink: true }).* from "./lifecycle"
}
module poolrestart {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = { ...config(false, 0), poolRestart: true }).* from "./lifecycle"
}
module recycle {
  import lifetypes.* from "./lifecycle"
  import lifecycle(CFG = { ...config(false, 0), recycle: true }).* from "./lifecycle"
}
