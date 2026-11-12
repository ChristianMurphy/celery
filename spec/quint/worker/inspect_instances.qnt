// -*- mode: Bluespec; -*-
// Instances of inspect.qnt: pool (three plain tasks wait for the one
// process), eta (three tasks with a countdown), bucket (three tasks with a
// rate limit), bucket_raise (and the rate is raised once), and the two
// designs.
//
// ---- instances: the tasks and the designs ------------------------------------
module pool {
  import insptypes.* from "./inspect"
  import inspect(CFG = PLAIN).* from "./inspect"
}
module eta {
  import insptypes.* from "./inspect"
  import inspect(CFG = { ...PLAIN, eta: true }).* from "./inspect"
}
module bucket {
  import insptypes.* from "./inspect"
  import inspect(CFG = { ...PLAIN, limited: true }).* from "./inspect"
}
module bucket_raise {
  import insptypes.* from "./inspect"
  import inspect(CFG = { ...PLAIN, limited: true, raiseRate: true }).* from "./inspect"
}
module bucket_visible {
  import insptypes.* from "./inspect"
  import inspect(CFG = { ...PLAIN, limited: true, bucketVisible: true }).* from "./inspect"
}
module bucket_raise_move {
  import insptypes.* from "./inspect"
  import inspect(CFG = { ...PLAIN, limited: true, raiseRate: true, bucketVisible: true, moveOnRaise: true }).* from "./inspect"
}
