// -*- mode: Bluespec; -*-
// Instances of eta.qnt: <what is set>_<clocks>[_relative]. `cd`: countdown
// 2; `exp`: expires 1; `cdexp`: countdown 1 and expires 3; `cdpastexp`: countdown 3
// and expires 2. Clocks: `same`,
// `wahead` (the worker's clock 2 ahead of the client's), `wbehind` (2
// behind). `relative`: the message carries durations (a comparison).
//
// ---- instances: what is set and the clocks ----------------------------------
module cd_same {
  import etatypes.* from "./eta"
  import etaskew(CFG = config(2, NONE)).* from "./eta"
}
module cd_wahead {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(2, NONE), workerOff: 2 }).* from "./eta"
}
module cd_wbehind {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(2, NONE), workerOff: -2 }).* from "./eta"
}
module exp_same {
  import etatypes.* from "./eta"
  import etaskew(CFG = config(NONE, 1)).* from "./eta"
}
module exp_wahead {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(NONE, 1), workerOff: 2 }).* from "./eta"
}
module exp_wbehind {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(NONE, 1), workerOff: -2 }).* from "./eta"
}
module cdexp_same {
  import etatypes.* from "./eta"
  import etaskew(CFG = config(1, 3)).* from "./eta"
}
module cdexp_wahead_relative {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(1, 3), workerOff: 2, design: Relative }).* from "./eta"
}
module exp_wahead_relative {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(NONE, 1), workerOff: 2, design: Relative }).* from "./eta"
}
module exp_same_relative {
  import etatypes.* from "./eta"
  import etaskew(CFG = { ...config(NONE, 1), design: Relative }).* from "./eta"
}
module cdpastexp_same {
  import etatypes.* from "./eta"
  import etaskew(CFG = config(3, 2)).* from "./eta"
}
