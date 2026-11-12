// -*- mode: Bluespec; -*-
// Instances of eager.qnt: <real|eager>_<body>[_<options>]. real: a worker
// runs the task; eager: task_always_eager. cd_expires: countdown 3 and
// expires 1; retry_expires: a retry countdown of 2 and expires 1.
//
// ---- instances: the mode and the task ------------------------------------------
module real_returns {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Returns, false)).* from "./eager"
}
module real_retry_once {
  import eagertypes.* from "./eager"
  import eager(CFG = config(RetryOnce, false)).* from "./eager"
}
module real_retry_always {
  import eagertypes.* from "./eager"
  import eager(CFG = config(RetryAlways, false)).* from "./eager"
}
module real_ignores {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Ignores, false)).* from "./eager"
}
module real_rejects {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Rejects, false)).* from "./eager"
}
module real_raises {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Raises, false)).* from "./eager"
}
module real_cd_expires {
  import eagertypes.* from "./eager"
  import eager(CFG = { ...config(Returns, false), countdown: 3, expires: 1 }).* from "./eager"
}
module real_retry_expires {
  import eagertypes.* from "./eager"
  import eager(CFG = { ...config(RetryAlways, false), retryCountdown: 2, expires: 1 }).* from "./eager"
}
module eager_returns {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Returns, true)).* from "./eager"
}
module eager_retry_once {
  import eagertypes.* from "./eager"
  import eager(CFG = config(RetryOnce, true)).* from "./eager"
}
module eager_ignores {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Ignores, true)).* from "./eager"
}
module eager_rejects {
  import eagertypes.* from "./eager"
  import eager(CFG = config(Rejects, true)).* from "./eager"
}
module eager_cd_expires {
  import eagertypes.* from "./eager"
  import eager(CFG = { ...config(Returns, true), countdown: 3, expires: 1 }).* from "./eager"
}
module eager_retry_expires {
  import eagertypes.* from "./eager"
  import eager(CFG = { ...config(RetryAlways, true), retryCountdown: 2, expires: 1 }).* from "./eager"
}
