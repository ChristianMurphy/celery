// -*- mode: Bluespec; -*-
// Instances of housekeeping.qnt: <settings>_<ending or client op>.
// `ignore`: ignore_result; `storeerr`: store_errors_even_if_ignored;
// `forget_running`: the client forgets the task at any time; `group_*`: a
// saved group of the task, deleted or forgotten, then restored.
//
// ---- instances: settings and the client ------------------------------------
module plain_returns {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(Returns, NoOp)).* from "./housekeeping"
}
module plain_retry {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(RetryThenReturns, NoOp)).* from "./housekeeping"
}
module plain_raises {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(Raises, NoOp)).* from "./housekeeping"
}
module ignore_retry {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = { ...config(RetryThenReturns, NoOp), ignoreResult: true }).* from "./housekeeping"
}
module ignore_storeerr_retry {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = { ...config(RetryThenReturns, NoOp), ignoreResult: true, storeErrors: true }).* from "./housekeeping"
}
module ignore_storeerr_raises {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = { ...config(Raises, NoOp), ignoreResult: true, storeErrors: true }).* from "./housekeeping"
}
module forget_running {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(Returns, ForgetTask)).* from "./housekeeping"
}
module forget_running_ignore {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = { ...config(Returns, ForgetTask), ignoreResult: true }).* from "./housekeeping"
}
module group_delete {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(Returns, GroupDelete)).* from "./housekeeping"
}
module group_forget {
  import housetypes.* from "./housekeeping"
  import housekeeping(CFG = config(Returns, GroupForget)).* from "./housekeeping"
}
