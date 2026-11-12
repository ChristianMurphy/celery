// -*- mode: Bluespec; -*-
// Instances of handlers.qnt: <first run's outcome>_<settings>. `default`:
// acks_late, acks_on_timeout on, reject_on_worker_lost off. `noackto`:
// task_acks_on_timeout off; `rejectlost`: task_reject_on_worker_lost on.
//
// ---- instances: an outcome and the ack settings ------------------------------
module returns_default {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = config(Returns)).* from "./handlers"
}
module raises_default {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = config(Raises)).* from "./handlers"
}
module hard_default {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = config(HardLimit)).* from "./handlers"
}
module hard_noackto {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = { ...config(HardLimit), acksOnTimeout: false }).* from "./handlers"
}
module lost_default {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = config(WorkerLost)).* from "./handlers"
}
module lost_rejectlost {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = { ...config(WorkerLost), rejectOnWorkerLost: true }).* from "./handlers"
}
module any_default {
  import handlerstypes.* from "./handlers"
  import handlers(CFG = { ...config(Returns), outcomes: Set(Returns, Raises, HardLimit, WorkerLost) }).* from "./handlers"
}
