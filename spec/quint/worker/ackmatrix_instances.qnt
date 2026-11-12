// -*- mode: Bluespec; -*-
// Instances of ackmatrix.qnt: <ack mode>_<ending>[_<setting>][_<broker>].
// `early`: acks_late off; `late`: acks_late on. Settings: `noackfail`
// (task_acks_on_failure off), `rejectlost` (task_reject_on_worker_lost on).
// Broker: Redis unless `dlx` (RabbitMQ, a dead letter exchange on the queue).
//
// ---- instances: an ending, the ack settings and the broker -------------------
module early_returns {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Returns, false)).* from "./ackmatrix"
}
module early_raises {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Raises, false)).* from "./ackmatrix"
}
module early_retries {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Retries, false)).* from "./ackmatrix"
}
module early_rejectrequeue {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(RejectRequeue, false)).* from "./ackmatrix"
}
module early_rejectdrop {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(RejectDrop, false)).* from "./ackmatrix"
}
module early_ignores {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Ignores, false)).* from "./ackmatrix"
}
module early_lost {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Lost, false)).* from "./ackmatrix"
}
module late_returns {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Returns, true)).* from "./ackmatrix"
}
module late_raises {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Raises, true)).* from "./ackmatrix"
}
module late_retries {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Retries, true)).* from "./ackmatrix"
}
module late_rejectrequeue {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(RejectRequeue, true)).* from "./ackmatrix"
}
module late_rejectdrop {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(RejectDrop, true)).* from "./ackmatrix"
}
module late_ignores {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Ignores, true)).* from "./ackmatrix"
}
module late_lost {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = config(Lost, true)).* from "./ackmatrix"
}
module late_raises_noackfail {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(Raises, true), acksOnFailure: false }).* from "./ackmatrix"
}
module late_raises_noackfail_dlx {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(Raises, true), acksOnFailure: false, broker: RabbitDlx }).* from "./ackmatrix"
}
module late_lost_rejectlost {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(Lost, true), rejectOnLost: true }).* from "./ackmatrix"
}
module early_lost_rejectlost {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(Lost, false), rejectOnLost: true }).* from "./ackmatrix"
}
module late_rejectdrop_dlx {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(RejectDrop, true), broker: RabbitDlx }).* from "./ackmatrix"
}
module early_rejectdrop_dlx {
  import ackmatrixtypes.* from "./ackmatrix"
  import ackmatrix(CFG = { ...config(RejectDrop, false), broker: RabbitDlx }).* from "./ackmatrix"
}
