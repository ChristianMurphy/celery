// -*- mode: Bluespec; -*-
// The projection of worker.qnt onto the abstract delivery environment of
// ../lib/delivery.qnt: every step of the detailed model must be a step of
// the environment (or none) on the broker's copies of each message.
//
// The projection: a message is ready when it is on the queue, and taken
// when the unacked hash has a record of it. A run of the projected model
// keeps the previous projection in `prev`, and `projectionOk` says that
// the change from `prev` to now is made of environment steps, one per
// message. The witnesses say which steps occur; `neverTaken` is the one
// that matters: a ready message that disappears without a record is main's
// lost BRPOP reply (kombu issue 305), which the environment allows only as
// a loss by the layer below.
//
// This is what lets the canvas specs (chain.qnt and later ones) assume the
// environment instead of the transport: what they find holds for any
// broker whose steps project this way, and worker.qnt shows Redis does,
// with the losses named.
module worker_projection {
  import workertypes.* from "./worker"
  import deliverytypes as DT from "../lib/delivery"

  const PCFG: Config
  import worker(CFG = PCFG).* from "./worker"

  def project: MsgId -> DT::Copies = MSGS.mapBy(m => {
    ready: if (queue.contains(m)) 1 else 0,
    taken: if (hasRec(unacked.get(m))) 1 else 0,
  })
  var prev: MsgId -> DT::Copies

  action pinit: bool = all {
    init, prev' = MSGS.mapBy(_ => DT::NO_COPIES),
  }
  action pstep: bool = all {
    step, prev' = project,
  }
  // The projection of the state after the step is checked against `prev`.
  val now: Set[DT::DeliveryStep] = DT::stepsOf(prev, project, MSGS)
  val projectionOk: bool = now.forall(s => not(DT::isBad(s)))

  // Witnesses (violated means reachable).
  val neverDelivered: bool = not(MSGS.exists(m => now.contains(DT::DDeliver(m))))
  val neverAcked: bool = not(MSGS.exists(m => now.contains(DT::DAck(m))))
  val neverRequeued: bool = not(MSGS.exists(m => now.contains(DT::DRequeue(m))))
  val neverTaken: bool = not(MSGS.exists(m => now.contains(DT::DTake(m))))
}

module proj_main_late_partition {
  import workertypes.* from "./worker"
  import worker_projection(PCFG = { ...config(Late, MAIN),
    max: { ...NONE, partitions: 1, restarts: 1 } }).* from "./worker_projection"
}
module proj_pr_late_cut {
  import workertypes.* from "./worker"
  import worker_projection(PCFG = { ...config(Late, PR),
    max: { ...NONE, cuts: 1, restarts: 1 } }).* from "./worker_projection"
}
module proj_pr_late_shutdown {
  import workertypes.* from "./worker"
  import worker_projection(PCFG = { ...config(Late, PR),
    max: { ...NONE, shutdowns: 1, restarts: 1 } }).* from "./worker_projection"
}
module proj_pr_early_crash {
  import workertypes.* from "./worker"
  import worker_projection(PCFG = { ...config(Early, PR),
    max: { ...NONE, crashes: 1, restarts: 2 } }).* from "./worker_projection"
}
module proj_pr_late_long {
  import workertypes.* from "./worker"
  import worker_projection(PCFG = { ...config(Late, PR),
    max: { ...NONE, timeouts: 1 } }).* from "./worker_projection"
}
