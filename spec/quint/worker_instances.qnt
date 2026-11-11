// -*- mode: Bluespec; -*-
// Instances of worker.qnt, written by gen_worker_instances.py:
// <design>_<ack mode>_<fault class>.
//
//   quint run --main=<instance> worker_instances.qnt --invariant=<property>
//
// ---- instances: a design, an ack mode and a fault class -------------------
module main_late_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, MAIN),
  }).* from "./worker"
}
module main_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, MAIN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module main_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, MAIN),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module main_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, MAIN),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module main_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, MAIN),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module main_early_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, MAIN),
  }).* from "./worker"
}
module main_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, MAIN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module main_early_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, MAIN),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
  }).* from "./worker"
}
module token_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module token_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module token_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module token_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, restoreBy: ByPop }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_early_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, restoreBy: ByPop }),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module tagrestore_late_etashutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module ownrecord_late_etashutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, restoreBy: ByPop }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
  }).* from "./worker"
}
module pr_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module pr_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module pr_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module pr_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, PR),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module pr_early_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
  }).* from "./worker"
}
module pr_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module pr_early_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module pr_early_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_early_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module pr_early_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, PR),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module keepwritten_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten }),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewAll }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewAll }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewAll }),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module renewall_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewAll }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewAll }),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, renewal: RenewAll }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_early_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, renewal: RenewAll }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewUnstarted }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewUnstarted }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewUnstarted }),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module renewidle_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewUnstarted }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module renewidle_early_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, renewal: RenewUnstarted }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewScheduled }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewScheduled }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewScheduled }),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module reneweta_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, renewal: RenewScheduled }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module resend_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent }),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, failedAck: Resent }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module ackfull_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks }),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks }),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, failedAck: Resent, collectedAck: StillAcks }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreall_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks, reconnectRestore: ReconnectRestoresAll }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreall_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks, reconnectRestore: ReconnectRestoresAll }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module restoreidle_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks, reconnectRestore: ReconnectRestoresIdle }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreidle_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks, reconnectRestore: ReconnectRestoresIdle }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module restoreidle_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, failedAck: Resent, collectedAck: StillAcks, reconnectRestore: ReconnectRestoresIdle }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
  }).* from "./worker"
}
module best_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module best_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module best_early_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
  }).* from "./worker"
}
module best_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module best_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, { ...PR, flush: DropUnwritten, renewal: RenewScheduled, failedAck: Resent, collectedAck: StillAcks, closeRestore: CloseRestoresIdle, reconnectRestore: ReconnectRestoresIdle, restoreBy: ByPop }),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
