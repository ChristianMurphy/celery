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
    ...config(Late, TOKEN),
  }).* from "./worker"
}
module token_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module token_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module token_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TOKEN),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module token_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, TOKEN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module token_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, TOKEN),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module token_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, TOKEN),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module token_early_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, TOKEN),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module tagrestore_late_etashutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, TAGRESTORE),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module ownrecord_late_etashutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, OWNRECORD),
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
    ...config(Late, KEEPWRITTEN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, KEEPWRITTEN),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, KEEPWRITTEN),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, KEEPWRITTEN),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module keepwritten_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, KEEPWRITTEN),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWALL),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWALL),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWALL),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module renewall_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWALL),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWALL),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module renewall_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, RENEWALL),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewall_early_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, RENEWALL),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWIDLE),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWIDLE),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module renewidle_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWIDLE),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module renewidle_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWIDLE),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module renewidle_early_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, RENEWIDLE),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWETA),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWETA),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module reneweta_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWETA),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module reneweta_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RENEWETA),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESEND),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESEND),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module resend_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESEND),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module resend_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, RESEND),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, ACKFULL),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, ACKFULL),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module ackfull_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, ACKFULL),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, ACKFULL),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module ackfull_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, ACKFULL),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreall_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESTOREALL),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreall_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESTOREALL),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module restoreidle_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESTOREIDLE),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module restoreidle_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESTOREIDLE),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module restoreidle_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, RESTOREIDLE),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
  }).* from "./worker"
}
module best_late_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_late_long {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_late_kill {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, kills: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_crash {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, crashes: 1, restarts: 2 },
  }).* from "./worker"
}
module best_late_partition {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, partitions: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_cutlong {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, cuts: 1, timeouts: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_reset {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, resets: 1, restarts: 1 },
  }).* from "./worker"
}
module best_late_hang {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Late, BEST),
    max: { ...NONE, hangs: 1 },
  }).* from "./worker"
}
module best_early_none {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, BEST),
  }).* from "./worker"
}
module best_early_shutdown {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, BEST),
    max: { ...NONE, shutdowns: 1, restarts: 1 },
  }).* from "./worker"
}
module best_early_eta {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, BEST),
    eta: Set(M(1)), outlasts: OnlyEta,
    max: { ...NONE, timeouts: 1 },
  }).* from "./worker"
}
module best_early_cut {
  import workertypes.* from "./worker"
  import worker(CFG = {
    ...config(Early, BEST),
    max: { ...NONE, cuts: 1, restarts: 1 },
  }).* from "./worker"
}
