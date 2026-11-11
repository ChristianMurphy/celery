// -*- mode: Bluespec; -*-
// Instances of chord.qnt: one module per configuration checked.
//
//   quint run --main=<instance> chord_instances.qnt --invariant=<property>
//
// ---- instances: one per protocol and fault class -------------------------
module redis_none {
  import chordtypes.* from "./chord"
  import chord(CFG = lateAck(Redis)).* from "./chord"
}
module redis_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Redis), max: { ...NO_FAULTS, dups: 2 } }).* from "./chord"
}
module redis_refuse {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    max: { ...NO_FAULTS, refusals: 1 },
  }).* from "./chord"
}
module redis_wlost_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module redis_wlost_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Redis),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module redis_child_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module redis_child_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Redis),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module redis_child_reject {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    rejectOnWorkerLost: true,
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module redis_lease {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    leaseSafe: false,
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"
}
module kv_none {
  import chordtypes.* from "./chord"
  import chord(CFG = lateAck(Kv)).* from "./chord"
}
module kv_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Kv), max: { ...NO_FAULTS, dups: 2 } }).* from "./chord"
}
module kv_refuse {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Kv), max: { ...NO_FAULTS, refusals: 1 } }).* from "./chord"
}
module kv_wlost_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module kv_wlost_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Kv),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module kv_child_late {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Kv), max: { ...NO_FAULTS, childLost: 1 } }).* from "./chord"
}
module kv_child_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Kv),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module kv_child_reject {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    rejectOnWorkerLost: true,
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module unlock_none {
  import chordtypes.* from "./chord"
  import chord(CFG = lateAck(Unlock)).* from "./chord"
}
module unlock_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Unlock), max: { ...NO_FAULTS, dups: 2 } }).* from "./chord"
}
module unlock_refuse {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Unlock),
    max: { ...NO_FAULTS, refusals: 1 },
  }).* from "./chord"
}
module unlock_wlost_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Unlock),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module unlock_wlost_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Unlock),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module unlock_child_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Unlock),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module unlock_child_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(Unlock),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module unlock_child_reject {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Unlock),
    rejectOnWorkerLost: true,
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}

// ---- celery issue 11036: the counting backends ----------------------------
// once_*    the fix (1fa71d4), under the same eight fault classes as kv_*
module once_none {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
  }).* from "./chord"
}
module once_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, dups: 2 },
  }).* from "./chord"
}
module once_refuse {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, refusals: 1 },
  }).* from "./chord"
}
module once_wlost_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module once_wlost_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(KvOnce),
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module once_child_late {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module once_child_early {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...earlyAck(KvOnce),
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
module once_child_reject {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    rejectOnWorkerLost: true,
    max: { ...NO_FAULTS, childLost: 1 },
  }).* from "./chord"
}
// A worker killed between a header's result (or its key) and its count,
// with acks_late: main counts the copy, KvSkip skips it, the fix counts it.
module kv_kill_before_count {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    killWindow: BeforeCount,
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module skip_kill_before_count {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvSkip),
    killWindow: BeforeCount,
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module once_kill_before_count {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    killWindow: BeforeCount,
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
// A worker killed between taking the dispatch and the publish (documented
// as remaining: the body is lost, as on main).
module kv_kill_after_claim {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    killWindow: AfterClaim,
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
module once_kill_after_claim {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    killWindow: AfterClaim,
    max: { ...NO_FAULTS, workerLost: 1 },
  }).* from "./chord"
}
// KvSkip under no fault and under duplicates.
module skip_none {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvSkip),
  }).* from "./chord"
}
module skip_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvSkip),
    max: { ...NO_FAULTS, dups: 2 },
  }).* from "./chord"
}
// A header task with ignore_result=True.
module kv_ignore {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    ignoreResult: Set(H(2)),
  }).* from "./chord"
}
module once_ignore {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    ignoreResult: Set(H(2)),
  }).* from "./chord"
}
// One key expires or is evicted.
module kv_lose_result {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    evictable: Set(ResultKey(H(1)), ResultKey(H(2))),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module kv_lose_group {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    evictable: Set(GroupKey),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module kv_lose_counter {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Kv),
    evictable: Set(CounterKey),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module once_lose_result {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(ResultKey(H(1)), ResultKey(H(2))),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module once_lose_group {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(GroupKey),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module once_lose_counter {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(CounterKey),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module once_lose_partkey {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(PartKey(H(1)), PartKey(H(2))),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
module once_lose_done {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(DoneKey),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}
// Pairs of faults.
module once_dup_wlost {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    max: { ...NO_FAULTS, dups: 1, workerLost: 1 },
  }).* from "./chord"
}
module once_dup_lose_result {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(ResultKey(H(1)), ResultKey(H(2))),
    max: { ...NO_FAULTS, dups: 1, evictions: 1 },
  }).* from "./chord"
}
module once_dup_lose_done {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(DoneKey),
    max: { ...NO_FAULTS, dups: 1, evictions: 1 },
  }).* from "./chord"
}
// A header result and its key both gone (the keys expiring with the
// results, as before 60517d3).
module once_lose_result_and_key {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    evictable: Set(ResultKey(H(1)), PartKey(H(1))),
    max: { ...NO_FAULTS, evictions: 2 },
  }).* from "./chord"
}
// cd8aa46: the missing results read once (the race found by this spec).
module oncev1_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    rereadMissing: false,
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"
}
// A rolling upgrade: process 3 belongs to a worker that still runs Kv.
module mixed_none {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    oldExecs: Set(W(3)),
  }).* from "./chord"
}
module mixed_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    oldExecs: Set(W(3)),
    max: { ...NO_FAULTS, dups: 1 },
  }).* from "./chord"
}
module mixed_lose_result {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(KvOnce),
    oldExecs: Set(W(3)),
    evictable: Set(ResultKey(H(1)), ResultKey(H(2))),
    max: { ...NO_FAULTS, evictions: 1 },
  }).* from "./chord"
}

// ---- celery issue 11039: an interrupted dispatch (redis) ------------------
// redis_*  768e476   redisfix1_*  5f844ea   redisfix2_*  5446659
// redisfix3_*  ee48301   redisfix_*  ad00b4a
module redis_interrupt {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: Unhandled,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
module redisfix1_interrupt {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailWhileDispatching,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
module redisfix2_interrupt {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailUntilSent,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
module redisfix3_interrupt {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailAndClose,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
module redisfix_interrupt {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: CloseAlways,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
// With a copy of a header task that arrives after the interrupt.
module redis_interrupt_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: Unhandled,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"
}
module redisfix2_interrupt_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailUntilSent,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"
}
module redisfix3_interrupt_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: FailAndClose,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"
}
module redisfix_interrupt_dup {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: CloseAlways,
    interruptAt: Set(InJoin, InExtend, InPublishUnsent, InCleanup),
    max: { ...NO_FAULTS, interrupts: 1, dups: 1 },
  }).* from "./chord"
}
// What the fix leaves: an interrupt while the body may already be out, and
// one in the reply of the claim script.
module redisfix_interrupt_sent {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: CloseAlways,
    interruptAt: Set(InPublishSent),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
module redisfix_interrupt_reply {
  import chordtypes.* from "./chord"
  import chord(CFG = {
    ...lateAck(Redis),
    onInterrupt: CloseAlways,
    interruptAt: Set(InClaimReply),
    max: { ...NO_FAULTS, interrupts: 1 },
  }).* from "./chord"
}
