// -*- mode: Bluespec; -*-
// Instances of priorities.qnt: <broker>_<messages>_p<prefetch>.
// `prio`: three messages on one queue with ranks 1, 0, 2 in publish order
// (Redis priorities 6, 9, 3 or 0 as the steps go; RabbitMQ 5, 0, 9);
// `queues`: four equal messages on queues A, B, A, A; `nomax`: a RabbitMQ
// queue without x-max-priority; `rr` and `fixed`: queue_order_strategy
// round_robin and priority.
//
// ---- instances: broker, messages and prefetch -------------------------------
module redis_prio_p1 {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Redis, prefetch: 1, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 1 }, { q: 0, rank: 0 }, { q: 0, rank: 2 }] }).* from "./priorities"
}
module redis_prio_p2 {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Redis, prefetch: 2, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 1 }, { q: 0, rank: 0 }, { q: 0, rank: 2 }] }).* from "./priorities"
}
module rabbit_prio_p1 {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Rabbit, prefetch: 1, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 1 }, { q: 0, rank: 0 }, { q: 0, rank: 2 }] }).* from "./priorities"
}
module rabbit_prio_p2 {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Rabbit, prefetch: 2, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 1 }, { q: 0, rank: 0 }, { q: 0, rank: 2 }] }).* from "./priorities"
}
module rabbit_nomax_p1 {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Rabbit, prefetch: 1, maxPriority: false,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 1 }, { q: 0, rank: 0 }, { q: 0, rank: 2 }] }).* from "./priorities"
}
module redis_queues_rr {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Redis, prefetch: 1, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 0 }, { q: 1, rank: 0 }, { q: 0, rank: 0 }, { q: 0, rank: 0 }] }).* from "./priorities"
}
module redis_queues_fixed {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Redis, prefetch: 1, maxPriority: true,
                             queueOrder: FixedOrder, msgs: [{ q: 0, rank: 0 }, { q: 1, rank: 0 }, { q: 0, rank: 0 }, { q: 0, rank: 0 }] }).* from "./priorities"
}
module rabbit_queues {
  import priotypes.* from "./priorities"
  import priorities(CFG = { broker: Rabbit, prefetch: 1, maxPriority: true,
                             queueOrder: RoundRobin, msgs: [{ q: 0, rank: 0 }, { q: 1, rank: 0 }, { q: 0, rank: 0 }, { q: 0, rank: 0 }] }).* from "./priorities"
}
