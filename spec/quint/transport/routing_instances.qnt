// -*- mode: Bluespec; -*-
// Instances of routing.qnt: <broker>_<target>[_nocreate][_confirm].
// `nocreate`: task_create_missing_queues off; `confirm`: confirm_publish on.
//
// ---- instances: broker and target -------------------------------------------
module redis_consumed {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, Consumed)).* from "./routing"
}
module redis_unconsumed {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, Unconsumed)).* from "./routing"
}
module redis_missing {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, Missing)).* from "./routing"
}
module redis_missing_nocreate {
  import routingtypes.* from "./routing"
  import routing(CFG = { ...config(Redis, Missing), createMissing: false }).* from "./routing"
}
module redis_unbound {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, Unbound)).* from "./routing"
}
module redis_undeclared {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, Undeclared)).* from "./routing"
}
module redis_deleted {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Redis, DeletedCached)).* from "./routing"
}
module rabbit_consumed {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Rabbit, Consumed)).* from "./routing"
}
module rabbit_missing_nocreate {
  import routingtypes.* from "./routing"
  import routing(CFG = { ...config(Rabbit, Missing), createMissing: false }).* from "./routing"
}
module rabbit_unbound {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Rabbit, Unbound)).* from "./routing"
}
module rabbit_unbound_confirm {
  import routingtypes.* from "./routing"
  import routing(CFG = { ...config(Rabbit, Unbound), confirm: true }).* from "./routing"
}
module rabbit_undeclared {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Rabbit, Undeclared)).* from "./routing"
}
module rabbit_undeclared_confirm {
  import routingtypes.* from "./routing"
  import routing(CFG = { ...config(Rabbit, Undeclared), confirm: true }).* from "./routing"
}
module rabbit_deleted {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Rabbit, DeletedCached)).* from "./routing"
}
module rabbit_ttl {
  import routingtypes.* from "./routing"
  import routing(CFG = config(Rabbit, TtlUnconsumed)).* from "./routing"
}
