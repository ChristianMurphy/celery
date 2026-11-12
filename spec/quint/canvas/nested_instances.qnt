// -*- mode: Bluespec; -*-
// Instances of nested.qnt: <shape>_<what fails>_<how the errback is given>.
// Shapes: flat (a task), nested (a chord in the header), replaced (a task
// that replaces itself). Leaves 1 and 2 are X's (2 only in a nested
// chord), 3 is Y.
//
// ---- instances: shape, failing leaves, errback ------------------------------
module flat_ok_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Flat, AsOption, Set())).* from "./nested"
}
module flat_fail_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Flat, AsOption, Set(1))).* from "./nested"
}
module nested_ok_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Nested, AsOption, Set())).* from "./nested"
}
module nested_fail_onerror {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Nested, OnError, Set(2))).* from "./nested"
}
module nested_fail_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Nested, AsOption, Set(2))).* from "./nested"
}
module nested_yfail_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Nested, AsOption, Set(3))).* from "./nested"
}
module nested_bothfail_onerror {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Nested, OnError, Set(2, 3))).* from "./nested"
}
module replaced_ok_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Replaced, AsOption, Set())).* from "./nested"
}
module replaced_fail_option {
  import nesttypes.* from "./nested"
  import nested(CFG = config(Replaced, AsOption, Set(1))).* from "./nested"
}
