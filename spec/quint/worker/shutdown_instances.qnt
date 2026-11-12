// -*- mode: Bluespec; -*-
// Instances of shutdown.qnt: the signals delivered while task A runs and
// task B waits, in order. `pym`: the worker was started as
// `python -m celery` (the others: the `celery` console script).
//
// ---- instances: the signals ---------------------------------------------------
module term {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Term])).* from "./shutdown"
}
module term2 {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Term, Term])).* from "./shutdown"
}
module int2 {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Intr, Intr])).* from "./shutdown"
}
module quit {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Quit])).* from "./shutdown"
}
module term_quit {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Term, Quit])).* from "./shutdown"
}
module hup {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = config([Hup])).* from "./shutdown"
}
module hup_pym {
  import sdtypes.* from "./shutdown"
  import shutdown(CFG = { ...config([Hup]), launch: PythonM }).* from "./shutdown"
}
