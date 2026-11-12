// -*- mode: Bluespec; -*-
// Instances of content.qnt: what does not fit, one thing at a time.
// ok: everything fits; args: arguments the task serializer cannot encode;
// zstd_oldworker: a compression the worker cannot decode; pickle_json: a
// pickle message to a json-only worker; result: a return value the result
// serializer cannot encode; reader: a reader whose result settings do not
// fit the stored result.
//
// ---- instances: what does not fit ------------------------------------------------
module ok {
  import contenttypes.* from "./content"
  import content(CFG = ALL_OK).* from "./content"
}
module args {
  import contenttypes.* from "./content"
  import content(CFG = { ...ALL_OK, argsEncodable: false }).* from "./content"
}
module zstd_oldworker {
  import contenttypes.* from "./content"
  import content(CFG = { ...ALL_OK, compressionKnown: false }).* from "./content"
}
module pickle_json {
  import contenttypes.* from "./content"
  import content(CFG = { ...ALL_OK, accepted: false }).* from "./content"
}
module result {
  import contenttypes.* from "./content"
  import content(CFG = { ...ALL_OK, resultEncodable: false }).* from "./content"
}
module reader {
  import contenttypes.* from "./content"
  import content(CFG = { ...ALL_OK, readerMatches: false }).* from "./content"
}
