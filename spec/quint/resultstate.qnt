// -*- mode: Bluespec; -*-
// Result state for one task id, as Celery's result backends store it.
//
// Every rule cites celery at 768e476 (file:line). The store operation is
// split in the two steps the code performs, so interleavings between
// duplicate runs of the task are explicit:
//
//   begin(r, s)   the run r starts store_result(task_id, ..., s) and does the
//                 read the backend does before writing (if any).
//   commit(r)     the run's write reaches the store.
//
// STORE selects how a backend implements the write:
//
//   "rtw"     BaseKeyValueStoreBackend._store_result: read, then an
//             unconditional set (redis, memcached, couchdb, cassandra,
//             arangodb, consul, ...). base.py:1355-1377
//   "mongo"   replace_one with filter status $ne SUCCESS, one atomic
//             operation. mongodb.py:189-217
//   "sql"     unlocked read, then SELECT ... FOR UPDATE, check, update.
//             database/__init__.py:169-197
//   "es"      base read, then create, or get + update with if_seq_no
//             (optimistic CAS) that also refuses ready -> unready.
//             elasticsearch.py:128-230
//
// READY_STICKY: the store refuses to replace a ready state (SUCCESS, FAILURE,
// REVOKED) with an unready one (STARTED, RETRY). Only "es" does this
// (elasticsearch.py:195); it is a parameter so the property can be checked
// against every store.
module resultstate {
  const STORE: str
  const READY_STICKY: bool

  // states.py:144-145
  pure val READY = Set("SUCCESS", "FAILURE", "REVOKED")
  pure val UNREADY = Set("PENDING", "RECEIVED", "STARTED", "REJECTED", "RETRY")
  // What a duplicate run of a task writes: trace.py marks STARTED
  // (track_started), then RETRY, SUCCESS or FAILURE.
  pure val WRITES = Set("STARTED", "RETRY", "SUCCESS", "FAILURE")

  pure val RUNS = Set(1, 2)
  pure val MAX_WRITES = 2
  pure val MAX_FORGETS = 1

  type Meta = { status: str, v: int }
  pure val NOTHING: Meta = { status: "PENDING", v: 0 }

  // What is stored for the task id. NOTHING = no key / no row / no document,
  // which every backend reads back as PENDING (base.py:1390,
  // mongodb.py:251, database/__init__.py:245-247).
  var stored: Meta
  // Per run: "idle" or "begun", what it is writing, and what it read.
  var pc: int -> str
  var pending: int -> Meta
  var seen: int -> str
  var writes: int -> int
  var forgets: int
  // A client that saw SUCCESS (AsyncResult.get / get_task_meta).
  var observed: { has: bool, meta: Meta }
  // The stored meta before the last step, for the two-state property.
  var prev: Meta
  // The action that produced this state, read by the replay adapter.
  var lastAction: { kind: str, rn: int, status: str, v: int, outcome: str }

  action init = all {
    stored' = NOTHING,
    pc' = RUNS.mapBy(_ => "idle"),
    pending' = RUNS.mapBy(_ => NOTHING),
    seen' = RUNS.mapBy(_ => "PENDING"),
    writes' = RUNS.mapBy(_ => 0),
    forgets' = 0,
    observed' = { has: false, meta: NOTHING },
    prev' = NOTHING,
    lastAction' = { kind: "init", rn: 0, status: "", v: 0, outcome: "" },
  }

  // ---- begin: the read a backend does before it writes -----------------
  // rtw:   current_meta = self._get_task_meta_for(task_id)   base.py:1367
  // es:    the same base read (ES inherits _store_result)    base.py:1367
  // sql:   task = self._query_task(session, task_id), unlocked
  //                                                  database/__init__.py:174
  // mongo: no read; the filter does the check at write time mongodb.py:207
  action begin(r: int, s: str): bool = all {
    pc.get(r) == "idle",
    writes.get(r) < MAX_WRITES,
    pc' = pc.set(r, "begun"),
    pending' = pending.set(r, { status: s, v: r }),
    seen' = seen.set(r, stored.status),
    writes' = writes.set(r, writes.get(r) + 1),
    stored' = stored,
    forgets' = forgets,
    observed' = observed,
    prev' = stored,
    lastAction' = { kind: "begin", rn: r, status: s, v: r, outcome: "" },
  }

  // What the write does to the store, given what the run read at begin.
  // Returns the new stored meta and an outcome label.
  pure def commitEffect(store: str, sticky: bool, cur: Meta, read: str,
                        w: Meta): (Meta, str) =
    // READY_STICKY as a store rule: elasticsearch.py:195-197
    val stickyRefuses = sticky and READY.contains(cur.status)
                        and UNREADY.contains(w.status)
    if (store == "rtw")
      // if current_meta['status'] == SUCCESS: return        base.py:1369-1370
      // else self._set_with_state(key, encode(meta), state)  base.py:1373
      //   -> redis _set: SETEX/SET, unconditional           redis.py:816-825
      if (read == "SUCCESS") (cur, "skipped")
      else if (stickyRefuses) (cur, "skipped")
      else (w, "written")
    else if (store == "mongo")
      // replace_one({'_id': id, 'status': {'$ne': SUCCESS}}, upsert=True)
      //                                                    mongodb.py:207-210
      // a stored SUCCESS makes the upsert raise DuplicateKeyError, ignored
      //                                                    mongodb.py:212-213
      if (cur.status == "SUCCESS") (cur, "skipped")
      else if (stickyRefuses) (cur, "skipped")
      else (w, "written")
    else if (store == "sql")
      // row seen at begin: lock it and read again   database/__init__.py:175-180
      if (read != "PENDING")
        if (cur == NOTHING) (w, "written")           // row gone: insert :181-185
        else if (cur.status == "SUCCESS") (cur, "skipped") // :186-190
        else if (stickyRefuses) (cur, "skipped")
        else (w, "written")                          // :192-197
      // no row at begin: INSERT; a concurrent insert wins the unique
      // task_id constraint (models.py:34), and this run's commit raises
      else if (cur == NOTHING) (w, "written")        // :181-185
      else (cur, "error")
    else // "es"
      // base check on the earlier read                   base.py:1369
      if (read == "SUCCESS") (cur, "skipped")
      // index op_type=create succeeds if no document  elasticsearch.py:135-139
      else if (cur == NOTHING) (w, "written")
      // ConflictError -> _update: get, check, update if_seq_no
      //                                         elasticsearch.py:140-142,172-221
      else if (cur.status == "SUCCESS") (cur, "skipped")       // :192-194
      else if (READY.contains(cur.status) and UNREADY.contains(w.status))
        (cur, "skipped")                                       // :195-197
      else (w, "written")

  action commit(r: int): bool = all {
    pc.get(r) == "begun",
    val eff = commitEffect(STORE, READY_STICKY, stored, seen.get(r), pending.get(r))
    all {
      stored' = eff._1,
      lastAction' = { kind: "commit", rn: r, status: pending.get(r).status,
                      v: r, outcome: eff._2 },
    },
    pc' = pc.set(r, "idle"),
    pending' = pending,
    seen' = seen,
    writes' = writes,
    forgets' = forgets,
    observed' = observed,
    prev' = stored,
  }

  // forget(): BaseBackend.forget base.py:883-885 ->
  //   rtw/es: self.delete(key)                    base.py:1352-1353
  //   mongo:  delete_one({'_id': task_id})        mongodb.py:297
  //   sql:    DELETE ... WHERE task_id = id       database/__init__.py:352-353
  // A client that forgets a result drops what it saw.
  action forget: bool = all {
    forgets < MAX_FORGETS,
    stored' = NOTHING,
    forgets' = forgets + 1,
    observed' = { has: false, meta: NOTHING },
    pc' = pc, pending' = pending, seen' = seen, writes' = writes,
    prev' = stored,
    lastAction' = { kind: "forget", rn: 0, status: "", v: 0, outcome: "" },
  }

  // A client reads the result and sees SUCCESS (get_task_meta base.py,
  // _get_task_meta_for above).
  action observe: bool = all {
    stored.status == "SUCCESS",
    not(observed.has),
    observed' = { has: true, meta: stored },
    stored' = stored, pc' = pc, pending' = pending, seen' = seen,
    writes' = writes, forgets' = forgets,
    prev' = stored,
    lastAction' = { kind: "observe", rn: 0, status: stored.status,
                    v: stored.v, outcome: "" },
  }

  action step = {
    nondet r = RUNS.oneOf()
    nondet s = WRITES.oneOf()
    any { begin(r, s), commit(r), forget, observe }
  }

  // ---- properties -------------------------------------------------------
  // Once a reader saw SUCCESS v, the store keeps SUCCESS v until forget().
  // Stated in base.py:1362-1366 and by the "keep the first SUCCESS" fixes:
  // celery PR 10920 (database), 10944 (mongodb), 10945 (cassandra).
  val successStable = observed.has implies stored == observed.meta

  // A ready state is never replaced by an unready one (except by forget).
  // Only Elasticsearch promises this (elasticsearch.py:166-176).
  val readySticky =
    not(lastAction.kind == "commit" and READY.contains(prev.status)
        and UNREADY.contains(stored.status))

  // Sanity: the spec can reach a stored SUCCESS (witness, should be violated)
  val noSuccessEver = stored.status != "SUCCESS"
}

module rtw {
  import resultstate(STORE = "rtw", READY_STICKY = false).*
}
module mongo {
  import resultstate(STORE = "mongo", READY_STICKY = false).*
}
module sql {
  import resultstate(STORE = "sql", READY_STICKY = false).*
}
module es {
  import resultstate(STORE = "es", READY_STICKY = true).*
}
