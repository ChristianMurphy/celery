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
//   Rtw       BaseKeyValueStoreBackend._store_result: read, then an
//             unconditional set (redis, memcached, couchdb, cassandra,
//             arangodb, consul, ...). base.py:1355-1377
//   Mongo     replace_one with filter status $ne SUCCESS, one atomic
//             operation. mongodb.py:189-217
//   Sql       unlocked read, then SELECT ... FOR UPDATE, check, update.
//             database/__init__.py:169-197
//   Es        base read, then create, or get + update with if_seq_no
//             (optimistic CAS) that also refuses ready -> unready.
//             elasticsearch.py:128-230
//
// READY_STICKY: the store refuses to replace a ready state (SUCCESS, FAILURE,
// REVOKED) with an unready one (STARTED, RETRY). Only Es does this
// (elasticsearch.py:195); it is a parameter so the property can be checked
// against every store.
//
// Types: states, stores, outcomes and actions are sum types, so a misspelled
// state or an action with another action's fields does not typecheck. They
// live in their own module because an instance passes a constructor
// (STORE = Rtw), which must be in scope where the instance is made.
module resulttypes {
  // states.py:20-44
  type Status = Pending | Received | Started | Rejected | Retry | Success
              | Failure | Revoked
  // states.py:144-145
  pure def isReady(s: Status): bool = match s {
    | Success => true | Failure => true | Revoked => true | _ => false
  }
  pure def isUnready(s: Status): bool = not(isReady(s))

  type Store = Rtw | Mongo | Sql | Es
  // One of the duplicate runs of the task: a one-field record, not a bare
  // int, so a run id does not typecheck as a count or the other way round.
  // (A one-variant sum type would read the same, but Quint 0.33.1 gives it
  // an open row type that Apalache and TLC reject.)
  type RunId = { rn: int }
  pure def R(i: int): RunId = { rn: i }
  type Meta = { status: Status, by: RunId }
  // What is stored for the task id. Absent = no key / no row / no document,
  // which every backend reads back as PENDING (base.py:1390,
  // mongodb.py:251, database/__init__.py:245-247).
  type Stored = Absent | Present(Meta)
  pure def statusOf(s: Stored): Status = match s {
    | Absent => Pending
    | Present(m) => m.status
  }

  // A run is idle, or has begun a store: what it writes, and what it read.
  type Pc = Idle | Begun({ writing: Status, read: Status })
  pure def isBegun(p: Pc): bool = match p { | Begun(_) => true | Idle => false }
  pure val NOT_BEGUN: { writing: Status, read: Status } =
    { writing: Pending, read: Pending }
  // Only read under the guard isBegun(p).
  pure def begunOf(p: Pc): { writing: Status, read: Status } = match p {
    | Begun(b) => b
    | Idle => NOT_BEGUN
  }

  // What a commit did: wrote, kept what was stored, or raised.
  type Outcome = Written | Skipped | Raised
  // A client that saw SUCCESS (AsyncResult.get / get_task_meta).
  type Observed = NotObserved | Saw(Meta)
  // The action that produced a state, read by the replay adapter.
  type Action =
    | Init
    | Begin({ rn: RunId, status: Status })
    | Commit({ rn: RunId, status: Status, outcome: Outcome })
    | Forget
    | Observe(Meta)
  pure def isCommit(a: Action): bool = match a {
    | Commit(_) => true | _ => false
  }
}

module resultstate {
  import resulttypes.*

  const STORE: Store
  const READY_STICKY: bool
  // A run may also store REVOKED: a worker that discards the task
  // (_announce_revoked, request.py:464-473) or the revoke command
  // (control.py:236-244) calls mark_as_revoked, which goes through the
  // same store_result (base.py:445-451). ../worker/revoke.qnt has when.
  const REVOKES: bool
  // Design: the write refuses to replace any ready state with another
  // state (on Mongo a filter `status $nin READY_STATES`, one atomic
  // operation). Not in celery.
  const FINAL_STICKY: bool

  // What a duplicate run of a task writes: trace.py marks STARTED
  // (track_started), then RETRY, SUCCESS or FAILURE.
  // @cite celery celery/backends/base.py:445-451 "self.store_result(task_id, exc, state,"
  pure val WRITES: Set[Status] =
    if (REVOKES) Set(Started, Retry, Success, Failure, Revoked)
    else Set(Started, Retry, Success, Failure)

  pure val RUNS: Set[RunId] = Set(R(1), R(2))
  pure val MAX_WRITES: int = 2
  pure val MAX_FORGETS: int = 1

  // ---- state ----
  var stored: Stored
  var pc: RunId -> Pc
  var writes: RunId -> int
  var forgets: int
  // ---- observations ----
  var observed: Observed
  // What was stored before the last step, for the two-state property.
  var prev: Stored
  var lastAction: Action
  // Number of actions taken: makes "every run ends within D steps" an
  // invariant (steps <= D).
  var steps: int

  // Every action records itself and counts one step.
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  action init: bool = all {
    stored' = Absent,
    pc' = RUNS.mapBy(_ => Idle),
    writes' = RUNS.mapBy(_ => 0),
    forgets' = 0,
    observed' = NotObserved,
    prev' = Absent,
    lastAction' = Init, steps' = 0,
  }

  // ---- actions ----
  // begin: the read a backend does before it writes.
  // rtw:   current_meta = self._get_task_meta_for(task_id)   base.py:1367
  // es:    the same base read (ES inherits _store_result)    base.py:1367
  // sql:   task = self._query_task(session, task_id), unlocked
  //                                                  database/__init__.py:174
  // mongo: no read; the filter does the check at write time mongodb.py:207
  // @cite celery celery/backends/base.py:1367 "current_meta = self._get_task_meta_for(task_id)"
  // @cite celery celery/backends/database/__init__.py:174 "task = self._query_task(session, task_id)"
  action begin(r: RunId, s: Status): bool = all {
    pc.get(r) == Idle,
    writes.get(r) < MAX_WRITES,
    pc' = pc.set(r, Begun({ writing: s, read: statusOf(stored) })),
    writes' = writes.set(r, writes.get(r) + 1),
    stored' = stored,
    forgets' = forgets,
    observed' = observed,
    prev' = stored,
    did(Begin({ rn: r, status: s })),
  }

  // What the write does to the store, given what the run read at begin.
  // Returns the new stored value and what the commit did.
  pure def commitEffect(store: Store, sticky: bool, finalSticky: bool,
                        cur: Stored, read: Status, w: Meta): (Stored, Outcome) =
    val curStatus = statusOf(cur)
    // READY_STICKY as a store rule: elasticsearch.py:195-197
    val stickyRefuses = (sticky and isReady(curStatus) and isUnready(w.status))
      or (finalSticky and isReady(curStatus) and curStatus != w.status)
    match store {
      | Rtw =>
        // if current_meta['status'] == SUCCESS: return        base.py:1369-1370
        // else self._set_with_state(key, encode(meta), state)  base.py:1373
        //   -> redis _set: SETEX/SET, unconditional           redis.py:816-825
        if (read == Success) (cur, Skipped)
        else if (stickyRefuses) (cur, Skipped)
        else (Present(w), Written)
      | Mongo =>
        // replace_one({'_id': id, 'status': {'$ne': SUCCESS}}, upsert=True)
        //                                                    mongodb.py:207-210
        // a stored SUCCESS makes the upsert raise DuplicateKeyError, ignored
        //                                                    mongodb.py:212-213
        if (curStatus == Success) (cur, Skipped)
        else if (stickyRefuses) (cur, Skipped)
        else (Present(w), Written)
      | Sql =>
        // row seen at begin: lock it and read again   database/__init__.py:175-180
        if (read != Pending)
          if (cur == Absent) (Present(w), Written)     // row gone: insert :181-185
          else if (curStatus == Success) (cur, Skipped) // :186-190
          else if (stickyRefuses) (cur, Skipped)
          else (Present(w), Written)                   // :192-197
        // no row at begin: INSERT; a concurrent insert wins the unique
        // task_id constraint (models.py:34), and this run's commit raises
        else if (cur == Absent) (Present(w), Written)  // :181-185
        else (cur, Raised)
      | Es =>
        // base check on the earlier read                   base.py:1369
        if (read == Success) (cur, Skipped)
        // index op_type=create succeeds if no document  elasticsearch.py:135-139
        else if (cur == Absent) (Present(w), Written)
        // ConflictError -> _update: get, check, update if_seq_no
        //                                         elasticsearch.py:140-142,172-221
        else if (curStatus == Success) (cur, Skipped)            // :192-194
        else if (isReady(curStatus) and isUnready(w.status))
          (cur, Skipped)                                         // :195-197
        else (Present(w), Written)
    }

  // commit: the write, as commitEffect says per store.
  // @cite celery celery/backends/base.py:1369-1373 "if current_meta['status'] == states.SUCCESS:"
  // @cite celery celery/backends/mongodb.py:207-210 "query = {'_id': task_id, 'status': {'$ne': states.SUCCESS}}"
  // @cite celery celery/backends/database/__init__.py:175-180 "task = self._query_task(session, task_id, for_update=True)"
  // @cite celery celery/backends/elasticsearch.py:135-142 "except elasticsearch.exceptions.ConflictError:"
  action commit(r: RunId): bool = {
    val b = begunOf(pc.get(r))
    val eff = commitEffect(STORE, READY_STICKY, FINAL_STICKY, stored, b.read,
                           { status: b.writing, by: r })
    all {
      isBegun(pc.get(r)),
      stored' = eff._1,
      did(Commit({ rn: r, status: b.writing, outcome: eff._2 })),
      pc' = pc.set(r, Idle),
      writes' = writes,
      forgets' = forgets,
      observed' = observed,
      prev' = stored,
    }
  }

  // forget(): BaseBackend.forget base.py:883-885 ->
  //   rtw/es: self.delete(key)                    base.py:1352-1353
  //   mongo:  delete_one({'_id': task_id})        mongodb.py:297
  //   sql:    DELETE ... WHERE task_id = id       database/__init__.py:352-353
  // A client that forgets a result drops what it saw.
  // @cite celery celery/backends/base.py:883-885 "def forget(self, task_id):"
  action forget: bool = all {
    forgets < MAX_FORGETS,
    stored' = Absent,
    forgets' = forgets + 1,
    observed' = NotObserved,
    pc' = pc, writes' = writes,
    prev' = stored,
    did(Forget),
  }

  pure val NO_META: Meta = { status: Pending, by: R(0) }
  // Only read under the guard stored != Absent.
  pure def metaOf(s: Stored): Meta = match s {
    | Present(m) => m
    | Absent => NO_META
  }

  // A client reads the result and sees SUCCESS (get_task_meta base.py,
  // _get_task_meta_for above).
  // @cite celery celery/backends/base.py:934-956 "def get_task_meta(self, task_id, cache=True):"
  action observe: bool = all {
    statusOf(stored) == Success,
    observed == NotObserved,
    observed' = Saw(metaOf(stored)),
    stored' = stored, pc' = pc, writes' = writes, forgets' = forgets,
    prev' = stored,
    did(Observe(metaOf(stored))),
  }

  // ---- faults ----
  // None of their own: the scenario is two duplicate runs of one task (a
  // redelivery, a lost worker) whose writes interleave, and a forget().

  // ---- step ----
  action step: bool = any {
    {
      nondet r = RUNS.oneOf()
      nondet s = WRITES.oneOf()
      begin(r, s)
    },
    {
      nondet r = RUNS.oneOf()
      commit(r)
    },
    forget, observe,
  }

  // ---- properties -------------------------------------------------------
  // invariants
  // Once a reader saw SUCCESS by a run, the store keeps that until forget().
  // Stated in base.py:1362-1366 and by the "keep the first SUCCESS" fixes:
  // celery PR 10920 (database), 10944 (mongodb), 10945 (cassandra).
  val successStable: bool = match observed {
    | NotObserved => true
    | Saw(m) => stored == Present(m)
  }

  // A ready state is never replaced by an unready one (except by forget).
  // Only Elasticsearch promises this (elasticsearch.py:166-176).
  val readySticky: bool =
    not(isCommit(lastAction) and isReady(statusOf(prev))
        and isUnready(statusOf(stored)))

  // A ready state is never replaced by another state (except by forget):
  // stored states move one way. Every store keeps SUCCESS only (and Es
  // refuses ready -> unready): REVOKED replaces FAILURE, and SUCCESS
  // replaces REVOKED or FAILURE, on all four.
  val finalStable: bool =
    not(isCommit(lastAction) and isReady(statusOf(prev))
        and statusOf(stored) != statusOf(prev))

  // witnesses (violated means reachable)
  // Sanity: the spec can reach a stored SUCCESS
  val noSuccessEver: bool = statusOf(stored) != Success

  // progress measure (lemma 1 of the liveness argument, ../README.md)
  // Each action lowers "measure" (a begin uses up a write, a commit ends a
  // store, forget and observe happen a bounded number of times), so
  // steps + measure never exceeds its initial value. The exact longest run
  // D is measured by exhaustive search and checked as steps <= D.
  val measure: int =
    RUNS.fold(0, (s, r) => s + 2 * (MAX_WRITES - writes.get(r))
                           + (if (isBegun(pc.get(r))) 1 else 0))
    + 3 * (MAX_FORGETS - forgets)
    + (if (observed == NotObserved) 1 else 0)
  pure val MEASURE0: int = 2 * MAX_WRITES * RUNS.size() + 3 * MAX_FORGETS + 1
  val progress: bool = measure >= 0 and steps + measure <= MEASURE0

  // liveness (lemma 2; lemma 3 is the quiet state itself: every run wrote
  // what it had to)
  // Quiet: every run wrote all it had to write. A run stops only then: the
  // exhaustive search runs stepOrQuiet and reports any other state without
  // a successor as a deadlock.
  val quiet: bool =
    RUNS.forall(r => pc.get(r) == Idle and writes.get(r) == MAX_WRITES)
  action stutter: bool = all {
    quiet,
    stored' = stored, pc' = pc, writes' = writes, forgets' = forgets,
    observed' = observed, prev' = prev, lastAction' = lastAction,
    steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
}

// ---- instances: one per store --------------------------------------------
module rtw {
  import resulttypes.*
  import resultstate(STORE = Rtw, READY_STICKY = false, REVOKES = false, FINAL_STICKY = false).*
}
module mongo {
  import resulttypes.*
  import resultstate(STORE = Mongo, READY_STICKY = false, REVOKES = false, FINAL_STICKY = false).*
}
module sql {
  import resulttypes.*
  import resultstate(STORE = Sql, READY_STICKY = false, REVOKES = false, FINAL_STICKY = false).*
}
module es {
  import resulttypes.*
  import resultstate(STORE = Es, READY_STICKY = true, REVOKES = false, FINAL_STICKY = false).*
}
module rtw_rev {
  import resulttypes.*
  import resultstate(STORE = Rtw, READY_STICKY = false, REVOKES = true, FINAL_STICKY = false).*
}
module mongo_rev {
  import resulttypes.*
  import resultstate(STORE = Mongo, READY_STICKY = false, REVOKES = true, FINAL_STICKY = false).*
}
module sql_rev {
  import resulttypes.*
  import resultstate(STORE = Sql, READY_STICKY = false, REVOKES = true, FINAL_STICKY = false).*
}
module es_rev {
  import resulttypes.*
  import resultstate(STORE = Es, READY_STICKY = true, REVOKES = true, FINAL_STICKY = false).*
}
module mongofinal_rev {
  import resulttypes.*
  import resultstate(STORE = Mongo, READY_STICKY = false, REVOKES = true, FINAL_STICKY = true).*
}
