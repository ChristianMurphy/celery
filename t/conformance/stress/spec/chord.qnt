// -*- mode: Bluespec; -*-
// Chord completion: when is the body dispatched, and how often?
//
// Every rule cites celery at 768e476 (file:line). Three protocols, picked by
// PROTO:
//
//   "redis"   RedisBackend.on_chord_part_return with the Lua scripts
//             CHORD_PART_RETURN_LUA, CHORD_CLAIM_EXTEND_LUA and
//             CHORD_CLEANUP_LUA (redis.py:96-185, 934-1069), after
//             celery PR 10919 and PR 10998.
//   "kv"      BaseKeyValueStoreBackend.on_chord_part_return: a counter
//             with incr (base.py:1429-1503); memcached, dynamodb, gcs.
//   "unlock"  the celery.chord_unlock task polling the header results
//             (builtins.py:47-130); database backend and every backend
//             without incr (base.py:1069-1072, 1019-1067).
//
// A chord with header tasks HEADERS. Each header task message can be
// delivered more than once (broker redelivery, MAX_DUPS), worker processes
// can be lost at any step (whole worker: MAX_WORKER_LOST; pool child with the
// main process handling WorkerLostError: MAX_CHILD_LOST), and the broker can
// refuse a publish (MAX_REFUSALS).
module chord {
  const PROTO: str
  const ACKS_LATE: bool
  const REJECT_ON_WORKER_LOST: bool
  // The claim lease (redis) never expires while its holder is alive.
  // false = a holder can stall past the lease (GC pause, slow join with
  // result_chord_join_timeout=None, see the comment at redis.py:953-961).
  const LEASE_SAFE: bool
  const MAX_DUPS: int
  const MAX_WORKER_LOST: int
  const MAX_CHILD_LOST: int
  const MAX_REFUSALS: int

  pure val HEADERS = Set(1, 2)
  pure val N = 2
  pure val EXECS = Set(1, 2, 3)

  // A worker process running one delivery of a message.
  type Exec = {
    busy: bool,
    kind: str,        // "header" | "unlock"
    h: int,           // header index (1-based); group_index = h - 1
    pc: str,
    acked: bool,      // message acknowledged
    main: bool,       // running in the worker's main process (WorkerLostError path)
    tok: int,         // claim token (redis), unique per delivery
    res: str,         // what this run reports for the header
    args: Set[int],   // header indexes the body would get (after join)
    nargs: int,       // number of results the body would get
  }
  pure val FREE: Exec = { busy: false, kind: "", h: 0, pc: "", acked: true,
                          main: false, tok: 0, res: "", args: Set(), nargs: 0 }

  // ---- broker ----
  var queue: int -> int       // queued copies of each header task message
  var uq: int                 // queued chord_unlock messages
  var execs: int -> Exec
  // ---- result backend: header results (keep the first SUCCESS) ----
  var results: int -> str     // "PENDING" | "SUCCESS" | "FAILURE"
  // ---- redis chord keys (redis.py:944-948) ----
  var jset: Set[{ h: int, res: str, tok: int }]   // <gid>.j
  var dset: Set[int]                              // <gid>.d
  var skey: bool                                  // <gid>.s exists
  var ckey: int                                   // <gid>.c: 0 none, -1 "done", >0 token
  var nextTok: int
  // ---- kv chord keys ----
  var counter: int            // chord-unlock-<gid> counter
  var groupSaved: bool        // the saved GroupResult
  // ---- observations ----
  var dispatched: int
  var bodyArgs: Set[int]      // header indexes given to the last dispatched body
  var bodyArgsSize: int       // number of results given to it
  var chordError: bool        // chord_error_from_stack was called
  var errorAfterDispatch: bool
  // ---- fault budgets ----
  var dups: int
  var workerLost: int
  var childLost: int
  var refusals: int
  var lastAction: { kind: str, w: int, h: int, outcome: str }

  def allReady = HEADERS.forall(h => results.get(h) != "PENDING")
  def anyFailed = HEADERS.exists(h => results.get(h) == "FAILURE")

  action init = all {
    // the header messages are published once (canvas.py chord.run)
    queue' = HEADERS.mapBy(_ => 1),
    // fallback_chord_unlock publishes one chord_unlock (base.py:1058)
    uq' = if (PROTO == "unlock") 1 else 0,
    execs' = EXECS.mapBy(_ => FREE),
    results' = HEADERS.mapBy(_ => "PENDING"),
    jset' = Set(), dset' = Set(),
    // set_chord_size: drop <gid>.c, set <gid>.s (redis.py:862-868)
    skey' = PROTO == "redis",
    ckey' = 0, nextTok' = 1,
    // _apply_chord_incr saves the group (base.py:1422-1427); cache
    // backend sets the counter to 0 (cache.py:238-242)
    counter' = 0, groupSaved' = PROTO == "kv",
    dispatched' = 0, bodyArgs' = Set(), bodyArgsSize' = 0,
    chordError' = false, errorAfterDispatch' = false,
    dups' = 0, workerLost' = 0, childLost' = 0, refusals' = 0,
    lastAction' = { kind: "init", w: 0, h: 0, outcome: "" },
  }

  // ---- frame helpers ----
  action keepRedis = all { jset' = jset, dset' = dset, skey' = skey, ckey' = ckey }
  action keepKv = all { counter' = counter, groupSaved' = groupSaved }
  action keepObs = all {
    dispatched' = dispatched, bodyArgs' = bodyArgs, bodyArgsSize' = bodyArgsSize,
    chordError' = chordError, errorAfterDispatch' = errorAfterDispatch,
  }
  action keepBudgets = all {
    dups' = dups, workerLost' = workerLost, childLost' = childLost,
    refusals' = refusals,
  }
  action markError = all {
    chordError' = true,
    errorAfterDispatch' = (errorAfterDispatch or dispatched > 0),
    dispatched' = dispatched, bodyArgs' = bodyArgs, bodyArgsSize' = bodyArgsSize,
  }

  // ---- delivery ------------------------------------------------------------
  // A worker takes a header message. Without acks_late it is acked before
  // the task runs (worker/request.py, Request.execute_using_pool acks
  // early unless acks_late).
  action deliver(w: int, h: int): bool = all {
    not(execs.get(w).busy), queue.get(h) > 0,
    queue' = queue.set(h, queue.get(h) - 1),
    execs' = execs.set(w, { ...FREE, busy: true, kind: "header", h: h,
                            pc: "run", acked: not(ACKS_LATE), tok: nextTok }),
    nextTok' = nextTok + 1,
    uq' = uq, results' = results, jset' = jset, dset' = dset, skey' = skey,
    ckey' = ckey, keepKv, keepObs, keepBudgets,
    lastAction' = { kind: "deliver", w: w, h: h, outcome: "" },
  }

  action deliverUnlock(w: int): bool = all {
    PROTO == "unlock", not(execs.get(w).busy), uq > 0,
    uq' = uq - 1,
    execs' = execs.set(w, { ...FREE, busy: true, kind: "unlock", pc: "upoll",
                            acked: not(ACKS_LATE) }),
    queue' = queue, nextTok' = nextTok, results' = results, keepRedis, keepKv,
    keepObs, keepBudgets,
    lastAction' = { kind: "deliverUnlock", w: w, h: 0, outcome: "" },
  }

  // ---- header task stores its result ----------------------------------------
  // mark_as_done: store_result, then on_chord_part_return (base.py:290-296).
  // Stores keep the first SUCCESS (resultstate.qnt). The unlock protocol has
  // no part return (BaseBackend.on_chord_part_return is a no-op, base.py:1016).
  action store(w: int): bool = {
    val e = execs.get(w)
    all {
      e.busy, e.kind == "header", e.pc == "run",
      results' = results.set(e.h, "SUCCESS"),
      execs' = execs.set(w, { ...e, res: "SUCCESS",
                              pc: if (PROTO == "unlock") "finish" else "stored" }),
      queue' = queue, uq' = uq, nextTok' = nextTok, keepRedis, keepKv,
      keepObs, keepBudgets,
      lastAction' = { kind: "store", w: w, h: e.h, outcome: "" },
    }
  }

  // ---- redis: CHORD_PART_RETURN_LUA (redis.py:96-146), one atomic step ----
  action partReturn(w: int): bool = {
    val e = execs.get(w)
    all {
      PROTO == "redis", e.busy, e.pc == "stored",
      if (ckey == -1) all {
        // done marker: late duplicate, return -1 (redis.py:104-107, 976-977)
        execs' = execs.set(w, { ...e, pc: "finish" }),
        jset' = jset, dset' = dset, ckey' = ckey,
        lastAction' = { kind: "partReturn", w: w, h: e.h, outcome: "done" },
      } else {
        // add the result once per task id and header index (redis.py:111-119)
        val fresh = not(dset.contains(e.h))
        val j2 = if (fresh) jset.union(Set({ h: e.h, res: e.res, tok: e.tok })) else jset
        val d2 = dset.union(Set(e.h))
        // readycount == size + totaldiff, then SET ckey token NX
        // (redis.py:120-142); totaldiff is 0 here (no add_to_chord)
        val complete = skey and j2.size() == N
        val claim = complete and ckey == 0
        all {
          jset' = j2, dset' = d2,
          ckey' = if (claim) e.tok else ckey,
          // if readycount == total and claimed (redis.py:984)
          execs' = execs.set(w, { ...e, pc: if (claim) "claimed" else "finish" }),
          lastAction' = { kind: "partReturn", w: w, h: e.h,
                          outcome: if (claim) "claimed" else if (complete) "complete" else "counted" },
        }
      },
      skey' = skey, queue' = queue, uq' = uq, results' = results,
      nextTok' = nextTok, keepKv, keepObs, keepBudgets,
    }
  }

  // redis: read <gid>.j and unpack (redis.py:1007-1017). A FAILURE raises
  // ChordError: chord_error_from_stack, then DEL <gid>.s (redis.py:1052-1060).
  action redisJoin(w: int): bool = {
    val e = execs.get(w)
    val failed = jset.exists(x => x.res == "FAILURE")
    all {
      PROTO == "redis", e.busy, e.pc == "claimed",
      if (failed) all {
        markError, skey' = false,
        execs' = execs.set(w, { ...e, pc: "finish" }),
        lastAction' = { kind: "join", w: w, h: e.h, outcome: "error" },
      } else all {
        keepObs, skey' = skey,
        execs' = execs.set(w, { ...e, pc: "joined", args: jset.map(x => x.h),
                                nargs: jset.size() }),
        lastAction' = { kind: "join", w: w, h: e.h, outcome: "ok" },
      },
      jset' = jset, dset' = dset, ckey' = ckey,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, keepBudgets,
    }
  }

  // redis: CHORD_CLAIM_EXTEND_LUA (redis.py:171-185, called at 1021-1031)
  action extend(w: int): bool = {
    val e = execs.get(w)
    val mine = ckey == e.tok
    val reacquire = ckey == 0 and skey
    all {
      PROTO == "redis", e.busy, e.pc == "joined",
      ckey' = if (not(mine) and reacquire) e.tok else ckey,
      execs' = execs.set(w, { ...e, pc: if (mine or reacquire) "extended" else "finish" }),
      lastAction' = { kind: "extend", w: w, h: e.h,
                      outcome: if (mine) "extended" else if (reacquire) "reacquired" else "lost" },
      jset' = jset, dset' = dset, skey' = skey,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, keepObs, keepBudgets,
    }
  }

  // Publish the body: app.amqp._retry_publish(callback.delay, ...)
  //   redis  redis.py:1032-1040 (then cleanup in finally)
  //   kv     base.py:1485-1492 (then deps.delete / delete counter in finally)
  //   unlock builtins.py:124-129
  // A refused publish calls chord_error_from_stack.
  action publish(w: int, ok: bool): bool = {
    val e = execs.get(w)
    val nextPc = if (PROTO == "redis") "published" else if (PROTO == "kv") "kclean" else "finish"
    all {
      e.busy, e.pc == "extended" or e.pc == "kjoined" or e.pc == "ujoined",
      ok or refusals < MAX_REFUSALS,
      if (ok) all {
        dispatched' = dispatched + 1, bodyArgs' = e.args,
        bodyArgsSize' = e.nargs,
        chordError' = chordError, errorAfterDispatch' = errorAfterDispatch,
        refusals' = refusals,
      } else all { markError, refusals' = refusals + 1 },
      execs' = execs.set(w, { ...e, pc: nextPc }),
      lastAction' = { kind: "publish", w: w, h: e.h, outcome: if (ok) "ok" else "refused" },
      dups' = dups, workerLost' = workerLost, childLost' = childLost,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepRedis, keepKv,
    }
  }

  // redis: CHORD_CLEANUP_LUA (redis.py:150-168, called at 1041-1051)
  action cleanup(w: int): bool = {
    val e = execs.get(w)
    val mine = ckey == e.tok
    all {
      PROTO == "redis", e.busy, e.pc == "published",
      if (mine) all { jset' = Set(), dset' = Set(), skey' = false, ckey' = -1 }
      else keepRedis,
      execs' = execs.set(w, { ...e, pc: "finish" }),
      lastAction' = { kind: "cleanup", w: w, h: e.h, outcome: if (mine) "cleaned" else "lost" },
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepKv, keepObs, keepBudgets,
    }
  }

  // ---- kv: counter (base.py:1429-1503) ----
  action count(w: int): bool = {
    val e = execs.get(w)
    all {
      PROTO == "kv", e.busy, e.pc == "stored",
      if (not(groupSaved)) all {
        // GroupResult.restore() is None: ChordError (base.py:1446-1455)
        markError, counter' = counter,
        execs' = execs.set(w, { ...e, pc: "finish" }),
        lastAction' = { kind: "count", w: w, h: e.h, outcome: "nogroup" },
      } else all {
        // val = self.incr(key); val == size -> join (base.py:1456-1466)
        keepObs, counter' = counter + 1,
        execs' = execs.set(w, { ...e, pc: if (counter + 1 == N) "kcomplete" else "finish" }),
        lastAction' = { kind: "count", w: w, h: e.h,
                        outcome: if (counter + 1 == N) "complete" else if (counter + 1 > N) "over" else "counted" },
      },
      groupSaved' = groupSaved, keepRedis,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepBudgets,
    }
  }

  // kv: deps.join_native(timeout=result_chord_join_timeout, propagate=True)
  // (base.py:1467-1483). It waits for every header result; ok=false is the
  // join timing out (or a FAILURE propagating).
  action kvJoin(w: int, ok: bool): bool = {
    val e = execs.get(w)
    all {
      PROTO == "kv", e.busy, e.pc == "kcomplete",
      if (ok and allReady and not(anyFailed)) all {
        keepObs,
        execs' = execs.set(w, { ...e, pc: "kjoined", args: HEADERS, nargs: N }),
        lastAction' = { kind: "join", w: w, h: e.h, outcome: "ok" },
      } else all {
        // join raises: a header FAILURE propagates, or a header result is
        // still missing when result_chord_join_timeout runs out
        anyFailed or (not(ok) and not(allReady)),
        markError,
        execs' = execs.set(w, { ...e, pc: "kclean" }),
        lastAction' = { kind: "join", w: w, h: e.h, outcome: "error" },
      },
      keepRedis, keepKv,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepBudgets,
    }
  }

  // kv: finally: deps.delete(); self.delete(key) (base.py:1493-1495)
  action kvClean(w: int): bool = {
    val e = execs.get(w)
    all {
      PROTO == "kv", e.busy, e.pc == "kclean",
      groupSaved' = false, counter' = 0,
      execs' = execs.set(w, { ...e, pc: "finish" }),
      lastAction' = { kind: "cleanup", w: w, h: e.h, outcome: "cleaned" },
      keepRedis, keepObs,
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepBudgets,
    }
  }

  // ---- unlock: chord_unlock (builtins.py:50-130) ----
  // ready -> join; not ready -> self.retry() publishes a new chord_unlock
  // and this one is acked (Retry). A refused retry publish raises Reject:
  // chord_error_from_stack (celery PR 10959, builtins.py:87-106).
  action unlockPoll(w: int, ok: bool): bool = {
    val e = execs.get(w)
    all {
      PROTO == "unlock", e.busy, e.pc == "upoll",
      if (allReady) all {
        ok,
        if (anyFailed) all {
          // join(propagate=True) raises: builtins.py:114-122
          markError, execs' = execs.set(w, { ...e, pc: "finish" }),
          lastAction' = { kind: "poll", w: w, h: 0, outcome: "error" },
        } else all {
          keepObs, execs' = execs.set(w, { ...e, pc: "ujoined", args: HEADERS, nargs: N }),
          lastAction' = { kind: "poll", w: w, h: 0, outcome: "ready" },
        },
        uq' = uq, refusals' = refusals,
      } else if (ok) all {
        keepObs, uq' = uq + 1, refusals' = refusals,
        execs' = execs.set(w, { ...e, pc: "finish" }),
        lastAction' = { kind: "poll", w: w, h: 0, outcome: "retry" },
      } else all {
        refusals < MAX_REFUSALS,
        markError, uq' = uq, refusals' = refusals + 1,
        execs' = execs.set(w, { ...e, pc: "finish" }),
        lastAction' = { kind: "poll", w: w, h: 0, outcome: "refused" },
      },
      dups' = dups, workerLost' = workerLost, childLost' = childLost,
      keepRedis, keepKv,
      queue' = queue, results' = results, nextTok' = nextTok,
    }
  }

  // The task returns; the message is acked (acks_late) and the process is free.
  action finish(w: int): bool = {
    val e = execs.get(w)
    all {
      e.busy, e.pc == "finish",
      execs' = execs.set(w, FREE),
      lastAction' = { kind: "finish", w: w, h: e.h, outcome: "" },
      queue' = queue, uq' = uq, results' = results, nextTok' = nextTok,
      keepRedis, keepKv, keepObs, keepBudgets,
    }
  }

  // ---- faults ----------------------------------------------------------------
  // The broker delivers a header message again (redelivery after a lost
  // connection, visibility timeout, or a duplicate publish).
  action dup(h: int): bool = all {
    dups < MAX_DUPS,
    queue' = queue.set(h, queue.get(h) + 1), dups' = dups + 1,
    workerLost' = workerLost, childLost' = childLost, refusals' = refusals,
    uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepRedis, keepKv, keepObs,
    lastAction' = { kind: "dup", w: 0, h: h, outcome: "" },
  }

  // The whole worker is lost: nothing else runs; an unacked message goes
  // back to the queue (broker).
  action loseWorker(w: int): bool = {
    val e = execs.get(w)
    all {
      e.busy, workerLost < MAX_WORKER_LOST,
      execs' = execs.set(w, FREE),
      queue' = if (not(e.acked) and e.kind == "header") queue.set(e.h, queue.get(e.h) + 1) else queue,
      uq' = if (not(e.acked) and e.kind == "unlock") uq + 1 else uq,
      workerLost' = workerLost + 1,
      dups' = dups, childLost' = childLost, refusals' = refusals,
      results' = results, nextTok' = nextTok, keepRedis, keepKv, keepObs,
      lastAction' = { kind: "loseWorker", w: w, h: e.h, outcome: if (e.acked) "lost" else "requeued" },
    }
  }

  // A prefork pool child is lost; the main process gets WorkerLostError
  // and runs Request.on_failure (worker/request.py:709-790):
  //  - SUCCESS already stored: ack (acks_late) and return, without a chord
  //    part return (celery PR 10752, request.py:714-736);
  //  - acks_late and reject_on_worker_lost: requeue (request.py:751-758);
  //  - otherwise ack (acks_on_failure) and mark_as_failure, which stores
  //    FAILURE and runs on_chord_part_return in the main process
  //    (request.py:778-783, base.py:298-309).
  // chord_unlock runs with ignore_result, so the SUCCESS check is skipped
  // (request.py:850-851) and mark_as_failure has no chord to report to.
  action loseChild(w: int): bool = {
    val e = execs.get(w)
    all {
      // also at "finish": the child can die after on_chord_part_return
      // returned, in on_success, a task_postrun receiver or process_cleanup
      e.busy, not(e.main), childLost < MAX_CHILD_LOST,
      childLost' = childLost + 1,
      if (e.kind == "header" and results.get(e.h) == "SUCCESS") all {
        execs' = execs.set(w, FREE), queue' = queue, uq' = uq, results' = results,
        lastAction' = { kind: "loseChild", w: w, h: e.h, outcome: "success_stored" },
      } else if (ACKS_LATE and REJECT_ON_WORKER_LOST and not(e.acked)) all {
        execs' = execs.set(w, FREE), results' = results,
        queue' = if (e.kind == "header") queue.set(e.h, queue.get(e.h) + 1) else queue,
        uq' = if (e.kind == "unlock") uq + 1 else uq,
        lastAction' = { kind: "loseChild", w: w, h: e.h, outcome: "requeued" },
      } else if (e.kind == "header") all {
        // store_result keeps a stored SUCCESS (resultstate.qnt)
        results' = if (results.get(e.h) == "SUCCESS") results
                   else results.set(e.h, "FAILURE"),
        execs' = execs.set(w, { ...e, main: true, acked: true, res: "FAILURE",
                                pc: if (PROTO == "unlock") "finish" else "stored" }),
        queue' = queue, uq' = uq,
        lastAction' = { kind: "loseChild", w: w, h: e.h, outcome: "mark_failure" },
      } else all {
        execs' = execs.set(w, FREE), queue' = queue, uq' = uq, results' = results,
        lastAction' = { kind: "loseChild", w: w, h: e.h, outcome: "acked" },
      },
      dups' = dups, workerLost' = workerLost, refusals' = refusals,
      nextTok' = nextTok, keepRedis, keepKv, keepObs,
    }
  }

  // The claim key's TTL runs out (redis.py:963, 1018-1020). With LEASE_SAFE
  // only once the holder is gone.
  action claimExpires: bool = all {
    PROTO == "redis", ckey > 0,
    not(LEASE_SAFE) or not(EXECS.exists(w => execs.get(w).busy and execs.get(w).tok == ckey)),
    ckey' = 0,
    jset' = jset, dset' = dset, skey' = skey,
    queue' = queue, uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepKv, keepObs, keepBudgets,
    lastAction' = { kind: "claimExpires", w: 0, h: 0, outcome: "" },
  }

  // The "done" marker expires (done_expires, redis.py:966).
  action doneExpires: bool = all {
    PROTO == "redis", ckey == -1,
    ckey' = 0,
    jset' = jset, dset' = dset, skey' = skey,
    queue' = queue, uq' = uq, execs' = execs, results' = results, nextTok' = nextTok,
    keepKv, keepObs, keepBudgets,
    lastAction' = { kind: "doneExpires", w: 0, h: 0, outcome: "" },
  }

  action step = {
    nondet w = EXECS.oneOf()
    nondet h = HEADERS.oneOf()
    nondet ok = Set(true, false).oneOf()
    any {
      deliver(w, h), deliverUnlock(w), store(w), partReturn(w), redisJoin(w),
      extend(w), publish(w, ok), cleanup(w), count(w), kvJoin(w, ok),
      kvClean(w), unlockPoll(w, ok), finish(w),
      dup(h), loseWorker(w), loseChild(w), claimExpires, doneExpires,
    }
  }

  // ---- properties --------------------------------------------------------------
  // Safety: the body is dispatched at most once.
  val atMostOnce = dispatched <= 1
  // Safety: the body is not dispatched before every header result is stored.
  val noPremature = dispatched > 0 implies allReady
  // Safety: the body gets exactly one result per header.
  val argsComplete = dispatched > 0 implies (bodyArgs == HEADERS and bodyArgsSize == N)
  // Safety: a chord whose body was dispatched is not failed afterwards
  // (chord_error_from_stack stores FAILURE for the body id and calls its
  // errbacks, base.py:464-520).
  val noErrorAfterDispatch = not(errorAfterDispatch)
  // Safety: the chord is failed only for a reason: a header FAILURE or a
  // refused publish (not a duplicate delivery, a timeout on a header that
  // would have finished, or a cleaned-up group).
  val noSpuriousError = chordError implies (anyFailed or refusals > 0)
  // Witness (expected to be violated): some behavior dispatches the body.
  val neverDispatched = dispatched == 0
  // Bounded liveness: once nothing can run any more (no message queued, no
  // process busy, no claim left to expire), the chord has ended: the body
  // was dispatched or the chord was failed. Fault budgets are finite, so
  // every behavior reaches such a state if it runs long enough.
  val quiet = EXECS.forall(w => not(execs.get(w).busy))
              and HEADERS.forall(h => queue.get(h) == 0) and uq == 0 and ckey <= 0
  val quietDone = quiet implies (dispatched > 0 or chordError)

  val safety = atMostOnce and noPremature and argsComplete and noErrorAfterDispatch
}

// ---- instances: one per protocol and fault class -------------------------
module redis_none {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module redis_dup {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 2, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module redis_refuse {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 1).*
}
module redis_wlost_late {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module redis_wlost_early {
  import chord(PROTO = "redis", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module redis_child_late {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module redis_child_early {
  import chord(PROTO = "redis", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module redis_child_reject {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = true, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module redis_lease {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = false, MAX_DUPS = 1, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module kv_none {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module kv_dup {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 2, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module kv_refuse {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 1).*
}
module kv_wlost_late {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module kv_wlost_early {
  import chord(PROTO = "kv", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module kv_child_late {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module kv_child_early {
  import chord(PROTO = "kv", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module kv_child_reject {
  import chord(PROTO = "kv", ACKS_LATE = true, REJECT_ON_WORKER_LOST = true, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module unlock_none {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module unlock_dup {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 2, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module unlock_refuse {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 1).*
}
module unlock_wlost_late {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module unlock_wlost_early {
  import chord(PROTO = "unlock", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 1, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).*
}
module unlock_child_late {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module unlock_child_early {
  import chord(PROTO = "unlock", ACKS_LATE = false, REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
module unlock_child_reject {
  import chord(PROTO = "unlock", ACKS_LATE = true, REJECT_ON_WORKER_LOST = true, LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 1, MAX_REFUSALS = 0).*
}
