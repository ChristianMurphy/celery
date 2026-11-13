# Transport: kombu's Redis consume path, and a RabbitMQ publisher

`redis_transport.qnt`, `redis_transport_instances.qnt` (27 instances; kombu
445027e and kombu PR 2831 at c61b525) and `publisher.qnt`,
`publisher_instances.qnt` (6 instances). The worker on top of the transport
is in [../worker/](../worker/README.md), which also compares the designs.

## Redis transport

One queue, two messages, two consumers with prefetch 1.

| `design` | Code |
|---|---|
| `BrpopThenRecord` | main: BRPOP, and the record in `unacked` when the reply is read, redis.py:1457-1496, 474-495 at 445027e |
| `PopScript` | kombu PR 2831: `POP_UNACKED` pops and records in one step (:223-305), wake tokens (:279-292, 1442-1445, 1977-2007), `PUT_BACK` with its score check for a reply without credit or as old as the visibility timeout (:313-326, 1689-1698), the second record under the message's own tag in one transaction (:601-624) |
| `PopScriptPaused` | the PR before eb41008: a reply without credit waits and is delivered whatever its age |

Faults: a consumer killed, a delivered message held past the visibility
timeout by a live consumer, a reply delayed past it, credit lost while a reply
is on its way, and the script reading another delivery tag than kombu.

Quiet: every message published, and no consumer can pop, read a reply, ack,
regain credit or restore a dead consumer's message.

Since then it has `CFG.renewal` (the renewal of unacked messages proposed
for kombu issue 337, as a blind `ZADD XX` or as compare and set), `CFG.putBack`
(by score, or by a token stored with the record), the property `noStranded`
(no record is left with a consumer that dropped the message), and the action
`waitAfterEmpty`, which recorded command histories showed was missing. The
worker side (prefetch above 1, the pool, shutdown, partitions, the classes of
duplicates and the designs compared) is `../worker/worker.qnt`; the designs are compared in
[../worker/README.md](../worker/README.md). Not modeled anywhere yet: several
queues and priorities.

### Properties

`ops` counts round trips to Redis.

<!-- generated: properties redis_transport.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `inSomePlace` | safety | At every moment a published message is on the queue, in unacked, or acked. Assumes: one queue, two messages, two consumers with prefetch 1. | Universality, global (conservation) | kombu PR 2831 (c61b525): its state-machine test states this conservation; redis.py:223-305 pops and records in one script |
| `neverLost` | safety | A published message is on the queue, in unacked, acked, or in a reply a live consumer is about to read. | Universality, global (conservation, weak) | kombu issue 305; kombu 445027e redis.py:1457-1496 |
| `notTwice` | safety | A published message is never in two places. | Absence of a second place, global, while no consumer was killed and none held a message past the timeout | kombu PR 2831 (c61b525) redis.py:601-624: the second record replaces the script's in one transaction |
| `noDuplicate` | safety | No message is delivered twice. | Bounded existence (at most one delivery), while no consumer was killed and none held a message past the timeout | kombu PR 2831 (c61b525) redis.py:313-326, 1689-1698: `PUT_BACK` checks the score |
| `prefetchRespected` | safety | A consumer never holds more messages than its prefetch count. | Universality, global | docs/userguide/optimizing.rst:119 (celery 768e476): "The prefetch limit is a limit for the number of tasks (messages) a worker can reserve for itself" |
| `noMissedWake` | safety | No message waits on the queue while a consumer with credit waits and no token or pop is on the way. Assumes: the pop script designs only. | Universality, global, while no consumer was killed | kombu PR 2831 (c61b525) redis.py:279-292, 1442-1445, 1977-2007: wake tokens |
| `noStranded` | safety | No record is left in unacked with a consumer that dropped the message. | Absence, global | kombu PR 2831 (c61b525) redis.py:313-326: a put-back that finds its record gone does nothing |
| `quietDone` | liveness | A quiet system has acked every message and left no record behind. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Response: published leads to acked (lemma 3 of 3) | kombu 445027e redis.py:636-638 (`QoS.ack`), 660-699 (restore) |
| `exactlyOnce` | safety | Without a fault a message is never delivered twice, and when quiet it was delivered once and acked. | Bounded existence (exactly one delivery), while no fault was used | the transport contract; kombu PR 2831 |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: no action other than a fault can run. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Absence of a stop outside a quiet state (lemma 2 of 3) | the spec: `quiet`, checked by TLC with deadlock detection on `stepOrQuiet` |
| `settles` | liveness | A run in which the code keeps working ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes; weak fairness of `work` (every action that uses no fault budget), none for the faults. | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. | Existence | sanity |
| `neverDuplicated` | witness | Some run delivers a message twice. | Existence | docs/getting-started/backends-and-brokers/redis.rst:315-320 (celery 768e476): a task not acknowledged within the visibility timeout "will be redelivered to another worker and executed" |
| `neverPutBack` | witness | Some run puts a reply back. | Existence | sanity |
<!-- end generated -->

### Mapping to the code

| Spec action | Code |
|---|---|
| `sendPop`, `serverPop`, `receive` (main) | `_brpop_start`, `_brpop_read`, `QoS.append`, kombu 445027e redis.py:1457-1496, 474-495 |
| `sendPop`, `serverPop`, `popEmpty`, `wakeUp`, `waitTimeout` (PR) | `_brpop_start` c61b525 redis.py:1647-1672; `POP_UNACKED` :223-305; `_brpop_read` :1674-1722 |
| `receive` (PR) | put back :1689-1698 and `PUT_BACK` :313-326; `_deliver_popped` :1748-1760; `QoS.append(replaces=...)` :601-624 |
| `ack` | `QoS.ack` :636-638 |
| `restore` | `restore_visible` :660-686, `restore_by_tag` :688-699, `_do_restore_message` :1431-1450 |

### Results

`python3 tools/check.py --tier full --only transport`. `main_` is 445027e,
`pr_` is c61b525, `paused_` is the PR before eb41008, `renewblind_` and
`renewown_` add the renewal, `token_` the pop token.

<!-- generated: results redis_transport.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| main_crash | 16 | inSomePlace (4), neverLost (5), quietDone (11) | neverQuiet (11), neverDuplicated (10) |
| main_credit | 14 | inSomePlace (4) | neverQuiet (13) |
| main_credit_crash | 18 | inSomePlace (4), neverLost (5), quietDone (11) | neverQuiet (11), neverDuplicated (10) |
| main_held_timeout | 17 | inSomePlace (4) | neverQuiet (13), neverDuplicated (9) |
| main_none | 12 | inSomePlace (4) | neverQuiet (13) |
| paused_credit_timeout | 36 | notTwice (7), noDuplicate (9) | neverQuiet (15), neverDuplicated (9) |
| pr_crash | 31 | none | neverQuiet (14), neverDuplicated (10) |
| pr_credit | 34 | none | neverQuiet (15), neverPutBack (6) |
| pr_credit_crash | 41 | none | neverQuiet (14), neverDuplicated (10), neverPutBack (6) |
| pr_credit_timeout | 43 | none | neverQuiet (15), neverPutBack (6) |
| pr_held_timeout | 34 | none | neverQuiet (15), neverDuplicated (9) |
| pr_mismatch | 24 | none | neverQuiet (15) |
| pr_mismatch_crash | 31 | none | neverQuiet (14), neverDuplicated (10) |
| pr_none | 24 | none | neverQuiet (15) |
| pr_reply_timeout | 33 | none | neverQuiet (15) |
| renewblind_held_credit | 44 | noStranded (11) | neverQuiet (15), neverDuplicated (9), neverPutBack (6) |
| renewown_held_credit | 44 | none | neverQuiet (15), neverDuplicated (9), neverPutBack (6) |
| token_crash | 31 | none | neverQuiet (14), neverDuplicated (10) |
| token_credit | 34 | none | neverQuiet (15), neverPutBack (6) |
| token_credit_crash | 41 | none | neverQuiet (14), neverDuplicated (10), neverPutBack (6) |
| token_credit_timeout | 43 | none | neverQuiet (15), neverPutBack (6) |
| token_held_timeout | 34 | none | neverQuiet (15), neverDuplicated (9) |
| token_mismatch | 24 | none | neverQuiet (15) |
| token_mismatch_crash | 31 | none | neverQuiet (14), neverDuplicated (10) |
| token_none | 24 | none | neverQuiet (15) |
| token_renewblind_held_credit | 44 | none | neverQuiet (15), neverDuplicated (9), neverPutBack (6) |
| token_reply_timeout | 33 | none | neverQuiet (15) |
<!-- end generated -->

Why each violation is expected:

<!-- generated: reasons redis_transport.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `inSomePlace` | main_crash (4), main_credit (4), main_credit_crash (4), main_held_timeout (4), main_none (4) | open | tracker reply-kombu-305-pr (kombu issue 305); kombu PR 2831 fixes it. between BRPOP and the record the message is in no key; no fault needed |
| `neverLost` | main_crash (5), main_credit_crash (5) | open | tracker reply-kombu-305-pr (kombu issue 305); kombu PR 2831 fixes it. a consumer killed between the pop and the record |
| `notTwice` | paused_credit_timeout (7) | fixed | eb41008 in kombu PR 2831 |
| `noDuplicate` | paused_credit_timeout (9) | fixed | eb41008 in kombu PR 2831 |
| `noStranded` | renewblind_held_credit (11) | open | tracker reply-kombu-337-renew-scope (kombu issue 337). a design variant: the blind `ZADD XX` renewal next to kombu PR 2831 |
| `quietDone` | main_crash (11), main_credit_crash (11) | open | tracker reply-kombu-305-pr (kombu issue 305); kombu PR 2831 fixes it |
<!-- end generated -->

On the consume path the PR makes 3 round trips per message (publish, pop,
ack) where main makes 4 (publish, BRPOP, record, ack). A consumer that finds
the queue empty costs the PR two (the empty pop, then the wait on the wake
list) where main's BRPOP is one; after a pop that emptied the queue it waits
without popping first.

### Findings

* kombu, main: a message is in no key between the pop and the record
   (`main_none.inSomePlace`, 4 states, no fault needed), and a consumer killed
   there loses it for good (`main_crash.neverLost` and `quietDone`): a silent
   loss, kombu issue 305. With the PR every instance holds `inSomePlace` and
   `neverLost`.
* kombu, the PR before eb41008 (`paused_credit_timeout.noDuplicate`): a
   reply paused without credit for longer than the visibility timeout is
   restored and delivered by another consumer, then delivered by the first: a
   duplicate with no consumer lost. `pr_credit_timeout` holds; the mutant that
   drops `PUT_BACK`'s score check is caught.

What remains with kombu PR 2831:

* `pr_held_timeout`, `main_held_timeout` (witness `neverDuplicated`, 9
  states): a task that outlasts the visibility timeout is restored and
  delivered again while its consumer lives. Duplicate, with no fault. This
  is the transport's contract (at least once).
* `pr_crash` (witness `neverDuplicated`, 10 states): a consumer killed while
  it holds a delivered message. Duplicate; a task that was mid-run cannot be
  told from one that finished.

## Publisher

`publisher.qnt`: a RabbitMQ publisher with confirms off or on, two messages,
a durable queue that loses what it accepted and had not written when the
broker is killed, and a network that drops the publisher's data. Properties:
`neverLost` (a message whose publish returned is on its way, accepted or
written), `publishedOnce`, `exactlyOnce`, `quietDone`, `normalCost`.

### Properties

<!-- generated: properties publisher.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `neverLost` | safety | A message whose publish returned is on its way, accepted or written. Assumes: RabbitMQ, a durable queue, two messages. | Universality, global | docs/userguide/calling.rst:527 and docs/getting-started/backends-and-brokers/rabbitmq.rst:195 (celery 768e476): enable `confirm_publish` |
| `publishedOnce` | safety | The queue never has a message twice. | Bounded existence (at most one copy), global | the publish retry policy, docs/userguide/calling.rst |
| `exactlyOnce` | safety | Without a fault every message is on disk once when quiet. | Bounded existence (exactly one copy), while no fault was used | the publish path |
| `quietDone` | liveness | A quiet system has every message on disk and every publish returned. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Response: a publish leads to a written message (lemma 3 of 3) | docs/userguide/calling.rst:527 |
| `normalCost` | safety | One frame per publish without confirms, two with. | Universality at the end, while no fault was used | AMQP 0-9-1 basic.publish and basic.ack |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes. | Absence of a stop outside a quiet state (lemma 2 of 3) | the spec: `quiet`, checked by TLC with deadlock detection on `stepOrQuiet` |
| `settles` | liveness | A run in which the code keeps working ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: fault budgets are finite; the checks are exhaustive searches of small instances (two messages or two header tasks, two or three processes), not proofs for all sizes; weak fairness of `work` (every action that uses no fault budget), none for the faults. | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. | Existence | sanity |
<!-- end generated -->

### Results

<!-- generated: results publisher.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| off_crash | 8 | neverLost (3), quietDone (5) | neverQuiet (5) |
| off_drop | 6 | neverLost (3), quietDone (4) | neverQuiet (4) |
| off_none | 6 | none | neverQuiet (7) |
| on_crash | 16 | publishedOnce (8) | neverQuiet (9) |
| on_drop | 15 | publishedOnce (7) | neverQuiet (9) |
| on_none | 8 | none | neverQuiet (9) |
<!-- end generated -->

Why each violation is expected:

<!-- generated: reasons publisher.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `neverLost` | off_crash (3), off_drop (3) | open | tracker eo-rabbitmq-confirm-publish-loss (celery discussion 11073). confirms are off by default |
| `publishedOnce` | on_crash (8), on_drop (7) | by design | publisher confirms. a lost confirm makes the publisher send again: silent loss becomes a duplicate |
| `quietDone` | off_crash (5), off_drop (4) | open | tracker eo-rabbitmq-confirm-publish-loss (celery discussion 11073) |
<!-- end generated -->

Off: a publish that returned is lost when the broker dies or the data is
dropped (3 states). On: nothing is lost, and a lost confirm puts a second
copy on the queue (7 and 8 states): silent loss becomes a duplicate. Cost:
two frames per publish where there was one, and a wait for the second. The
model was not run against RabbitMQ; its results match the runs in the
exactly-once report (191 of 2,280 lost without confirms, none with).

## Routing

`routing.qnt`, `routing_instances.qnt` (15 instances; celery 768e476, the
same at 9c2f238, kombu 445027e, py-amqp 5.4.2): one client publishes m1 to
the target under test and then m2 to the default queue on the same
connection; a worker consumes the default queue. Targets: a queue in
task_queues that no worker consumes, a queue missing from task_queues
(task_create_missing_queues on and off), an exchange with no queue bound,
an exchange nobody declared, a queue deleted after the client cached its
declaration, a queue with x-message-ttl. Instances
`<broker>_<target>[_nocreate][_confirm]`.

<!-- generated: properties routing.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noSilentDrop` | safety | A message whose publish returned is not dropped by the broker. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Absence (a message dropped after its publish returned), global | docs/userguide/calling.rst:523-528 (celery 768e476): with RabbitMQ, messages can be silently dropped only when the broker hits resource limits; confirm_publish detects that |
| `errorIsOwn` | safety | A publish raises only for its own message: a call whose message is fine does not raise because of an earlier call. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Absence (an error raised to another call), global | docs/userguide/calling.rst:477-521 (celery 768e476): apply_async raises OperationalError when the message cannot be sent, and the caller handles it |
| `quietDone` | liveness | At the end every message whose publish returned has run. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Response (lemma 3 of 3: at the end every message whose publish returned ran) | docs/userguide/routing.rst:67-73 (celery 768e476): a worker processes the queues it is started with |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run that keeps going ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory; weak fairness of `step` (the spec has no fault budgets). | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Existence | sanity |
| `neverRaised` | witness | Some publish raises. Assumes: one client publishes m1 to the target under test and then m2 to the default queue on the same connection; a worker consumes the default queue and the target queue when it is in task_queues and consumed; Redis or RabbitMQ; confirm_publish and task_create_missing_queues as the instance says; Celery does not set mandatory. | Existence | sanity |
<!-- end generated -->

<!-- generated: results routing.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| rabbit_consumed | 4 | none | neverQuiet (5) |
| rabbit_deleted | 5 | noSilentDrop (2), quietDone (5) | neverQuiet (5) |
| rabbit_missing_nocreate | 3 | none | neverQuiet (4), neverRaised (2) |
| rabbit_ttl | 4 | noSilentDrop (3), quietDone (5) | neverQuiet (5) |
| rabbit_unbound | 3 | noSilentDrop (2), quietDone (4) | neverQuiet (4) |
| rabbit_unbound_confirm | 3 | noSilentDrop (2), quietDone (4) | neverQuiet (4) |
| rabbit_undeclared | 2 | noSilentDrop (2), errorIsOwn (3), quietDone (3) | neverQuiet (3), neverRaised (3) |
| rabbit_undeclared_confirm | 3 | none | neverQuiet (4), neverRaised (2) |
| redis_consumed | 4 | none | neverQuiet (5) |
| redis_deleted | 4 | none | neverQuiet (5) |
| redis_missing | 3 | quietDone (4) | neverQuiet (4) |
| redis_missing_nocreate | 3 | none | neverQuiet (4), neverRaised (2) |
| redis_unbound | 3 | noSilentDrop (2), quietDone (4) | neverQuiet (4) |
| redis_unconsumed | 3 | quietDone (4) | neverQuiet (4) |
| redis_undeclared | 3 | noSilentDrop (2), quietDone (4) | neverQuiet (4) |
<!-- end generated -->

<!-- generated: reasons routing.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noSilentDrop` | rabbit_unbound (2), rabbit_unbound_confirm (2), redis_unbound (2) | documented | amqp/channel.py:1745-1752 (py-amqp 5.4.2, the AMQP 0-9-1 text of basic.publish): without the mandatory flag the server silently drops a message it cannot route; Celery does not set mandatory, and confirm_publish does not change this (rabbit_unbound_confirm). kombu's virtual transports (Redis) do the same unless transport_options deadletter_queue is set (kombu virtual/base.py:467-471 and 717-744). an exchange with no queue bound for the routing key |
| `noSilentDrop` | redis_undeclared (2) | documented | kombu virtual/base.py:717-744 (445027e): Redis has no exchange declarations, so an unknown exchange routes to no queue as an unbound one does, and the message is dropped unless deadletter_queue is set |
| `noSilentDrop` | rabbit_undeclared (2) | open | finding undeclared-exchange-next-call (closest kombu issue 285, 2013, closed: a publish to an undeclared exchange is not reported). without confirm_publish the 404 is not read by the publish that caused it |
| `noSilentDrop` | rabbit_deleted (2) | documented | kombu/common.py:110-111 (445027e): maybe_declare declares an entity once per connection (cached); a queue deleted later is not declared again by the client. The worker that consumes it reconnects on basic.cancel and declares it again; a publish in between is dropped (1 of 1 on 9c2f238 with RabbitMQ 3.13, a queue deleted with queue_delete) |
| `noSilentDrop` | rabbit_ttl (3) | documented | kombu/entity.py:474-480 (445027e): with message_ttl a message nobody consumed in time is deleted and no consumer sees it |
| `errorIsOwn` | rabbit_undeclared (3) | open | finding undeclared-exchange-next-call. the 404 for m1 is raised by m2's publish, and m2 is not sent |
| `quietDone` | redis_unconsumed (4) | documented | docs/userguide/routing.rst:67-80 (celery 768e476): a worker consumes the queues given with -Q; a task on a queue no worker consumes waits there, PENDING |
| `quietDone` | redis_missing (4) | documented | docs/userguide/configuration.rst:3104-3112 and routing.rst:27-32 (celery 768e476): a queue missing from task_queues is created; the message waits there until a worker consumes it (probe: nothing logged) |
| `quietDone` | rabbit_unbound (4), rabbit_unbound_confirm (4), redis_unbound (4) | documented | as noSilentDrop |
| `quietDone` | rabbit_undeclared (3), redis_undeclared (4) | documented | as noSilentDrop (rabbit_undeclared: finding undeclared-exchange-next-call) |
| `quietDone` | rabbit_deleted (5) | documented | as noSilentDrop |
| `quietDone` | rabbit_ttl (5) | documented | as noSilentDrop |
<!-- end generated -->

What the model says, and what the real code did (9c2f238, kombu 445027e,
py-amqp 5.4.2, RabbitMQ 3.13 and Redis 7):

* An exchange nobody declared on RabbitMQ without confirm_publish
  (`rabbit_undeclared`): m1's call returns and m1 is lost; the 404 is
  raised by m2's call, and m2 is not sent (`errorIsOwn`, 3 states). With
  confirm_publish m1's own call raises and m2 runs
  (`rabbit_undeclared_confirm` passes every property). See
  [undeclared-exchange-next-call](../findings/undeclared-exchange-next-call/):
  3 of 3, the control 3 of 3, Celery 5.5.3 and 5.4.0 the same.
* On Redis the same call returns and the message is dropped with nothing
  raised or logged (3 of 3 in the same script); an exchange with no queue
  bound drops the message on both brokers, with or without confirms (AMQP
  without mandatory). Both documented (the reasons above).
* A queue missing from task_queues raises QueueNotFound at the call when
  task_create_missing_queues is off, and is created and waits, with nothing
  logged, when it is on (scratchpad probe, 1 of 1 each broker).
* A queue deleted while a worker consumes it: the worker logs
  ConsumerCancelled as a lost connection, reconnects and declares the
  queue again within a few milliseconds; a publish in between is dropped,
  because the client does not declare a cached queue again
  (`rabbit_deleted`, 1 of 1 on the real code). On Redis nothing is lost.

## Remote control replies

`pidbox.qnt`, `pidbox_instances.qnt` (4 instances; celery 9c2f238, kombu
445027e): one client sends two broadcasts with reply=True to one worker,
one after the other; the worker answers in order at any time, so a reply
can come after its call timed out. The two calls use different reply
queue names (`fresh`) or the same one (`reused`: the name comes from id()
of a short-lived Mailbox copy and repeats), with a destination (limit 1)
or without. Design `ticketedLimit`: only this call's replies count toward
limit. Finding control-late-reply-limit.

<!-- generated: properties pidbox.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `noLostReply` | safety | A call whose worker answered while the call was waiting returns that reply. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Universality (a call returns the reply its worker gave while it waited), global | celery/app/control.py:755-775 (celery 9c2f238): reply: wait for and return the reply; timeout: timeout in seconds to wait for the reply; limit: limit number of replies |
| `quietDone` | liveness | At the end both calls returned. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Response (lemma 3 of 3: at the end both calls returned) | celery/app/control.py:755-775 (celery 9c2f238): a broadcast with reply=True returns within its timeout |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run that keeps going ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none; weak fairness of `step` (the spec has no fault budgets). | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Existence | sanity |
| `neverStale` | witness | A call drains another call's reply. Assumes: one client sends two broadcasts with reply=True to one worker, one after the other; the worker answers in order, at any time (a reply after its call returned is late); each call's reply queue is deleted when it returns; the two calls use different or the same reply queue name; limit 1 (a destination) or none. | Existence | sanity |
<!-- end generated -->

<!-- generated: results pidbox.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| fresh_limit | 6 | none | neverQuiet (7) |
| reused_limit | 6 | noLostReply (7) | neverQuiet (7), neverStale (6) |
| reused_limit_ticketed | 7 | none | neverQuiet (7), neverStale (6) |
| reused_nolimit | 7 | none | neverQuiet (7), neverStale (6) |
<!-- end generated -->

<!-- generated: reasons pidbox.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `noLostReply` | reused_limit (7) | open | finding control-late-reply-limit (not in the tracker yet; closest celery issue 6778, open, No nodes replied within time constraint intermittently, no cause found there). a late reply of the first call lands in the second call's queue of the same name and uses its one drain (pidbox.py:387-411); the call returns no reply, and its own reply is deleted with the queue |
<!-- end generated -->

## Content: serializers, compression and accept lists

`content.qnt`, `content_instances.qnt` (6 instances; celery 9c2f238, kombu
445027e): one task with a link and a link_error; one setting at a time
does not fit: the arguments for the task serializer, the compression for
the worker's kombu, the content type for the worker's accept_content, the
return value for the result serializer, or the reader's result settings
for the stored result.

<!-- generated: properties content.qnt -->
| Property | Kind | Statement | Pattern | Source |
|---|---|---|---|---|
| `linkOnlyOnSuccess` | safety | The link runs only for a task whose SUCCESS is stored. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Precedence: a stored SUCCESS precedes the link | docs/userguide/calling.rst (celery 9c2f238), Linking: the callback is applied when the task returns successfully |
| `errbackOnFailure` | safety | A stored FAILURE is followed by its errback, which runs at most once. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Response (a stored FAILURE is followed by one errback call), global | docs/userguide/calling.rst (celery 9c2f238), Linking: the errback is applied when the task fails |
| `quietDone` | liveness | At the end the caller has a value or an error; it does not wait for ever. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Response (lemma 3 of 3: at the end the caller has a value or an error) | docs/userguide/calling.rst (celery 9c2f238), Serializers: the content type of a message must be one the worker accepts |
| `progress` | liveness | steps <= D: finite by exhaustive search with the step counter in the state. The spec has no closed-form measure. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Bounded existence of steps (lemma 1 of 3: every run is finite) | the search: D per instance in expected.json |
| `stepOrQuiet` | liveness | A run stops only in a quiet state: TLC runs stepOrQuiet with deadlock detection on. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Response (lemma 2 of 3: a run stops only in a quiet state) | the search |
| `settles` | liveness | A run that keeps going ends for good in a quiet state, and `quietDone` holds there. It fails exactly where a lemma fails. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result; weak fairness of `step` (the spec has no fault budgets). | Existence of a point after which quiet holds for ever (stabilisation), globally, under weak fairness; the three lemmas in one formula | the spec: the conclusion of `progress`, `stepOrQuiet` and `quietDone`, checked by TLC over every fair behavior of `stepOrQuiet` |
| `neverQuiet` | witness | Some run ends. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Existence | sanity |
| `neverError` | witness | The caller gets an error. Assumes: one task with a link and a link_error on Redis; one setting at a time does not fit: the arguments for the task serializer, the message's compression for the worker's kombu, the content type for the worker's accept_content, the return value for the result serializer, the reader's result serializer and result_accept_content for the stored result. | Existence | sanity |
<!-- end generated -->

<!-- generated: results content.qnt -->
| instance | D | violated | reachable (witnesses) |
|---|---|---|---|
| args | 1 | none | neverQuiet (2), neverError (2) |
| ok | 3 | none | neverQuiet (4) |
| pickle_json | 2 | quietDone (3) | neverQuiet (3) |
| reader | 3 | none | neverQuiet (4), neverError (4) |
| result | 3 | linkOnlyOnSuccess (3) | neverQuiet (4), neverError (4) |
| zstd_oldworker | 2 | quietDone (3) | neverQuiet (3) |
<!-- end generated -->

<!-- generated: reasons content.qnt -->
| Property | Instances (states in a shortest counterexample) | Why | Where |
|---|---|---|---|
| `linkOnlyOnSuccess` | result (3) | open | finding chain-store-error (a reply on celery issue 10954 drafted by the lead): the link is published before the result is stored (trace.py:599-612). probe on 9c2f238: a task returning an object JSON cannot encode stored FAILURE (EncodeError) and ran both its link and its link_error, 1 of 1 |
| `quietDone` | zstd_oldworker (3) | by design | consumer.py:565-579 (celery 9c2f238): on_decode_error logs CRITICAL and acks the message so that it does not loop; no task id is read from it (badmsg.qnt's undecodable case). probe: main (kombu 5.7.0) sent compression='zstd' to a 5.5.3 worker (kombu 5.5.4, no zstd decoder): "Can't decode message body: KeyError('application/zstd')", acked, PENDING for ever, the worker went on, 1 of 1 |
| `quietDone` | pickle_json (3) | documented | docs/userguide/configuration.rst:243-246 (celery 768e476): a message whose content type is not in accept_content is discarded with an error (badmsg.qnt's disallowed case). probe on 9c2f238: PENDING for ever, "Received invalid task message: Refusing to deserialize untrusted content of type pickle", 1 of 1 |
<!-- end generated -->

What the real code did on 9c2f238 (kombu 445027e, Redis 7, -c 2,
scratchpad `serprobe.py` and `mixcomp.py`, 1 of 1 each, nothing injected):

| Case | Result | Status |
|---|---|---|
| an argument JSON cannot encode | apply_async raised EncodeError at the call | passes (`args`) |
| a return value JSON cannot encode, alone | FAILURE (EncodeError); both the link and the link_error ran | finding chain-store-error (`result`) |
| the same as a group member | FAILURE for that member, SUCCESS for the other; get() returned the EncodeError and the value | passes |
| the same as a chord header task, with a link_error on the chord | the body FAILURE (ChordError), the errback once | passes |
| a pickle message to a worker with the default accept_content | "Received invalid task message", acked, PENDING for ever | documented (`pickle_json`) |
| compression zlib, bzip2, lzma, zstd on main to main | SUCCESS each; brotli raised at the call (no brotli module) | passes |
| compression zstd from main to a 5.5.3 worker (kombu 5.5.4) | CRITICAL "Can't decode message body: KeyError('application/zstd')", acked, PENDING for ever; zlib to the same worker SUCCESS | by design (`zstd_oldworker`) |
| results stored as pickle, a reader with result_accept_content json | get_task_meta and get() raised DecodeError at once | passes (`reader`): an error, not a wait |
