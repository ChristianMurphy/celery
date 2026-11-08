==========================================
 Chord harness results on main (768e476)
==========================================

Every run used two workers (concurrency 2 for prefork and threads),
``task_acks_late=True`` and ``task_reject_on_worker_lost=True``, 4 header
tasks per chord sleeping 0.05 to 1 s (0.05 to 5 s on their first delivery
only for the duplicate delivery runs). ``summary.jsonl`` has each run's full
configuration; ``<run>.jsonl.gz`` has the per-chord histories of every run
with an outcome other than ``ok`` (format in ``../../README.rst``).
``summary-cache-fix.jsonl`` is the memcached duplicate delivery run on a
prototype that counts each header task once.

What the outcomes come from:

* ``duplicate_body`` on chord_unlock backends: a copy of ``chord_unlock``
  delivered again after a lost worker or a broker restart, or restored by the
  Redis visibility timeout (kombu issue 337), sends the body again.
* ``body_and_error`` and ``spurious_error`` on memcached: a header copy that
  runs after the body was sent finds the group deleted and fails the chord;
  one that runs before the last part completes the count early and the join
  times out.
* ``body_and_error`` on PostgreSQL with broker restarts: a copy of
  ``chord_unlock`` failed the chord for a publish error, and a later copy
  sent the body.
* ``header_lost``, ``chord_error`` and ``body_failed`` with backend restarts:
  a store failed while the backend was down (the database backend retries 3
  times by default). ``chord_error`` on Elasticsearch: the first write to a
  new index failed with ``no_shard_available_action_exception``.
* ``header_lost`` and ``body_not_run`` with Redis broker worker kills: the
  message never ran again within the run. Not investigated; kombu issue 305
  may be related.
* ``publish_error``: the body couldn't be published within the retry policy
  while RabbitMQ restarted. Expected.

========== ============== ======== ========================================== ======= ========================================
Broker     Backend        Pool     Fault                                      Chords  Outcomes other than ok
========== ============== ======== ========================================== ======= ========================================
redis      memcached      prefork  duplicate delivery, visibility 3 s         40      body_and_error 36, spurious_error 4
redis      redis          prefork  duplicate delivery, visibility 3 s         40      -
redis      postgresql     prefork  duplicate delivery, visibility 3 s         40      duplicate_body 28
redis      redis          prefork  kill_random                                60      -
redis      postgresql     prefork  kill_random                                120     body_not_run 1, header_lost 1
rabbitmq   redis          prefork  kill_random                                60      -
rabbitmq   postgresql     prefork  kill_random                                60      -
redis      redis          prefork  kill_child                                 60      -
redis      postgresql     prefork  kill_child                                 60      -
rabbitmq   redis          prefork  kill_child                                 60      -
rabbitmq   postgresql     prefork  kill_child                                 60      -
redis      redis          prefork  kill_after_store                           60      -
redis      postgresql     prefork  kill_after_store                           60      -
rabbitmq   redis          prefork  kill_after_store                           60      -
rabbitmq   postgresql     prefork  kill_after_store                           60      -
redis      redis          prefork  broker_restart                             60      -
redis      postgresql     prefork  broker_restart                             60      -
rabbitmq   redis          prefork  broker_restart                             60      publish_error 2
rabbitmq   postgresql     prefork  broker_restart                             300     duplicate_body 1
redis      redis          prefork  backend_restart                            60      chord_error 1, header_lost 6
redis      postgresql     prefork  backend_restart                            60      chord_error 6
rabbitmq   redis          prefork  backend_restart                            60      header_lost 7
rabbitmq   postgresql     prefork  backend_restart                            60      body_failed 1, chord_error 2, header_lost 6
quorum     redis          prefork  kill_random                                60      -
quorum     postgresql     prefork  kill_random                                60      -
quorum     redis          prefork  broker_restart                             60      publish_error 2
quorum     postgresql     prefork  broker_restart                             60      -
redis      redis          solo     kill_random                                60      -
redis      postgresql     solo     kill_random                                180     body_not_run 1, duplicate_body 18
rabbitmq   redis          threads  kill_random                                60      -
rabbitmq   postgresql     threads  kill_random                                60      -
redis      redis          threads  broker_restart                             60      -
rabbitmq   postgresql     solo     broker_restart                             60      body_and_error 2, duplicate_body 2
rabbitmq   sqlite         prefork  kill_random                                60      -
rabbitmq   mongodb        prefork  kill_random                                60      -
rabbitmq   memcached      prefork  kill_random                                60      -
rabbitmq   cassandra      prefork  kill_random                                60      duplicate_body 1
rabbitmq   couchdb        prefork  kill_random                                60      -
rabbitmq   arangodb       prefork  kill_random                                60      -
rabbitmq   elasticsearch  prefork  kill_random                                60      chord_error 1
rabbitmq   mongodb        prefork  broker_restart                             60      -
rabbitmq   memcached      prefork  broker_restart                             60      body_and_error 6, spurious_error 2
redis      mongodb        prefork  backend_restart                            60      -
redis      sqlite         prefork  duplicate delivery, visibility 3 s         40      duplicate_body 28
redis      mongodb        prefork  duplicate delivery, visibility 3 s         40      duplicate_body 27
redis      cassandra      prefork  duplicate delivery, visibility 3 s         40      duplicate_body 30
redis      couchdb        prefork  duplicate delivery, visibility 3 s         40      duplicate_body 29
redis      arangodb       prefork  duplicate delivery, visibility 3 s         40      duplicate_body 25
redis      elasticsearch  prefork  duplicate delivery, visibility 3 s         40      duplicate_body 23
rabbitmq   postgresql     solo     kill_random                                60      duplicate_body 9
redis      mongodb        solo     kill_random                                60      duplicate_body 2
redis      postgresql     threads  kill_random                                60      -
redis      redis          prefork  backend_restart (always_retry)             60      -
redis      postgresql     prefork  backend_restart (always_retry)             60      chord_error 2, header_lost 3
rabbitmq   postgresql     prefork  backend_restart (always_retry)             60      header_lost 2
========== ============== ======== ========================================== ======= ========================================
