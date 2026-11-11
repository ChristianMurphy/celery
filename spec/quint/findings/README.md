# Findings confirmed on the real code

A counterexample is a candidate until the real code does the same. Each
folder here is one finding: `finding.json` (the instances and properties,
the outcome on the ranking duplicate, visible failure, hang, silent loss,
the status and the upstream thread in words), the counterexample as an ITF
trace (`<instance>.<property>.itf.json`, written by
`python3 tools/check.py --cex`), and the script that drives the real Celery
or kombu code path against a real service. `PYTHONPATH` selects the tree;
each script prints one line per case.

<!-- generated: findings -->
| Finding | Instance and property | Outcome | Status | Thread | On the real code |
|---|---|---|---|---|---|
| [ack-after-loss](ack-after-loss/): An ack that fails with its connection is forgotten, and one through a collected channel stops the worker | `pr_late_cut.noAvoidableDup`, `pr_late_cut.workerSurvives`, `main_late_partition.workerSurvives` | duplicate | open: tracker eo-redis-ack-after-connection-loss | the reply posted on celery issue 11054 | `confirm_ack_after_loss.py`. Main: delivered again when the channel closes. Resend prototype: not delivered again. The worker exit needs a worker; those runs are in the reply. |
| [flush-ack-modes](flush-ack-modes/): A warm shutdown or a reconnect runs a task twice: flush() drops a job the child already has | `pr_late_shutdown.exactlyOnce`, `pr_early_shutdown.exactlyOnce`, `ackfull_late_cut.noAvoidableDup` | duplicate | open: tracker eo-warm-shutdown-flush-duplicate; the variant DropUnwritten removes it | the reply posted on celery issue 3802 | `confirm_flush_ack_modes.py`. Main: the child runs the task and the message is never acked, in both ack modes. With the variant: acked in both. |
| [interrupt-cleanup](interrupt-cleanup/): The first fix for celery issue 11039 failed a chord after its body was sent | `redisfix1_interrupt.noErrorAfterDispatch`, `redisfix3_interrupt_dup.atMostOnce` | duplicate | fixed: 5446659 and ad00b4a on the fix branch for celery issue 11039 (5f844ea and ee48301 had them) | the thread of celery issue 11039 | `confirm_interrupt_cleanup.py`. On Redis at 5f844ea: the body published once and FAILURE stored. Main publishes twice after a later copy, ad00b4a once. |
| [interrupt-then-copy](interrupt-then-copy/): The second fix for celery issue 11039 sent the body after the chord was failed | `redisfix2_interrupt_dup.noDispatchAfterError`, `redisfix2_interrupt_dup.atMostOnce` | duplicate | fixed: ee48301 on the fix branch for celery issue 11039 (5446659 had it) | the thread of celery issue 11039 | `confirm_interrupt_then_copy.py`. On Redis at 5446659: "published 0, errors 1", then "published 1" after the copy. |
| [mixed-fleet](mixed-fleet/): A rolling upgrade to the fix for celery issue 11036 can leave a chord pending | `mixed_dup.quietDone`, `mixed_dup.atMostOnce`, `mixed_lose_result.quietDone` | hang | by design: documented in the fix since 2057a3c | the thread of celery issue 11036 | `confirm_mixed_fleet.py`. Both results SUCCESS, the counter at 3, the body never published. |
| [once-reread](once-reread/): The first fix for celery issue 11036 failed a chord whose header results were all SUCCESS | `oncev1_dup.noSpuriousError` | visible failure | fixed: 1fa71d4 on the fix branch for celery issue 11036 (cd8aa46 had it) | the thread of celery issue 11036; reported in the reply on celery discussion 11041 | `confirm_once_reread.py`. On memcached at cd8aa46: results SUCCESS and SUCCESS, the body published 0 times. At 1fa71d4: published once. |
| [putback-renewal](putback-renewal/): A blind renewal next to kombu PR 2831 strands a message in unacked | `renewblind_held_credit.noStranded` | hang | open: tracker reply-kombu-337-renew-scope; compare and set (RenewOwn) or the pop token (ByToken) removes it | kombu issue 337, the follow-up to the renewal proposal | `confirm_putback_renewal.py`. Control: the message is back on the queue. After a renewal: on no queue, in unacked. |
| [renewal-hung](renewal-hung/): Renewing every unacked message turns the recovery of a hung task into a hang | `renewall_late_hang.quietDone`, `renewidle_late_hang.quietDone` | hang | by design: the reason the renewal proposed for kombu issue 337 is narrowed to messages waiting for an ETA (tracker reply-kombu-337-renew-scope) | kombu issue 337 | `confirm_renewal_hung.py`. Main: the other consumer gets both messages after the visibility timeout. Renew-everything prototype: neither. |
| [restore-by-tag](restore-by-tag/): A channel restores by delivery tag: a consumer that held a message past the timeout restores the copy another worker runs | `tagrestore_late_etashutdown.restoresOwnOnly` | duplicate | open: tracker eo-restore-by-tag-third-delivery (a candidate; where to post is with the user) | undecided: kombu issue 337 or a new kombu issue | `confirm_restore_by_tag.py`, `natural_restore_by_tag.sh`, `bytagapp.py`. Main and the PR: a third delivery while the second still runs. Three workers, no injected delay: SIGKILL 12 of 12 run once; SIGTERM 11 of 12 run twice (PR branch 12 of 12). |
<!-- end generated -->

The other counterexamples are in `../traces/cex/`. `python3 -m adapter.show
<trace>` prints one.
