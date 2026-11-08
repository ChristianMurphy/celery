==================================
 Result backend conformance suite
==================================

These tests check that every result backend stores task state the same way,
against real services rather than mocks. They are not part of the unit suite.

``model.py`` states the rules: a task id with nothing stored reads as
``PENDING``, each write replaces the last, ``forget()`` removes it, and once
``SUCCESS`` is stored it is never replaced ("keep the first SUCCESS").

``test_store_sequential.py``
    Hypothesis generates sequences of ``store_result()`` and ``forget()``
    calls and compares every backend with the model after each step. A
    failure shrinks to the shortest sequence that shows it.

``test_store_concurrent.py``
    A task can run twice, and both runs write its result. One writer goes
    through Toxiproxy, which delays the server's replies; the other stores
    ``SUCCESS`` during the delayed writer's round trips. The ``SUCCESS`` must
    survive.

Running
=======

Start the services you want (``docker-compose.yml`` has them all), then::

    pytest t/conformance/backends                      # every reachable backend
    pytest t/conformance/backends --backend redis      # just one

Unreachable services are skipped. ``CONFORMANCE_EXAMPLES``,
``CONFORMANCE_RACE_TRIALS`` and ``CONFORMANCE_LATENCY_MS`` tune the run.

Known gaps
==========

Each target in ``targets.py`` lists the gaps known for it, with the upstream
report. They run as strict xfails, so a fix shows up as an unexpected pass.
