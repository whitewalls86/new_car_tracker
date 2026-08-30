"""
Airflow pool names — Plan 142 Stage 0 item 3.

A pool is the maintenance gate. Every task that can mutate production takes one
slot from `maintenance`; a maintenance window holds the pool at zero slots and
those tasks queue instead of running. Pausing DAGs would work too, and is the
mechanism this replaces — see the plan's Stage 0 item 3 table for why a
pause manifest is the worst of the three options.

Why a pool rather than a sensor, verified against apache-airflow-core 3.2.0
rather than assumed:

    A pool-starved task instance stays in SCHEDULED. `_executable_task_
    instances_to_queued` sees `open_slots <= 0`, logs "Not scheduling since
    there are 0 open slots", and skips it (scheduler_job_runner.py:703).
    It never reaches QUEUED, so it never acquires a `queued_dttm`, so
    `_get_tis_stuck_in_queued` — the only thing that fails a task for waiting,
    at `[scheduler] task_queued_timeout` = 600s — cannot see it
    (scheduler_job_runner.py:2472).

That is the acceptance criterion "an hour-long maintenance pause creates no
failed DAG solely because of the pause", and it is exactly what the deploy
intent sensor cannot offer: its own 600s timeout failed two
`check_deploy_intent` tasks in the 2026-08-18 window.

THE POOL IS NOT CREATED FROM GIT, AND THAT IS DELIBERATE.

`airflow pools set` is an upsert, so putting it in `airflow-init` would reset
the slot count on every `docker compose up -d`. The slot count *is* the hold
state, so a declarative create would silently release a maintenance hold the
first time anything recreated the stack — during a maintenance window, which is
when the stack gets recreated. Plan 142's first design principle is that
maintenance never auto-releases, so the pool is created once, out of band, and
its slot count is only ever changed by an operator:

    docker exec cartracker-airflow-scheduler \
      airflow pools set maintenance 16 "Plan 142 maintenance gate"

Done on production 2026-08-24 UTC, before this code shipped -- see the ordering
rule at the bottom of this docstring.

The cost of that choice is real and belongs in preflight: the pool lives only
in the Airflow metadata DB. If it is missing — a rebuilt DB, a fresh
environment — the scheduler logs `Tasks using non-existent pool 'maintenance'
will not be scheduled` (scheduler_job_runner.py:693) and every task below stops
running, with no failure and no alert. Check `airflow pools list` before
trusting a quiet fleet.

Which is also the deploy ordering rule: **create the pool before this code
lands, never after.**
"""

# The gate. Held at 0 for a maintenance window, restored to MAINTENANCE_SLOTS
# to release. Any task that mutates production state belongs here; sensors in
# `reschedule` mode do not, since they must not hold a slot while they wait.
MAINTENANCE_POOL = "maintenance"

# Sized far above concurrent demand so the assignment is inert. Steady-state
# peak is 4 -- orphan_checker fans out to three parallel tasks and
# results_processing contributes one under max_active_runs=1. It was 5 until
# Plan 147 Stage 3 (2026-08-30) removed scrape_detail_pages.claim_batch from
# the pool: the guard against a processing pause looping the scraper now lives
# next to the fetch, so that DAG no longer needs holding.
#
# The ceiling is higher than that peak, and deliberately so: orphan_checker
# sets no max_active_runs, so it inherits the default of 16 runs and could in
# principle want 3 slots per queued run. That is the same thundering-herd risk
# Phase B is meant to measure on release, and it is the reason this is 16
# rather than 4 -- headroom bought before the measurement rather than after
# it. The slot count is therefore unchanged by the removal above.
MAINTENANCE_SLOTS = 16
