# Plan 166: Container PID 1 Does Not Reap Its Adopted Orphans

## Status

**Backlog.** Written 2026-08-31 from a read-only pass over production taken
while verifying [Plan 142](plan_142_planned_host_maintenance.md) Stage 4. Two
services accumulate zombie processes indefinitely as a direct consequence of
ordinary, correct operation; a third produced a transient batch and reaped it.

Priority **42 (conditional)**. Effort **S** — Stages 0 and 1 are a reading
exercise and a Compose change, and Stage 2's gauge is what makes it more than a
day's work.

Nothing is currently broken. This is recorded because the accumulation is
**unbounded in uptime, invisible to every instrument the fleet has, and reset by
every deploy** — the combination that guarantees it will be rediscovered from
scratch rather than remembered. The MOTD on the 2026-08-28 SSH login said
`=> There are 120 zombie processes.` and that is the only place in the entire
system where the number has ever appeared.

## The measurement

Taken 2026-08-31 16:15 UTC, host up 13 days, containers up since 2026-08-25.

```
=== zombie count:
264
=== zombies by parent pid:
    141 2025096   /usr/local/bin/uvicorn dbt_runner.app:app     (cartracker-dbt-runner)
    114 2031563   bun run apps/api/src/index.ts                 (cartracker-trawl)
      9 2062203   airflow dag-processor                         (cartracker-airflow-dag-processor)
```

Every parent is its container's **PID 1**. None of the three sets
`HostConfig.Init` — in fact **no service in `docker-compose.yml` sets `init:`
at all**, so this is a fleet-wide default rather than three individual
oversights.

The three are not the same problem, and the difference is the useful part.

| Service | Zombies | Cadence | Reaps? |
|---|---|---|---|
| `dbt_runner` | 141 | exactly one `python3.13` at `:00`, **every hour** | never |
| `trawl` | 114 | batches of 3 — `forkserver`, `Socket Process`, `RDD Process` — per browser launch, ~38 launches in 6 days | never |
| `airflow-dag-processor` | 9 | all within 2026-08-29 05:57–06:07, one incident | **yes** — gone within minutes on re-measure |

`dbt_runner`'s 141 against 140.5 hours of container uptime is one per hour with
no drift, and `hourly_analytics_refresh` is scheduled `0 * * * *` and is the DAG
that drives `dbt_runner`. `airflow-dag-processor` reaping its own batch between
two measurements minutes apart is what proves the other two are a real defect
rather than a normal transient.

## The mechanism

`dbt_runner` calls `subprocess.run(["dbt", ...])`, which waits — so the direct
child is reaped correctly and is not the leak. The leak is one generation down:
`dbt` spawns its own workers and exits without waiting for them. Those workers
are orphaned, the kernel reparents them to **PID 1 of the namespace**, and PID 1
here is `uvicorn`, which has no reason to call `wait()` on a process it never
started. When the orphan exits it has no one to collect its status, so it stays
in `Z` forever.

`trawl` is the same shape with a different tenant: the browser it drives spawns
`forkserver` / `Socket Process` / `RDD Process`, which outlive their launcher
and land on `bun` as PID 1.

This is the textbook case `docker run --init` exists for. `init: true` inserts
`docker-init` (tini) as PID 1, which forwards signals to the real entrypoint and
reaps anything reparented to it. **`docker-init` is present on this host** at
`/usr/bin/docker-init`, and `docker info` reports `Init Binary: docker-init`, so
the ARM64 image has it — that was checked, not assumed.

It matters that `trawl` is a **third-party image** ([Plan 136](plan_136_solver_recycle_and_liveness.md)
pins `TRAWL_IMAGE`). Its application code is not ours to fix, so an app-level
reaping fix could never cover it. A Compose-level fix covers both tenants
without touching either application.

## Why this is not urgent, stated precisely

At 264 zombies the host runs 624 processes against `pid_max` 4194304 and
`threads-max` 191241. A zombie holds a PID slot and a `task_struct` and nothing
else — no memory beyond that, no file descriptors, no CPU. At the observed rate
(~44/day across both services) the fleet would need years to reach any limit,
and **every deploy resets the count to zero**, which is why six days of steady
accumulation has never been noticed.

That last property is the actual problem. The count is:

- **unbounded in uptime** — the only thing that stops it is a restart;
- **unmeasured** — no metric, no alert, no dashboard reads it. Not
  `container_health` (which reports a health status, not a process table), not
  node-exporter, not `ct-container-unhealthy`;
- **silently reset** — a deploy erases the evidence, so the number seen is
  always a function of how long ago someone last deployed rather than of
  anything real.

A leak that resets whenever you look at it is a leak that masks the next one. If
a service ever starts leaking at a rate that *does* matter, the baseline it has
to stand out against is 264 and climbing, and nothing will page.

## Stages

### Stage 0 — Decide whether `init: true` is safe per service, before setting it

`init: true` changes PID 1 from the application to `docker-init`. tini forwards
signals to the real entrypoint, so the ordinary case is unaffected, but PID 1 is
also **what receives `SIGTERM` on `docker stop`** — and Plan 142's host window
stops the entire fleet with exactly that. A service whose entrypoint does its
own signal handling as PID 1 needs to be checked, not assumed.

For each candidate service, record: what PID 1 is today, whether it installs
signal handlers, and what its shutdown is expected to do. This is a reading
exercise over `docker-compose.yml` and the entrypoints, not an experiment.

**Exit:** a written per-service verdict for `dbt_runner` and `trawl` at minimum,
and a decision on whether the remaining services get it by default or on
demonstrated need.

### Stage 1 — Set `init: true` on the services Stage 0 cleared

Compose change only; no application code. Verify after deploy that PID 1 in each
container is `docker-init`, and that the zombie count for that container stays
at 0 across at least one full `hourly_analytics_refresh` cycle (`dbt_runner`)
and several browser launches (`trawl`).

**Exit:** both counts hold at 0 for 24 hours of ordinary operation.

### Stage 2 — Make the count visible, so the next one is not found by MOTD

The reason this plan exists is that the number had exactly one reader and it was
a login banner. `container_health` already holds the Docker grant and already
walks the container list; a per-container zombie gauge is the cheap place to put
this, and it composes with the existing `cartracker_container_*` series.

Alerting threshold is deliberately **not** specified here — Stage 1 should drive
the steady state to 0, and a threshold picked before that baseline exists would
be a guess. Decide it from Stage 1's observed floor.

**Exit:** a gauge exists, reads 0 in the steady state, and its alert threshold is
written down with the measurement it came from.

## Files

- `docker-compose.yml` — `init: true` on the cleared services
- `container_health/collector.py` — the gauge, if Stage 2 is taken
- `tests/test_observability_config.py` — the invariant, if Stage 2 is taken

## Out of scope

- **Fixing `dbt`'s or the browser's own reaping.** `trawl` is a third-party
  image and `dbt` is a vendored tool; neither is ours to change, and `init: true`
  makes it unnecessary.
- **The `airflow-dag-processor` batch of 2026-08-29 05:57–06:07.** It reaped
  itself, so it is not this defect. What produced nine orphans in ten minutes is
  a separate question and may be nothing.
- **Any change to `TRAWL_IMAGE`.** Plan 136 owns that pin and its soak.

## Success criteria

1. `dbt_runner` and `trawl` hold at zero zombies across 24 hours that include at
   least one `hourly_analytics_refresh` run and several browser launches.
2. The decision to set or not set `init: true` on every other service is
   written down, with its reason.
3. The count has at least one reader that is not an SSH login banner.

## Intersections

### Plan 142 — host maintenance

Two ways. First, this was found by Plan 142 Stage 4's read-only scoping pass,
which is what that stage is for. Second, `init: true` changes what receives
`SIGTERM` on `docker stop`, and Plan 142's window stops the whole fleet — so
Stage 0's per-service check is a genuine dependency in that direction, and
Stage 1 should not land inside a maintenance window.

### Plan 136 — solver recycle and liveness

`trawl` is the leaking service and Plan 136 owns its image pin, its memory soak
and its recycle policy. A `trawl` restart resets its zombie count, so any
recycle cadence Plan 136 settles on also caps this leak for that service as a
side effect — which is a reason to sequence after Plan 136's Stage 3 verdict
(due 2026-09-17) rather than before it.

### Plan 140 — service health contract

Stage 2's gauge belongs in `container_health`, which Plan 140 owns. The relevant
precedent is Plan 140's own argument that a metric which publishes nothing reads
as a healthy system: a zombie count that is absent is not the same as a zombie
count of zero, and the gauge should be built so absence is visible.
