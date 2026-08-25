# Plan 152: Scheduled Worker Lifecycle and One-Shot Execution

## Status

**BACKLOG, written 2026-08-25.** Priority **64 (medium)**. Effort **S**.

Trigger: Plan 142 Stage 1 lands, then schedule a production-behavior window to
separate monthly packing and daily disk measurement from continuously running
services.

## Problem

`pack-worker` was introduced to isolate month-scale pack, prune, and verify
work from the regular archiver. Its name and workload suggest a scheduled
worker, but its production lifecycle is currently that of an always-on HTTP
service: Compose sets `restart: unless-stopped`, Airflow waits for `/health`,
container-health expects it continuously, and Plan 142 drains its `/ready`
counter.

The container now also owns the daily disk-usage walk, including a larger
weekly scan. Stopping it between monthly pack runs would therefore break a
second schedule and create false missing-service alerts. The implementation is
internally consistent, but its lifecycle no longer matches the original
month-scale responsibility or its name.

Changing that behavior while Plan 142 is establishing coordination would mix
two risk classes. Plan 142 must describe the production system that exists;
this plan owns changing how scheduled work is launched, observed, retried, and
drained.

## Objective

Make infrequent maintenance workloads run in explicitly launched, disposable
containers while preserving Airflow ownership, scoped coordination, logs,
metrics, retries, and failure evidence. Split daily disk measurement from
monthly bronze packing so each workload has a truthful name and lifecycle.

## Principles

1. **Lifecycle is declared policy.** Continuous services, profile-gated
   continuous services, initialization jobs, and scheduled one-shot workloads
   are distinguishable in checked-in contracts.
2. **Airflow owns schedule and outcome, not broad Docker authority.** Container
   launch must use the narrowest reviewed mechanism available.
3. **Stopped is healthy for a one-shot workload.** Monitoring expects an
   execution only while one is scheduled or running; absence between runs is
   not an outage.
4. **Execution remains observable.** A disposable container must retain logs,
   duration, exit status, oldest-start evidence, and a durable Airflow result.
5. **Retries do not overlap.** A retry cannot launch beside an orphaned prior
   execution, and `max_active_runs=1` is not treated as container-level proof.
6. **Plan 142 remains authoritative.** Scheduled work observes scoped admission
   and contributes live execution evidence to drain aggregation.

## Non-goals

- Replacing Docker Compose with Kubernetes or a general-purpose orchestrator.
- Moving ordinary archiver endpoints out of the long-running archiver service.
- Changing bronze pack, prune, verification, or disk-measurement semantics.
- Giving the Airflow application containers unrestricted access to Docker.
- Folding this production behavior change into Plan 142 Stage 1.

## Target shape

The intended boundaries are:

```text
monthly Airflow DAG
    -> narrow one-shot launcher
    -> pack execution container
    -> pack / prune / verify result and durable logs

daily Airflow DAG
    -> narrow one-shot launcher
    -> disk-measurement execution container
    -> node-exporter textfile plus durable logs
```

The two jobs may continue sharing an image if that remains economical. They do
not share a continuously running service identity merely because they reuse
code. The launcher may be a constrained host-side helper, a Docker socket
proxy exposing only the required operation, or another bounded mechanism
selected during Stage 0. Direct unrestricted socket mounting is excluded.

## Stage 0 — Freeze the lifecycle and authority contract

1. Inventory every Compose service as continuous, profile-gated continuous,
   initialization, manually invoked one-shot, or scheduled one-shot.
2. Record the exact current callers, secrets, mounts, network access, output,
   retry behavior, and time bounds for packing and disk measurement.
3. Select the narrow launch authority and document its refusal boundary.
4. Define execution identity, orphan detection, mutual exclusion, cancellation,
   timeout, and retry semantics.
5. Define how Plan 142 sees queued, starting, running, exited, unreadable, and
   absent execution state. Queued backlog does not block drain; starting or
   running execution does; unreadable state is unknown.
6. Define monitoring changes so continuously expected services and scheduled
   executions cannot be confused.

### Stage 0 gate

Proceed only when the launcher cannot create arbitrary containers or mount
arbitrary host paths, and every current `pack-worker` responsibility has an
explicit future owner.

## Stage 1 — Separate disk measurement

1. Give the disk-usage workload its own service/execution identity.
2. Preserve the daily fast walk, Sunday slow walk, textfile atomicity, host
   path allow-list, and carried-forward-series behavior.
3. Move its health and freshness checks from continuous container presence to
   scheduled-run evidence.
4. Prove one ordinary daily run and one slow-path fixture before removing the
   disk responsibility from `pack-worker`.

## Stage 2 — Convert packing to one-shot execution

1. Replace the pack-worker health sensor and HTTP calls with one reviewed
   execution launch per DAG run.
2. Preserve pack, prune, and verify ordering and the current refusal checks.
3. Make duplicate launch, orphaned execution, timeout, non-zero exit, and lost
   status fail visibly without starting a second writer.
4. Preserve logs after `--rm` cleanup or use an equally bounded cleanup policy.
5. Remove the continuously running `pack-worker` only after the one-shot path
   has completed a representative dry run.

## Stage 3 — Align coordination and service-health policy

1. Change Plan 142's registry entry from continuous service to scheduled
   one-shot workload.
2. Replace `/ready` evidence with launcher/container execution evidence.
3. Remove `pack-worker` from the continuously expected container-health set and
   add missed-run, failed-run, stuck-run, and orphan-execution signals.
4. Verify a scoped archive drain both between runs and during an active pack.
5. Delete compatibility HTTP routes and worker-specific configuration only
   after no callers remain.

## Acceptance criteria

1. No pack or disk-measurement container runs between scheduled executions.
2. Monthly packing and daily disk measurement retain their existing schedule,
   mutual exclusion, output, and alert behavior.
3. A missing idle one-shot container is healthy; a missed, failed, duplicated,
   orphaned, or stuck execution is visible.
4. Plan 142 reports zero between runs, a positive count while a relevant job is
   active, and unknown when execution evidence is unreadable.
5. Airflow cannot use the launch mechanism to start an unapproved image,
   command, mount, network, or privileged container.
6. Rollback restores the current long-running HTTP worker without changing
   bronze data or the node-exporter textfile contract.

## Relationship to Plan 142

Plan 142 models current production truth and introduces lifecycle as an
explicit registry dimension. Until this plan lands, `pack-worker` remains
`continuous` and its separate `/ready` counter remains required drain evidence.
This plan changes that declaration and swaps the evidence adapter only after
production execution changes. The coordination state machine, operational
surfaces, admission rules, and fail-closed aggregation do not change.
