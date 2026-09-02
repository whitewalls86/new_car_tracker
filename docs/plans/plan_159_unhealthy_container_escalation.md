# Plan 159: A Container The Drain Blocks On Is A Container Health Must Alert On

## Status

Backlog, written 2026-08-30. Found while diagnosing
[Plan 158](plan_158_coordination_gate_deadlock.md), which recorded it
deliberately so it would not be lost and explicitly declined to claim it:
*"That is a third defect, separate from both of the above, and it is written
down here only so it is not lost — it wants its own plan."*

Priority and effort are proposed in [`docs/PLANS.md`](../PLANS.md), which owns
both; this document does not choose them.

## The problem

On 2026-08-30 a `cartracker-archiver-run-*` one-shot container from
[Plan 145](plan_145_april_cutover_reconciliation.md) Stage 6 printed its results
at 2026-08-29 15:32 and then never exited — 0.00% CPU, sleeping, six threads,
the classic non-daemon connection-pool shape.

Its healthcheck failed **1,727 consecutive times across fourteen hours.**
Nothing surfaced it. It was found by hand, by an operator looking for something
else.

### The asymmetry, which is the whole plan

That container was not invisible to the system. `container_processes` counted
it as live admitted work, and it therefore **blocked every deploy for fourteen
hours** — it was the first blocker on the 2026-08-30 hang, and removing it
revealed Plan 158's underneath.

So the same container was:

| Subsystem | Verdict |
|---|---|
| Coordination drain | live work, authoritative enough to refuse a deploy |
| Container health alerting | not in the expected-service set; no alert |

**A container the drain trusts enough to block on is a container health must be
able to alert on.** One of those two readings is wrong, and it is not the
drain's — failing closed on an unrecognised one-off is correct, and Plan 142
argues that case well.

### Why the existing alert did not fire, and why that is not a bug in it

[Plan 140](plan_140_service_health_contract.md) Stage 4a built
`ct-container-unhealthy` on `cartracker_container_health`, and it works: it
reads `0` within one 15s scrape and goes Pending inside a minute. Its coverage
is the **expected-service set** — the services declared to be running
continuously. That was the right scope for Plan 140, whose defect was a
*stopped expected service* publishing no series at all.

A `docker compose run` one-off is deliberately outside that set. It is supposed
to appear, work and leave. Nothing in Plan 140 is wrong; the set simply has an
edge, and a one-shot that outlives its work sits exactly on it.

## Non-goals

- **Not a change to what the drain counts.** Failing closed on a live one-off is
  correct and is Plan 142's design. This plan is about noticing, not excusing.
- **Not the one-shot lifecycle itself.** Whether `pack-worker` and the archiver
  runs *should* be disposable containers is
  [Plan 152](plan_152_scheduled_worker_lifecycle.md)'s subject. The two are
  complementary: 152 reduces how often a one-shot can outlive its work, this
  one makes it visible when one does. **Neither subsumes the other** — a
  perfectly-behaved one-shot population still wants the alarm, and a working
  alarm does not fix the lifecycle.
- **Not a new metric pipeline.** `cartracker_container_health` already exists
  and already inspects every container in the project; the question is which
  ones it is willing to speak about.
- **Not an alert on a healthy long-running one-off.** A legitimately long job
  must not page. Duration alone is not the signal — repeated healthcheck
  failure, or blocking a drain, is.

## Open questions this plan must answer

1. **What is the exposure today?** How many containers currently sit outside
   the expected-service set, and how many of those carry a healthcheck at all?
   This is a cheap read against `docker inspect` and should be the first thing
   measured, because the answer may make the rest small.
2. **What is the right signal?** Consecutive healthcheck failures on any
   project container is the most direct reading of what went unnoticed. An
   alternative is to alert from the drain side — a blocker that has held for
   longer than any plausible unit of work — which catches a hung one-off with
   no healthcheck at all, and would also have caught this.
3. **Do the two signals want to be one alert or two?** They fail differently: a
   failing healthcheck is a container problem, a stuck drain is a deploy
   problem, and Plan 140's own lesson is that an alert named after the wrong
   component sends triage to the wrong place, late.
4. **What is the escalation for a one-off with no healthcheck?** Neither
   existing mechanism can see it, and `healthcheck-exemptions.txt` means "not
   pollable", never "not healthy".

## Success criteria

1. A container that has been unhealthy for a bounded period surfaces, whether
   or not it is a declared expected service.
2. A one-off that outlives its work is distinguishable from one that is
   legitimately still working, without relying on a duration guess.
3. The 2026-08-30 case is reproducible against the mechanism, and fires.
4. No new alert fires for a healthy long-running one-off.

## Intersections

- **[Plan 140](plan_140_service_health_contract.md)** owns
  `cartracker_container_health` and `ct-container-unhealthy`. This extends its
  coverage rather than replacing it; the three-state metric and the
  expected-service set both stand.
- **[Plan 152](plan_152_scheduled_worker_lifecycle.md)** owns one-shot
  lifecycle. The 2026-08-30 container is recorded there as evidence for its
  trigger. Complementary, as above.
- **[Plan 142](plan_142_planned_host_maintenance.md)** owns the drain that
  blocked. If question 2 resolves toward a drain-side signal, the alert belongs
  beside its evidence sources.
- **[Plan 158](plan_158_coordination_gate_deadlock.md)** is where this was
  found and is now closed. Its Stage 2 bound means a stuck source fails a
  deploy loudly after ten minutes instead of hanging — which shortens this
  defect's blast radius but does not detect it, because a deploy nobody is
  running is not a monitor.
