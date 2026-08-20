# Plan 144: Deploy Script Hardening

## Status

**Draft — not started.** Priority **78 (high)**. Effort **XS** (hours to 1 day).

Small, concrete, and unblocked as of 2026-08-18. Every defect below was observed
during the Plan 133 production deploy on 2026-08-20.

## Why this exists now

`scripts/redeploy.sh` is the deploy path for every service in this system. It
works, and it has three defects that were tolerable while nobody looked closely
and are not tolerable now that [Plan 142](plan_142_planned_host_maintenance.md)
intends to build a whole-host maintenance procedure on top of it.

The trigger is that **[Plan 140](plan_140_service_health_contract.md) Stage 1
made the main fix possible.** The script carries this comment:

```bash
# TODO Plan 76: replace sleep with health endpoint polling
sleep 10
```

That TODO could not be actioned when it was written: only 7 of 31 services had a
healthcheck, so there was nothing uniform to poll. As of PR #216 all 25 in-scope
services have one. The blocker cleared on 2026-08-18 and nothing noticed.

**The reference is also stale.** Plan 76 is *complete* — it closed 2026-03-30 as
"Service Health Gate," and it never owned this TODO. A comment pointing at a plan
that finished five months ago is worse than no comment: it reads as tracked work.

## The three defects

### 1. `docker compose up -d` without `--no-deps`

```bash
docker compose up -d "$@"
```

[ARCHITECTURE.md](ARCHITECTURE.md) documents the deployment flow as
`docker compose up -d --no-deps <service>`. The script omits it, so Compose walks
the dependency graph.

**Observed 2026-08-20:** deploying `ops archiver pack-worker processing` re-ran
`flyway`. It was harmless — 43 migrations validated, none pending, exit 0 — but
it is unintended work inside a deploy window, it will recur on every deploy, and
it means the blast radius of a deploy is larger than the service list implies.

The fix is `--no-deps`, plus a decision recorded here about whether dependency
health should be *checked* rather than *recreated*.

### 2. `sleep 10` instead of health polling

Ten seconds is not a readiness contract. It is longer than some services need and
shorter than others: `loki` and `pgadmin` were both observed reporting
`health: starting` past that mark during the Plan 140 Stage 1 deploy.

A deploy that returns "Done." while a service is still starting is a deploy that
cannot tell success from a slow failure. Poll
`docker inspect --format '{{.State.Health.Status}}'` until every recreated
service is `healthy` or a bounded timeout expires, and **fail loudly on timeout**
rather than proceeding.

Services on the Stage 3 deny-list have no health status by design — `flyway` and
`airflow-init` are completed one-shots, `oauth2-proxy` is the documented
distroless exception. The poller must treat "no healthcheck configured" as
*not pollable* rather than as *not healthy*, and it should read that exemption
from one place shared with `TestServiceHealthCoverage` rather than keeping a
second copy of the list.

### 3. Intent is released from an `EXIT` trap, including on failure

```bash
trap _on_exit EXIT
```

This is **probably correct and definitely undocumented.** Releasing deploy intent
when a build fails is right: nothing was recreated, so blocking DAGs serves no
purpose, and a Telegram alert fires. But the reasoning exists nowhere, and the
failure mode it does *not* cover is a partial recreation — `up -d` failing
halfway leaves a mixed fleet with intent released and work resuming.

Decide the behaviour deliberately, write it down, and distinguish the two cases:
build failure (safe to release) from recreation failure (not obviously safe).

## Out of scope

- **Replacing the deploy mechanism.** [Plan 88](PLANS.md) (Kubernetes) is the
  plan that would make this script unnecessary, and its trigger has not fired.
  This plan makes the current mechanism honest; it does not defend it forever.
- **Deploy intent semantics.** The `/deploy/start` and `/deploy/complete`
  endpoints and the drain contract are unchanged here.
- **Whole-host maintenance.** Plan 142 owns the reboot/apt procedure and should
  consume this script rather than fork it.
- **A deploy trigger endpoint.** That is [Plan 108](plan_108_deploy_trigger_endpoint.md),
  which is backlogged pending Plan 136's narrower restart-authority design.

## Success criteria

| Metric | Gate |
|--------|------|
| Deploying a service list | Recreates exactly that list; no dependency is recreated as a side effect |
| Deploy completion | Returns only after every recreated, pollable service reports `healthy` |
| Deploy timeout | Fails loudly and alerts; never reports "Done." on an unhealthy fleet |
| Deny-list exemptions | Read from one shared source, not a second hand-maintained copy |
| The stale TODO | Gone, with the intent-release behaviour documented in its place |

## Verification

Deploy one low-risk service (`pgadmin` is the obvious candidate — it is not on
any critical path and it was one of the two services observed exceeding the
10-second window). Confirm the script waits for real health rather than a fixed
sleep, that no dependency is recreated, and that a deliberately failed build
still releases intent and alerts.
