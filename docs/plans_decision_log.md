# Plans decision log

Narrative that [PLANS.md](PLANS.md) used to carry inline. That file says at the
top that it is *the index only*, and by 2026-08-21 more than half of it was
prose -- which costs on every fresh read, because the sequencing essay is
consulted rarely and the dated records go stale by design.

Nothing here is authoritative about *status*. Status lives in PLANS.md and in
each plan's own document; this is the reasoning and the point-in-time records.

---

### Soak results, 2026-08-21

**[Plan 140](plan_140_service_health_contract.md) Stage 2 — green, and the
strong kind of green.** `ct-container-unhealthy` produced **zero state-change
annotations** across 21h 28m in which Grafana logged 51 transitions overall; it
reads `inactive` with 28 of 28 instances `Normal`. The exporter never missed a
scrape. Three containers did read `0` — `scraper` and `processing` at 20:42:45
during the Plan 136 deploy, `airflow-scheduler` at 21:12:15 — each for exactly
**one 15-second sample** against a `for: 5m` that is twenty samples wide. That
is the metric tracking Docker correctly while the alert stays quiet, which is
the whole contract. The documented removed-container limitation also reproduced
live and harmlessly: one evaluation out of 5,154 saw 27 series rather than 28,
because `dbt_runner` left the metric while being recreated.

**[Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 2 — half green,
half inconclusive, and the inconclusive half is the one that mattered for
sequencing.** Both rules held across 1,189 evaluations with zero annotations,
and the volume guard earned its place: in 14 checkpoints overnight the
20-minute `ok` count was genuinely 0, total volume was also 0, and `> bool 20`
kept the rule silent where the rejected `> 0` form would have paged. Solver
rate landed at 2.29/hour against ~2.4 predicted from `_CF_SESSION_TTL`, with
**0 `challenge` and 0 `error` all window**.

But **open question 2 is not answered, and could not have been.** It asks
whether the solve rate decays gradually or falls off a cliff, because that
choice sets Stage 3's recycle interval. `trawl` has 3.5 days of uptime and zero
restarts; the 2026-08-14 outage followed **22 days**. A day-long window taken
early in a three-week cycle has no decay in it to read, and waiting out the
remaining four hours would not have changed that. The counters are healthy and
flat, which is the correct baseline and not a measurement. **Stage 3 therefore
still has nothing to choose its interval from** — it needs an observation window
measured in weeks, not the deploy it was waiting on.

### What to work now the soaks have closed

**[Plan 144](plan_144_deploy_script_hardening.md) was deployed 2026-08-21 and
is complete.** It ran *through* the hardened script rather than around it, which
was the point. `--restart prometheus` observed a real `starting` → `healthy`
transition in 6 seconds and verified inode 519823 on both sides of the mount
that produced the trap twice.

Two further defects surfaced during the build, both the same shape as the first
four — a recreate silently orphans peers holding a cached address (Plan 136 D6),
and `up -d` on an unchanged service reports a successful deploy having done
nothing. The second was found by dry-run *after* the first five had shipped, and
`pgadmin` — the verification candidate this page nominated — turned out to be
exactly such a no-op, so it could never have exercised the health gate at all.
The plan grew from three defects to six without growing past XS.

**Next: Plan 142 Stage 0**, which was the other half of the recommendation.

Plan 142 sits higher in the build order and its Stage 0 was executable, so
putting Plan 144 first was a deviation. It was argued rather than assumed when
the soaks opened, and the soak results strengthened rather than weakened it.
The argument, kept as the record:

1. **Plan 144 is XS and it is now unblocked**, where Plan 142 is M *plus a
   first observed maintenance window*. Clearing the small owed deploy before
   opening a large build is the cheaper ordering.
2. **Plan 142 Stage 2 consumes `redeploy.sh` rather than forking it**, which
   the build-order row already says. Hardening it first is dependency order,
   not queue-jumping.
3. **It gained a fourth piece of evidence on 2026-08-20, from the Plan 136
   Stage 2 deploy — and the sharpest kind: the script was not used.** That deploy was
   driven by hand, because `redeploy.sh` would have run `docker compose up -d`
   without `--no-deps` across `scraper processing dbt_runner`, and would have
   reported "Done." after `sleep 10` with no idea whether anything was healthy.
   A deploy tool that the operator routes around on a real deploy is the
   argument for fixing it.
4. **The single-file bind-mount trap is now reproduced twice.** Plan 140 found
   it on 2026-08-20 (`prometheus.yml` mounted as a *file*, so `git pull` lands
   the new content on a new inode while the container keeps reading the old
   one, and SIGHUP logs a successful reload of the stale config). The Plan 136
   deploy hit it again hours later — host `519823`, container `519700` — and it
   was caught only because the earlier finding said to look. That belongs in
   the deploy script or its documented procedure, not in an operator's memory.

**[Plan 145](plan_145_april_cutover_reconciliation.md) Stage 0 is the other
thing worth doing right now, and it is the cheapest row on this page.** It is
measurement only — no writes, no deploy, no container touched — and its five
gates decide whether the plan is an L or an XL before any code is written.
Gate 0a in particular decides whether the ~224,600 legacy gap is real or a join
artifact.

Plan 141 remains the other genuinely unblocked row and is the right pick if a
larger slice is wanted instead; its live `ct-403-log-spike` false positive is
still the best fixture source available, **and reading the soak's alert history
sharpened what that fixture has to cover.**

The earlier "fires *diurnally*, between 03:00 and 08:00 UTC" note was drawn from
too small a sample and is **wrong**. Over the 21h 28m window this rule produced
**49 of the 51 alert state-change annotations on the whole instance** — 9
transitions to `Alerting`, 20 to `Pending`, and the rest back — and while the
03:00–08:00 band is its loudest, it also alerted at **00:21 and again at 16:21
UTC**, with `Pending`/`Normal` flapping straight through the afternoon
(09:01, 09:46, 10:31, 11:46, 12:46, 13:01, 13:46, 14:31). It is a flapping
all-day rule, not a nocturnal one.

The mechanism is visible in its own labels, and it is a **parsing** defect
rather than a threshold one. The firing instance carries
`level=WARNING, logger=scraper`, but the rule also holds instances labelled
`level=INFO, logger=shared.minio` and `level=INFO, logger=scraper`. The
expression is `count_over_time({service="scraper"} |= "403" [5m])` — an
unanchored substring match, so any INFO line whose object path, byte count or
identifier merely *contains* "403" is counted as a 403 response. That is Plan
141's parsing-and-labels thesis with a live reproduction attached, and fixtures
should capture all three label sets across a full day rather than one overnight
window.

---

## Sequencing Rationale

**Plans 135/140/143/136/142/141 operational sequence** - Plan 135 closed the
storage blind spot. Plan 140 established the uniform container-health floor and
**deployed on 2026-08-20 and soaked green on 2026-08-21**, leaving only its
gated Stage 4.
Plan 143 then made analytics freshness truthful at one serving boundary and
removed recurring metrics/public-page reads from DuckDB. Plan 136 resumes with
solver-outcome signals before any automatic restart is trusted — those signals
are now deployed and quiet, but their *shape* needs weeks of observation before
Stage 3 picks a recycle interval. Plan 142 reuses
those health and drain primitives for deliberate
whole-host maintenance without granting application code reboot authority. Plan
141 then makes log parsing and dashboard semantics a tested contract. Plan 134
starts its warning-only observation window after 141 because
enforcement depends on a week of trustworthy evidence. Plan 133 was that small,
explicit prerequisite for the packed artifact reparse and closed on 2026-08-20.
**That reparse now belongs to Plan 145, which superseded Plans 132 and 137 on
2026-08-21**, after verifying Plan 132's assumptions against production and
finding that its 36,241 orphans are one of three populations left by the same
April cutover — the largest being ~224,000 successful legacy captures with no
observation. **Plan 145 states deletion as its goal rather than deferring it**,
because splitting disposition into its own document is precisely what left Plan
137 unscheduled, and because Plan 102 already demonstrated the failure mode:
removing the only lifecycle rule bronze had and replacing it with nothing is the
origin of the inode problem Plan 131 exists to solve. Plan 145 Stage 2
also carries a measurement Plan 133 could not make: the verifier drops caches by
design, so the `PACK_INDEX_CACHE_PACKS` 4-to-48 change is proven safe but not
yet proven effective, and a month-sized sequential reparse is where that shows. Most Plan 138 work
is independent, but its stats presentation consumes Plan 143; it belongs before
the next major platform milestone so the public surface and its source-controlled
work feed start from an accurate baseline.

**Plans 126-127 after the lakehouse/adaptive-refresh substrate** - The old
Plan 87 Kafka placeholder is superseded by Plan 126. The natural streaming seam
is the existing staging-event/outbox pattern, not direct app-to-broker writes.
Plan 126 should first prove Kafka-compatible event transport, replay, and a
low-risk consumer while preserving Airflow/batch parity. Plan 127 can then use
those events for adaptive scrape-control feedback once Plan 125 provides the
stable analytics substrate and Plans 112/113 clarify refresh-policy promotion.

**Plan 79 whenever needed** - IP flagging is not currently active.
Prerequisites all exist. Provision Oracle Cloud VMs and fan out the DAG when
needed. Note that Plan 79 is also the plan that creates real multi-host need,
which makes it the honest precondition for Plan 88 below.

**The accumulating control-plane cost, and why Plan 88 still scores 25**
(recorded 2026-08-20, after the question was raised directly). Several queued
plans are, in effect, hand-built orchestration:

| Plan | What it builds | Kubernetes equivalent |
|---|---|---|
| [142](plan_142_planned_host_maintenance.md) | Maintenance intent, drain, quiescence | `kubectl drain` / cordon |
| [136](plan_136_solver_recycle_and_liveness.md) Stage 4 | Restart authority for an unhealthy service | Liveness probe + `restartPolicy` |
| [140](plan_140_service_health_contract.md) Stage 2 | Container health as a metric — **delivered 2026-08-20** | `kube-state-metrics` |
| [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Updating a Deployment's image tag |
| [144](plan_144_deploy_script_hardening.md) | Readiness-gated deploys | Rolling update with readiness probes |

Priced together that is genuinely comparable to a single-node k3s migration, so
**the honest reading is that 25 understates it** — and nobody was summing those
rows. One of them is now paid: Plan 140 Stage 2 shipped on 2026-08-20 at roughly
250 lines of exporter plus a socket proxy, which is a data point for the
argument rather than against it — the `kube-state-metrics` equivalent was small,
and the part that was hard (three states, project scoping, a false-paging
expression) is domain judgment an orchestrator would not have supplied. Four
arguments still keep the score there:

1. **It would not have caught either 2026-08 incident.** The solver was a
   *healthy* container reporting `status:ok` at a 0% solve rate for eight hours;
   a Kubernetes liveness probe tells exactly the same lie, because it is the same
   probe. The apiserver failure it would have restarted — but Plan 136 Stage 4
   buys that specific win at S/M rather than XL.
2. **Every real win in this system has come from domain observability, not
   infrastructure** — solve-rate counters, extraction yield, pack-verification
   refusal, snapshot freshness. An orchestrator supplies none of them, and Plan
   136 Stage 2 remains the highest-value unbuilt work on this list.
3. **One node gives reconciliation without scheduling.** The service count is not
   the argument it appears to be: of 31 services there are 10 `build:` entries,
   three of which are one-shot/profile-gated and one of which shares another's
   image. Roughly six long-running first-party services sit behind ~20 vendor
   containers. Kubernetes does not reduce that count; it re-expresses it as
   Deployments, Services, PVCs, and ConfigMaps — typically 3-5x the YAML, plus a
   control plane on a box already running everything in 23 GB.
4. **It stalls the data-integrity chain.** Plans 132 and 137 are about not losing
   data, and an XL migration puts them behind it.

**The decision rule, so this stops being re-argued from feeling:** revisit Plan
88 when a second host actually exists — which Plan 79 is what creates. A
single-node migration is also the weakest version of the skills case for it,
since it teaches manifest syntax rather than the multi-node scheduling, ingress,
RBAC, and resource-budgeting work the technology is actually for.

**Plan 69 is the counter-example and moved accordingly.** Terraform was pulled
out of this backlog on 2026-08-20 into build-order row 13, because its trigger
("a second environment is approved") fires the moment Plan 121 starts, and
because it is the case where a marketable skill and a real engineering need
point the same direction. Plan 88 is not yet that case; Plan 69 is.

**Plans 114/129/130/131 bronze storage sequence** - Plan 114 measured sectioned
dedup and rejected it (-223%; MinIO's ~8 KB/object floor). Plan 129 shipped the
trained dictionary it recommended instead, cutting bytes ~73% logical / ~60%
physical while leaving object count untouched. Plan 131 is the remaining lever
and the only one that addresses **inodes**, which compression cannot; its Stage 0
must first re-read `df -i` and price a results-page retention policy, which may
be the cheaper answer. Plan 130 is the largest measured win and the only
irreversible one, so it stays blocked until the reversible options are exhausted
and the taxonomy gap is closed. **Plan 131 Stage 5 pushes that exhaustion out
~3 years**: run continuously it converts the inode deadline into a steady state
and leaves bytes as the binding constraint, at roughly 36 months of full
retention on the current disk. Retention/expiry for raw HTML is still unwritten
and is the gap behind all four.

**Plans 110-125 lakehouse/adaptive-refresh sequence** - Plans 110 and 111 are
the completed foundation: storage normalization and adaptive-refresh feature
outputs. Plan 117 resets the forward roadmap toward a portable local lakehouse.
Plan 120 provides production-derived fixture snapshots consumed by CI and local
development. Plan 112 proved Iceberg/MLflow reproducibility, but it is paused
before backtest/model gates because DuckDB still owns the analytics contract.
Plan 143 first establishes the versioned serving snapshot that Plan 125 Gate D
can keep stable while moving dbt/analytics from DuckDB toward Iceberg-native
tables. Once that substrate is stable, Plan 112 resumes policy backtesting and Plan 113
deploys only an approved, pinned policy config into ops claim logic. Plans 114,
121, and 119 can follow in whichever order is most useful: raw HTML retention
research, staging environment, and governance/catalog expansion.

**Plan 138 is mostly independent public-surface work.** It should land before
another major platform milestone adds more documentation drift. Its stats UI
depends on Plan 143, but it must not recreate that producer. Its copy must
describe Plan 125 as a proven migration track, not as the current production
serving path, and it does not block or change lakehouse, storage, or liveness
behavior.

---
