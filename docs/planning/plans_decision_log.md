# Plans decision log

Narrative that [PLANS.md](../PLANS.md) used to carry inline. That file says at the
top that it is *the index only*, and by 2026-08-21 more than half of it was
prose -- which costs on every fresh read, because the sequencing essay is
consulted rarely and the dated records go stale by design.

Nothing here is authoritative about *status*. Status lives in PLANS.md and in
each plan's own document; this is the reasoning and the point-in-time records.

---

### Soak results, 2026-08-21

**[Plan 140](../plans/plan_140_service_health_contract.md) Stage 2 — green, and the
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

**[Plan 136](../plans/plan_136_solver_recycle_and_liveness.md) Stage 2 — half green,
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

**[Plan 144](../plans/plan_144_deploy_script_hardening.md) was deployed 2026-08-21 and
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

**[Plan 145](../plans/plan_145_april_cutover_reconciliation.md) Stage 0 is the other
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
| [142](../plans/plan_142_planned_host_maintenance.md) | Maintenance intent, drain, quiescence | `kubectl drain` / cordon |
| [136](../plans/plan_136_solver_recycle_and_liveness.md) Stage 4 | Restart authority for an unhealthy service | Liveness probe + `restartPolicy` |
| [140](../plans/plan_140_service_health_contract.md) Stage 2 | Container health as a metric — **delivered 2026-08-20** | `kube-state-metrics` |
| [108](../plans/plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Updating a Deployment's image tag |
| [144](../plans/plan_144_deploy_script_hardening.md) | Readiness-gated deploys | Rolling update with readiness probes |

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

---

## Deploy narrative through 2026-08-21

This was the `**Now:**` block of PLANS.md's Current State section: ~100 lines of
per-plan deploy history, nearly every sentence carrying a date, a PR number or a
merge SHA. It is a changelog, and it was being read as orientation. Each claim
below is also recorded in the plan document it belongs to, which is the
authoritative copy.

**Now:** Plan 120's final Gate F production verification is complete, and Plan
139 Stages A+B have taken the CI path every plan below pays from 333s to a
stable ~260s, with coverage now reported on every run. Plan 136 Stage 0 shipped
and was **verified against production** on 2026-08-18 — Airflow itself reports
the 20+20 pool, the shared anchor did not leak it to the other three services,
and `ct-pipeline-failures` now renders exactly two named instances where it
previously rendered a third reading `DAG [no value] failed`. Plan 140 Stage 3 is
verified, Stage 1's 24-hour soak closed clean, and **Stage 2 deployed to
production on 2026-08-20** (PRs #221 and #222). `cartracker_container_health`
now publishes three states for all 28 running services — 27 healthy and one
`-1` for `oauth2-proxy`, the documented distroless exception — from a dedicated
exporter computing at scrape time behind `docker-socket-proxy`, and
`ct-service-down` went from covering two of eight scrape jobs to all nine under
an exact-set-equality test. A Service Health row opens the Infrastructure
dashboard. **Its soak closed green on 2026-08-21 and Plan 140 is now complete
through Stage 3.** The soak existed because the first deployed expression
produced a false page in six minutes: a filtering comparison dropped the series
when a container recovered, and Grafana's `reduce: last` kept the dead value
alive for the rest of its 600s window. `== bool` fixed it, and the soak proved
it — `ct-container-unhealthy` logged **zero state transitions** in a window
where Grafana recorded 51, with 28 of 28 instances `Normal`. Three containers
read `0` for exactly one 15-second sample each during deploys, against a
`for: 5m` twenty samples wide, which is the metric tracking Docker correctly
while the alert stays quiet. **Plan 143 is complete as of 2026-08-20.** PR #217 (merge
`e5d3a46`) shipped the saved-SQL, post-build snapshot that makes `dbt_runner` the
direct metrics owner while `ops` renders `/info` without opening DuckDB or using
the transient artifact queue as freshness; PR #218 (merge `a3cdd59`) corrected
the unscoped Grafana consumers and the 900-second threshold that the first soak
exposed. The corrected soak recorded 24 hourly publications, no failed publish,
a 3,580.9s worst-case freshness gap against the 4,500s threshold, zero DuckDB
lock conflicts, and `/info` at 0.157s.
**Plan 136 Stage 2 deployed to production on 2026-08-20** (PR #223, merge
`50bba68`) — the solver-outcome counters, which are the layer no healthcheck can
supply, because the solver was healthy for all eight hours. Prometheus now
scrapes the scraper at all, which it never did before; all six outcome series
published at `0` before any traffic, both new alert expressions returned exactly
one series each, and `ct-container-unhealthy` stayed Normal across all 28
instances through three container recreates — an unplanned live test of Plan
140's `== bool` fix. The 21:00 build then closed D1's partial-hour defect for
real — `data_through` reads 20:00 rather than 21:00, and the hourly observation
count is 8,133 where the unfixed query would have published about one minute of
data against a threshold of 100. **Its soak closed on 2026-08-21 split: both
rules held across 1,189 evaluations with zero state transitions, and the volume
guard was vindicated by real overnight data — 14 checkpoints where the 20-minute
`ok` count was genuinely 0 and `> bool 20` correctly kept the rule silent where
the rejected `> 0` form would have paged. But open question 2 went unanswered,
because a healthy 24 hours taken 3.5 days into a cycle whose known failure
horizon is 22 days contains no solver decay to read. Stage 3 still cannot choose
its recycle interval, and now needs an observation window measured in weeks.**
Two corrections came out of building it. The alert expression
the plan specified was the same filtering-comparison shape that false-paged Plan
140 six minutes after its deploy, so both new rules are written as `bool`
products. And one rule cannot cover both solver failure modes: a *refusing*
solver never caches credentials and so drives solver request volume **up**,
while a *lying* solver caches normally for 25 minutes and moves that counter not
at all — only its 403s on detail fetches show it. Hence
`ct-solver-not-solving` (fast, solver counter) and `ct-detail-fetch-failing`
(shape-independent, fetch counter). Stage 2 also closed D1's remaining half:
the Plan 143 snapshot selected `MAX(hour)` from an hourly mart while the build
runs at `0 * * * *`, so it published the in-progress hour as an hourly total —
a manufactured drop under a threshold of 100, and the likely reason that alert
fired forty minutes into the healthy recovery.
**Stage 3 was deliberately left for a second change**: its recycle interval is
chosen from what these counters show, and it needs a `POST` verb on the socket
proxy that Plan 140 left read-only. **That is what takes Plan 136 out of the
executable rows** — its next slice is an observation window, not code, so the
top of the build order moves down to the rows that can be worked. The soak
lengthened that window rather than ending it: the counters now have a *baseline*
but not a *shape*, and the interval needs the shape.
Only after that does Stage 4 get restart authority. It inherits the
`docker-socket-proxy` Plan 140 Stage 2 introduced, so that authority is a
single added verb on an existing grant rather than a second socket path. Plan 142 then
turns the same drain and health primitives into a safe, explicit whole-host
maintenance workflow. Plan 141 then makes the newly bounded log pipeline and
its dashboards share one tested schema before Plan 134 begins its warning-log
observation window. **Plan 133 deployed and verified on 2026-08-20** — 720
artifacts read through the pack path across April-July with 0 failures — so the
next data-integrity step is unblocked. **That step is now
[Plan 145](../plans/plan_145_april_cutover_reconciliation.md), which superseded Plan 132
on 2026-08-21.** Verifying Plan 132 against production found its own numbers
intact but its surroundings changed — Plan 131's prune had already completed its
Stage 4, and its proposed reparse would have corrupted live pricing state — and
then found a third population behind it: ~224,600 successful April captures that
exist only in the legacy Parquet, with no silver observation. Plan 145 replaces
three plans' worth of separate recovery machinery with one ledger and one
backfill write path.
Plan 138 should land before the next major platform milestone, consuming rather
than recreating Plan 143's public-stats cache. Plan 125 then resumes at its
remaining Gate C production measurement and Gate D reader migration -- not at
the already-proven early gates; Gate D swaps Plan 143's producer adapter rather
than rebuilding its page and metric contracts.

Plan 112 remains intentionally paused until Plan 125 supplies stable
Iceberg-native inputs. Plans 114, 115, and 128 have completed their intended
work and no longer belong in the executable queue. Plans 124 and 131 closed out
2026-08-18, as did Plan 120's final Gate F production verification. Plan 129
is a production system under rollout rather than a new build.

---

## Closeout-table history, to 2026-08-21

Preamble that accumulated in PLANS.md's closeout section, one paragraph
per gate that closed and left the table. It includes the decision to call
the two 24-hour soaks early, which is worth keeping and was not recorded
anywhere else.

### The preamble as it stood

Plan 120's authenticated production download and checksum round trip was
verified on 2026-08-18. **Plan 140 Stage 1 and Plan 143 Stage 5 closed green on
2026-08-20; Plan 140 Stage 2 and Plan 136 Stage 2 closed on 2026-08-21** and
have all left this table. Their evidence is recorded in their plan documents.
What remains here is **verification, not work**: no code is owed on these rows.
Two of the three are Plan 135's, ride weekly schedules, and land on the same
Sunday. The third is Plan 136 Stage 3's decay window, which is measured in
weeks rather than days and is why the build order's top row has no executable
step until September.

**The two 24-hour soaks were read at 2026-08-21 16:35 UTC and called early** —
21h 28m of 24h for Plan 140, 19h 50m for Plan 136. That was a decision, not an
oversight, and it is defensible for one of them and irrelevant for the other:
Plan 140's rule had zero state transitions in Grafana's history and nothing was
trending, while Plan 136's unanswered half could not have been answered by four
more hours (see below). Both plan documents state the elapsed window rather
than claiming a full day.


### Soak results, 2026-08-21

Both closed; the evidence lives where this file's own instruction says to put
it -- [Plan 140](../plans/plan_140_service_health_contract.md) Stage 2 and
[Plan 136](../plans/plan_136_solver_recycle_and_liveness.md) Stage 2 -- with the full
narrative in [plans_decision_log.md](plans_decision_log.md).

**Plan 140 Stage 2: green.** `ct-container-unhealthy` logged zero state
transitions in a window where Grafana recorded 51, 28 of 28 instances `Normal`.

**Plan 136 Stage 2: splits.** Alert half green across 1,189 evaluations. Open
question 2 unanswered and unanswerable in a day -- see the Stage 3 row in the
closeout table above.

---

## Build-order commentary, to 2026-08-21

Eighty-six lines of prose that sat directly under the build-order table. Nearly
all of it argues an ordering that has since been settled by delivery -- Plan 140
led Plan 136 and shipped, Plan 143 completed, Plan 139 Stages A+B left the
table -- or restates blockers that are now columns in the table itself.

> **One block below is wrong and is kept only as a record.** The Plan 141
> paragraph says `ct-403-log-spike` notifies "between 03:00 and 08:00 UTC".
> The Plan 140 soak's alert history disproved that on 2026-08-21: the rule
> produced 49 of the 51 annotations on the whole instance, firing at 00:21 and
> 16:21 as well, and flapping through the afternoon. It is **not diurnal**. The
> mechanism it describes -- a bare substring match catching `403` inside UUIDs
> and sha256 prefixes -- is accurate and now lives in Plan 141's build-order
> row.

**Plan [139](../plans/plan_139_test_suite_maintenance.md) occupies two rows rather than
one**, because scoring it as a single plan (62) hid that its first two stages
were two lines of YAML and a dev dependency. That argument paid off: **Stages
A+B shipped 2026-08-18** (PR #213) and left the build order. The critical path
is now a stable ~260s against a 333s baseline, and coverage reports on every run
at 88% with no gate. Only the scheduling edge earned it — the pip-cache
hypothesis was wrong and was reverted under Stage B's own verification rule.
Stages C and D never had that argument and take their turn by score.

The step that previously looked urgent — covering the 25%-covered analytics
gauge module — first moved into Plan 136 Stage 1 and now belongs to **Plan 143**
with the full producer redesign. It therefore appears in no Plan 139 row. Tests
written against the old silent-stale behavior would encode exactly what Plan
143 deletes, so they belong with the replacement producer and snapshot contract.

Plan 136 Stage 4 is deliberately not part of the first slice: it grants restart
authority and should start only after Stage 2's outcome counters have established
a trustworthy baseline. Plan 134's observation window may run while Plan 136
proceeds, but the endpoint-by-endpoint 500 rollout returns to this order when
the evidence is ready.

**Plan 140 led despite scoring 87 against Plan 136's 98, and that argument is
now settled by delivery.** Plan 136's Stage 0b was to build a container-health
metric and Plan 140 Stage 2 to generalize it — one set of files, one mental
model — but reading 140 against the compose file settled it more sharply:
**Docker reports no health status at all for a container without a
healthcheck**, and only 7 of 31 services had one. A metric built at Stage 0b
would have been blank for the other 24, and a service with no healthcheck would
have been indistinguishable from a healthy one — 140's own words, that it
"would have caught the apiserver incident and missed the solver incident."

So 0b was never sequenced before 140; it **was** 140 Stage 2, and it landed
strictly better there: three states rather than two, with `-1` making "no
healthcheck configured" loud instead of absent. Stage 1's healthchecks shipped
first, so the metric covered 27 of 28 services on the day it deployed, and the
one it does not cover is *visible* rather than missing.

**Plan 136 holds the top row on score but is not workable**: Plan 140's soak
closed green on 2026-08-21, leaving only 140's gated Stage 4 outstanding there,
while 136's own Stage 3 now waits on a decay signal its healthy soak did not
contain. It also inherits `docker-socket-proxy` — Stage 2
deliberately introduced the shape Plan 136 Stage 4 extends, so restart authority
becomes one added verb on an existing read-only grant rather than a second
socket path. Two smaller inheritances are worth naming because they were paid
for by 140's deploy rather than assumed: `ct-service-down` now actually covers
every scrape job, so any new exporter's liveness is watched by default; and the
`== bool` lesson applies directly to Plan 136's own solver-outcome alerts, which
are exactly the appear-and-disappear shape that false-paged here.

**Plan 143 left the executable order after its 2026-08-18 production
deployment and completed on 2026-08-20.** It preceded Plan 136's remaining Stage
2 despite scoring 94 against 98 because shipping the gauge work locally inside
Plan 136 would have created a serving boundary Plans 125 and 138 would both
replace. Plan 136 returns to scraper-owned solver telemetry without carrying
analytics-serving code, and Plans 125 Gate D and 138 now have a stable serving
contract to consume rather than rebuild.

**Plan 142 follows Plan 136 despite scoring 86 against Plan 141's 85.** This is
dependency order, not an emergency ranking. Plan 140 has now supplied the
resume gate — `cartracker_container_health` is the signal 142 resumes against,
and its soak closed green on 2026-08-21 — and Plan 136 Stage 3 establishes the
drain-aware safe-boundary pattern Plan 142 should reuse. Once those exist, host maintenance is recurring
production safety work with a fully observed first-window failure record, so it
belongs ahead of dashboard-contract cleanup. It remains below Plan 136 because
there is no current unpatched emergency and no application endpoint should gain
host package or reboot authority.

**Plan 141 now has a live false positive to build its fixtures from.** The
2026-08-20 watch-item check found `ct-403-log-spike` firing — the only rule not
inactive — and notifying repeatedly between 03:00 and 08:00 UTC. Its expression
is `count_over_time({service="scraper"} |= "403" [5m]) > 10`, a bare substring
match against the whole JSON log line. It matches UUIDs and sha256 prefixes
containing the digits `403` (`...ded709847403`, `a1fd40307748`), so Loki returns
roughly 341 matching lines per hour drawn from ordinary INFO `write_html` and
`scrape_detail_fetch` traffic. Actual 403 responses over the same period: **23
in 12 hours**, against a healthy pipeline at 99.6% extraction yield and 2 block
events per hour.

This is precisely the defect Plan 141 exists to fix, and it argues for that
plan's parsing work over its dashboard work: the logs are **already** structured
JSON carrying a `level` field, so the alert should parse and match a real status
field rather than grepping the raw line. It is also a reminder that alert noise
is not only annoying — an operator who learns to ignore `ct-403-log-spike` is
being trained to ignore the scraper's alert channel, which is the channel the
2026-08-14 solver outage needed.

---

## Plan 146 — what left `PLANS.md`, 2026-08-21

Stage 2 collapsed the index from 232 lines and eight status-bearing sections to
169 lines and five tables. Everything below was evicted **from the index**, not
deleted: it is reasoning and point-in-time record, which is what this file is
for. Status for every plan named here lives in `PLANS.md` or the archive.

### Why observability sat at the top of the build order

Two incidents in four days — 2026-08-14 solver, 2026-08-18 Airflow apiserver —
were each found by a human noticing downstream damage, not by an alert. Plan 136
covered the two components that actually failed; Plan 140 covered the other
twenty-four before they did. Plan 140 is complete through Stage 3, Plan 144
hardened the deploy path that touches all of them, and Plan 136 Stage 3 waits on
a decay signal.

This paragraph lived under "Current State" and is exactly the kind of thing rule
3 evicts: it describes *what happened to the system*, while that section is
supposed to describe *the system*.

### The `== bool` inheritance, carried forward for Plan 136

Plan 140's six-minute false page — an alert expression that fired on a recovered
container — was fixed by writing the rule as a bool product. Plan 136's
solver-outcome alerts have the same appear-and-disappear shape, which is why
both are written that way. Worth keeping in front of whoever picks up Stage 3;
it is not a status fact, so it does not belong in a table.

### The three surfaces that did not survive, and why

**The "Operational watch list"** had no dates and no exit criteria, so rows
could not leave. Three of its six rows had been asking for closeout that had
already happened — for weeks, in Plan 115's case. Its surviving idea splits
cleanly: a plan whose behaviour needs watching is either *in closeout* (a
specific thing to observe, by a date) or *archived* (and monitoring is then an
operational concern, not a plan). Plan 129 went to closeout; Plans 131 and 135
resolved to archive and closeout respectively.

**The "Plan inventory"** covered 30 of 72 plan files and nothing said which 42
were missing, so its silence was indistinguishable from a plan not existing.
Stage 0 measured the real cost of that: **24 plan documents were in no status
table at all**, and nine plans had no document *and* no archive row. `ls docs/`
is complete and free; a partial list is worse than no list.

**The "Paused or blocked" table** duplicated the build order's `Blocked by`
column and held a row — Plan 114's follow-on — whose parent plan was complete.
It folded into the backlog, whose `Trigger` column is the same idea under a
better name.

**The index's own "Completed" table** duplicated `completed_plans.md` and
disagreed with it: Plans 143, 133, 128 and 115 were in the index and absent from
the archive. The archive is now the only completion record.
[Plan 138](../plans/plan_138_public_surface_refresh.md)'s project-updates snapshot was
the one consumer of the index's copy and now reads the archive instead.

### Two decisions Plan 146's own document did not settle

**Superseded is a fifth table, not a fourth state.** The plan specifies four
states — backlog, build order, closeout, archive — and does not say where a
superseded plan goes. Folding them into `completed_plans.md` was rejected:
that file is the record of what was *finished*, and fourteen plans that were
replaced rather than delivered would make it claim work that never happened.
So the invariant is "exactly one of five tables", and Stage 4's test asserts it
that way.

**Plan 117 sits in the backlog.** It is an umbrella rather than a buildable
plan, and the index used to give it a section of its own — a sixth status
surface by another name. A backlog row with a real exit condition ("archived
when its arc lands") keeps the invariant intact without a special case. The
orientation it provided survives as three lines under Current State.

---

### `docs/` gets a hierarchy, 2026-08-21

Plan 146 Stage 3. Ninety-eight files and zero directories became five
directories: `plans/` (79 documents, flat), `planning/` (this file, the
archive, the reconciliation report), `runbooks/`, `prompts/`, `reference/`.
[ARCHITECTURE.md](../ARCHITECTURE.md) and [PLANS.md](../PLANS.md) stay at the
`docs/` root — they are the two entry points, and every external reference to
them still resolves.

**Directories encode kind, not state.** `plans/` is flat on purpose. An
`active/` and `completed/` split would move a file on every completion, break
every inbound link, and put the same fact in two places — the path and the
index — which is the defect this plan exists to remove. A plan document keeps
one path forever; PLANS.md and the archive say what state it is in.

The rename was one commit with no content edits, so the 163 relative links
rewritten in the next commit could be reviewed separately. All 98 moves are
recorded as renames, so `git log --follow` still reaches the whole history.

**Four references were already broken before the move.** None is a regression;
each was decided rather than silently patched:

- [plan_105](../plans/plan_105_vm_migration.md) linked to
  `scripts/provision_oracle_vm.py`, which no commit on any ref has ever
  contained, and to a `reference_server_ssh.md` under a `.claude/` memory
  directory outside the repo on a machine that no longer exists. Both are now
  backticked prose. Neither names a file this repo can resolve, so neither
  should look like a link — and Stage 4's test reads markdown links, not prose.
- [plan_135](../plans/plan_135_storage_observability.md) had nine links written
  repo-root-relative (`docker-compose.yml`, `loki/loki.yml`) from a file in
  `docs/`, so they had never resolved. They point at real files and are now
  correct: `../../docker-compose.yml`.
- `docker-compose.yml:175` cited `docs/plan_128_challenge_pages.md`; the file
  has always been `plan_128_false_block_detection.md`. Corrected to the new
  path.

Files that plans *propose* and nobody has written — `docs/governance_inventory.md`,
`docs/table_registration_standard.md`, `docs/dbt_spark_adapter_decision.md`,
`docs/staging_environment_decision.md`, `docs/runbook_lakehouse.md` — were left
exactly as they are. Every one appears as a backticked filename in a
deliverables list, never as a markdown link, which is what makes Stage 4's
dangling-link test safe to write: it follows markdown link syntax only, so a
backticked filename in prose cannot fail it.

`docs/prompts/claude_prompt_plan_146_stage_3.md` is the one file whose `docs/`
paths were **not** rewritten. It describes the flat layout as it stood when the
stage began, and rewriting it would make it claim that Stage 1 edited
`docs/planning/completed_plans.md` — a file that did not exist that day.

**Two changes to `scripts/audit_plan_state_history.py`.** Its output is the
evidence base for Stage 1's backfill, so it was re-run in all three modes and
diffed against the pre-move tree; the only differences are the ones below.

- The sweep collected plan documents with `docs/*plan_*.md`, which also matched
  `claude_prompt_plan_135_*.md` and `runbook_plan_131_*.md`. Sorted, a prompt
  file came first, so the tool had been reading *prompts* for Plans 120, 131,
  135, 140 and 146 when it wanted their plan documents. Globbing `docs/plans/`
  fixes it, and those five plans now resolve `corroborated` from their own
  documents instead of guessing from a prompt's mtime. The hierarchy made a
  distinction the flat directory could not express.
- `file_dates` skips commits that only renamed a file. Without that, the move
  would date all 79 documents to 2026-08-21 and destroy the only signal an
  `inferred` guess has.

`--coverage` still reports 3 never-used numbers (44, 85, 104) and 0 unrecorded.
Stage 3's prompt expected 4 unrecorded (5, 52, 55, 56); that was Stage 0's
number, and Stage 2 recorded all four in the superseded table. The pre-move
tree reports 0 as well, so nothing about the move changed it.

---

### The invariant becomes a test, 2026-08-21

Plan 146 Stage 4. `tests/test_planning_docs.py` — 18 assertions, 0.11s, no
deny-list. All seven rules held on the first run, so this stage froze a
structure that was already correct rather than fixing data: no edit to
`PLANS.md` or the archive was needed to make it pass.

**Coverage keys on the plan number a document declares, not on its filename.**
79 documents resolve to 73 numbers — Plan 125 has three documents, Plans 110,
120 and 123 two each — so several documents legitimately share one row. The
converse is deliberately *not* asserted: 61 table rows have no document, nearly
all of them archive rows for plans finished before plan documents existed, and
demanding a document for each would turn the record working into a test
failure.

`plan_v018_schema_migration.md` declares no number and contributes none. The
exclusion is by **form** — an identifier that is not a decimal integer — never
by filename, because a list of exempt filenames is the deny-list this test
exists to avoid. A separate assertion checks that every document's name parses
at all, so a file the parser cannot read fails loudly instead of vanishing from
coverage.

**Three archive `Plan` cell forms, and a fourth that yields nothing.** 22 of 108
rows are not a plain integer. `62 + 63` parses as both plans, `14.1` as a
sub-plan of 14, and `V029` / `Silver flush` as identifiers that name no plan.
The fourth form is recognised only by *starting with a letter*, so `62 & 63`
raises rather than silently counting as zero plans — silent dropping is how the
old "Plan inventory" covered 30 of 72 documents and said nothing about the rest.

**One parser bug the documents found.** Build-order row 4 quotes the LogQL
fragment `\|= "403"`. Splitting cells on every `|` shifts that row one column
left, which is exactly the class of misread this file is meant to prevent, so
cells split on unescaped pipes only.

**The dangling-link check follows link syntax, not filenames**, as Stage 3
promised. Fenced blocks and code spans are stripped first, so the five
documents that plans *propose* and nobody has written — `governance_inventory.md`
and the rest — stay invisible to it, and so do the three references Stage 3
broke by decision. 394 links resolve; a companion assertion fails if that count
collapses, because a link checker that has stopped matching passes forever.

Every one of the seven was broken deliberately in the working tree and confirmed
to name its offender — the plan number, the row, or the file and target — then
reverted. A structural assertion nobody has watched fail is not yet working.

Stage 4's line-budget assertion reads the budget out of `PLANS.md` rather than
holding its own copy. The number is an editorial decision and belongs where the
editor is looking; the test only enforces what the document already says.

---

### A skill for the edits, 2026-08-21

Plan 146 Stage 5. `.claude/skills/plans/SKILL.md` — the first skill in this
repo — performs the four routine state transitions and nothing else. Two
questions the plan left open are answered below, and `tests/test_planning_docs.py`
grew from 18 assertions to 27.

**The measured reason.** Stage 1 reconstructed 122 state transitions across 35
days and **92 of them are a row disappearing** rather than moving. Strip the two
bulk events — 45 archive rows in one 2026-04-07 revision, 14 when Stage 2
removed the index's duplicate Completed table — and **33 rows still vanished,
one and two at a time, across 16 separate days.** No single accident produces
that shape; it is what editing by hand sixteen times looks like.

**Stage 4's test is necessary and not sufficient, and measuring that came
first.** Seven mutations were applied to the working tree and the full file run
against each: a superseded row's `Superseded by` emptied, the archive reordered,
`Order` duplicated and jumped to 99, an archive date set to `sometime in
August`, a `Plan` cell whose link text and target name different plans, the
`**88**` backlog row deleted outright, and a backlog row duplicated. **All
eighteen assertions passed on all seven.** A skill written against that test
would have had a green run that meant nothing.

#### The editing rule, which is the whole design

**Splice, never reflow.** Every edit is anchored on the exact text of one line,
made with a tool whose match must be exact and unique or the call fails. The
skill never holds a whole table and never rewrites one.

That is deliberate in preference to the obvious alternative — read the rows,
mutate the list, re-render the table. Re-rendering passes every untouched row
through the writer, so any one of them can come out changed or missing; a
dropped row would be one token away. Anchored edits mean an untouched row is
never written at all, so losing one takes a bug rather than an omission. It
also keeps the diff reviewable, which is the property that lets the *next*
change be read: an archive rehearsal below touched two files and changed
exactly four lines.

Six of the seven mutations are now also caught by the test. **The seventh, G, is
not, and that is recorded rather than fixed.** A duplicate row inside one table
is invisible to a coverage check that works on sets, and the rule that would
reject it — one row per plan per table — is false by design, because Plan 139
legitimately holds build-order rows for Stage C and Stage D. G's only defence is
the splice rule.

#### Mutation F, and why the count was the wrong instrument

**F is the dangerous one.** Six index rows name a plan with no document — 88,
87, 5, 52, 55, 56. Coverage keys on plan documents, so a documentless row has
nothing whatsoever asserting it exists. That is not hypothetical: it is exactly
how Plans 5, 52, 55 and 56 were lost, and Stage 0 recovered them from the
index's git history.

The obvious check is to assert the *count* of documentless rows. It was
rejected. A count is a number in a test file, and the way to silence it is to
edit the number — which is the deny-list failure mode this test file exists to
avoid, and whoever deletes a row is precisely the person who will.

So the check keys on an **external census**:
[plan_state_reconciliation.md](plan_state_reconciliation.md), Stage 0's
deliverable, which settled every plan number this repo has ever named against
its document, its git history and production evidence. Its own header says it is
"a record of one reconciliation, not a surface that gets maintained", and that
is the property that makes it usable here — **it does not grow when a plan is
added**, so nobody has a routine reason to touch it. Silencing the assertion
means falsifying a dated evidence record, which is a different act from
deleting a number from a list.

The census reads **first cells of the reconciliation's tables only**, in three
forms: `**65**`, `81 data migration`, and `66, 122, 79, 94, 108, 88`. Sweeping
the prose instead would pick up "Numbers never used at all | 3 (44, 85, 104)"
and demand table rows for three numbers that name no plan. A companion
assertion fails loudly if the record is ever deleted, because losing the only
defence those six rows have should be a decision somebody makes rather than a
side effect.

#### The archive's row count is now checked

`PLANS.md` claims the archive holds a specific number of rows. It was the
index's only hard-coded count, maintained by hand, and nothing read it. That
mattered less while a human archived plans than it does now that a tool does:
archiving is two files, and the second is a number in a sentence. It is now
held against the archive's real length — one document checked against another,
the same shape as the line budget, not a number owned by the test.

#### Does the skill touch plan documents? Yes — the status marker, nothing else

It has to. `PLANS.md` states that when the index and a plan document disagree,
**the plan document wins**. Move a row and leave the document asserting the old
state and you have not merely created a contradiction, you have made the
*authority* the wrong one — worse than the defect Plan 146 was written to fix.
The index was in exactly that position when this stage began: it recorded
Plan 146 as build-order row 1 with Stages 0-4 done, while
[plan_146](../plans/plan_146_planning_system.md) opened with **"Draft — not
started."**

Status is written three ways across 79 documents and often not at all: a
`## Status` section (25), a `**Status:**` line near the top (37), neither (17).
The skill replaces **the leading state phrase only**, with a state word the
index already uses or with text the user supplies verbatim; every other line
stays byte-identical.

**It never creates a status marker in a document that has none.** Seventeen
documents have never carried one, and inventing structure for them is
authoring. Where there is no marker the skill reports the fact and changes
nothing — the gap stays visible instead of being filled with a guess.

#### "Record a soak result" contradicts "never prose". The user writes the sentence

The plan lists recording a soak result as an operation and forbids what it
requires two paragraphs later: *"it will not author a plan, summarise a result,
or decide an order."* A soak result is a summary.

Resolved by splitting the operation at the seam. **The user writes the sentence;
the skill moves the row and updates the count.** The skill transcribes verbatim,
reads no logs or dashboards, and does not decide that a gate has closed. If the
text has not been supplied it stops and asks — and it does not draft one for
approval either, because an approved draft is still the skill's sentence.

That is the same seam Stage 6 sits on. A tool that both summarises work and
moves rows between tables can move a row because its own summary said so, which
is a self-confirming record.

#### Exercised on the real files

One real transition: Plan 146's own stale status, `**Draft — not started.**` →
`**Build order.**` — a single-line diff that put the index back in agreement
with its authority. The state word carries no row position, since Plan 146's own
rule 5 is that cross-references key on plan numbers and never on ordinals.

Two rehearsals, both reverted:

- **The documentless row, mutation F's own row.** `**88**` was moved from the
  backlog into the superseded table and back. `PLANS.md` finished
  **byte-identical to `HEAD`** — the row survived intact, with its trigger and
  its bare-bold `Plan` cell unchanged, and the test was green at both ends.
- **The three-edit archive path**, rehearsed on Plan 135. Stopped deliberately
  after step two: the half-finished state was caught twice, by the pre-existing
  duplicate-table assertion and by the new count check. Completed, the whole
  operation changed **four lines across two files** and nothing reflowed.

The skill's `Plan` cell examples came out of that. The archive's `Plan` column
is a **bare number**, `| 135 |`, not the index's link or bold form — and a
separator line is not a unique anchor, because closeout and superseded both
have three columns and `|---|---|---|` therefore appears twice in `PLANS.md`.

None of the Stage 4 measurements moved: `PLANS.md` is 169 lines of its 250
budget, the archive holds 108 rows, and `--coverage` still reports 3 never-used
numbers (44, 85, 104) and 0 unrecorded.

---

### Recaps get their own directory, and weeks run Monday to Sunday, 2026-08-21

Maintainer's decision, taken while Stage 6's prompt was being written and
recorded here because it **overrides the plan document as written**.
`plan_146_planning_system.md` has been corrected to match; it is the authority
and this entry is the reasoning behind the change, not a second copy of it.

**Stage 6's output moves out of this file.** The plan said the weekly summary
would be *"a dated entry appended to the decision log, not a new surface."* It
now goes to **`docs/recaps/`, one file per week**, named for the window's end
date.

The original reasoning was sound — a new surface is what Plan 146 spent four
stages deleting — but it applied the rule to the wrong axis. What this plan
forbids is **two surfaces asserting the same fact**, and a recap and a decision
are not the same fact. The log answers *why a choice was made*; a recap answers
*what happened that week*. Merging them means this file accretes an event
stream on top of its argument, which is how `PLANS.md` reached 683 lines: a
paragraph of history per departed row, none of it wrong, none of it removable.
So recaps go to `recaps/` **only** — no pointer entry here, because a pointer
would be the duplication under a shorter name.

`recaps/` encodes kind rather than state, so Stage 3's directory rule holds, and
Stage 4's dangling-link check covers the new directory for free the moment it
exists.

**One file per week rather than one growing log.** The alternative — a single
appended file — is the shape `PLANS.md` had before Stage 2, a surface with no
eviction rule. A week is a natural closed unit: it ends, nothing amends it, and
the filenames sort.

**Weeks run Monday to Sunday, and only complete weeks are recapped.** Run the
skill on Friday 2026-08-21 and it recaps Monday 2026-08-10 through Sunday
2026-08-16, leaving five days for the next run.

That deferral is deliberate and it is also the shape's one hazard, which is why
it is written down here. Measured on 2026-08-21: **55 commits inside the
window, 114 after it.** The tail is twice the window. A recap that ignores 114
commits without saying so is indistinguishable from one that lost them, which
is the same defect as an unattributed commit one level up — so the recap states
its window and its deferred count, and the file is named for the window's end
so the gap shows on the filesystem rather than only in prose.

The second hazard is a missed week. One file per week plus last-complete-week
only means a fortnight's silence leaves a hole nothing announces, so the skill
reads `docs/recaps/` before writing and either fills each missing week in turn
or names them and stops.

---

### A skill for the recap, and reading the history instead of fingerprinting it, 2026-08-21

Plan 146 Stage 6, the last stage. `.claude/skills/plan-week/SKILL.md` writes
`docs/recaps/YYYY-MM-DD.md` and nothing else; `tests/test_planning_docs.py`
grew from 27 assertions to 33. The first real recap,
[2026-08-16](../recaps/2026-08-16.md), covers 55 commits.

The plan document specified this stage's shape and the entry above settled its
output. What follows is the two questions neither answered — how a commit gets
attributed, and what happens when a recap already exists — plus one assertion
deliberately not written.

#### The plan's attribution design is wrong, and measurement says so

`plan_146_planning_system.md` calls the mapping problem *"the whole
difficulty"* and prescribes *"the same layered attribution
`audit_plan_state_history.py` uses — subject, then branch name, then the plan
documents touched by the diff."* Every clause of that failed a measurement.

**There is no layered attributor in that script to reuse.**
`mentioned_numbers()` reads subjects only. `ever_mentioned()` reads subjects,
bodies, index history and the contents of `docs/` — but it answers *"was this
plan number ever real?"*, which is a different question from *"what happened
this week?"*.

**The diff layer contributes nothing.** Over the 30 days to 2026-08-21, 170
commits: subjects attribute 146, bodies add 6, the containing branch adds 18,
and plan documents touched by the diff add **zero**. Work commits touch code,
not plan documents — `2cbb7c3` changed 24 files and none was under
`docs/plans/`.

**And the branch layer evaporates.** All 18 commits it rescues are also on
`master`; `git branch --contains` finds them only because their plan-named
branches are still among this repo's 80 refs. Deleting a branch is what merging
is supposed to do, so the same window recapped today yields 0 unattributed and
in three months yields 18. A signal that decays on tidying up is not a signal.

The plan's own figure — *"conventional prefixes cover 11 commits of 1,041, and
subject-line mentions cover 298"* — is a **lifetime** number, measured before
the convention took hold. Over the last 30 days subjects alone reach **146 of
170, 86%**, and inside the recapped week **40 of 42 non-merge commits, 95%**.
Building to the 29% figure would build for a repo that no longer exists.

#### So the skill reads the commits, and the cost was measured before choosing

On the real window, 2026-08-10 to 08-16, 42 non-merge commits:

| What is read | Tokens |
|---|---:|
| subjects only | ~0.9k |
| subjects + `--stat` | ~3.5k |
| subjects + bodies | ~15.5k |
| **subjects + bodies + `--stat`** | **~19k** |
| full patches | ~262k |
| full patches, code only | ~162k |

The chosen tier is the fourth. It carries the author's stated rationale and the
shape of every change, it is genuinely reading the history rather than
pattern-matching metadata, and unlike a branch name it does not evaporate. Full
patches are 300x for a quiet week and about **106k of that is markdown** —
this plan's own documents being re-read at enormous cost.

#### The reframe: attribution links to the *why*, it does not decide what happened

This is the part that was backwards. **A plan number is not a prerequisite for
recapping a commit.** `Make two CLI path assertions platform-independent` needs
none; its subject and `--stat` say what it did. A plan number buys exactly one
thing — the right to open `docs/plans/plan_NNN_*.md` and say why the work
mattered.

So the rule is two lines long: **the subject, then the body, and stop.** A
branch name is a hint that may confirm a reading and never a dependency, and
its absence is not a failure. An unattributed commit is recapped anyway, under
what it did, and *also* named in a required section — what is missing is the
link to a why, not the work.

Both of the recapped week's unattributed commits argue for the rule. `820c944`
says in its own body *"Unrelated to Plan 131"*, which is an answer rather than a
gap. `a80b123` — `emergency commit to try and free space` — has no body at all,
and the next commit `9f078c6` records the notes for it by sha. That is real
evidence and the recap quotes it, but it does not attribute the commit: one
commit's testimony about another is how a list of special cases starts.

#### Merges are counted and named, never recapped

Thirteen of the window's 55 commits are merges, 232 of the repo's 1,082. A
merge and the commits it brings in are the same work counted twice — and
`Merge pull request #192 from user/plan-131-stage-3` attributes perfectly by
subject, which makes merges simultaneously the easiest thing to attribute and
the most misleading thing to count.

Decided: **the work sections cover non-merge commits; a `## Merges` section
names every merge with its sha and PR** so the denominator still reconciles to
55. The tempting stronger claim — that a merge only ever brings in-window work
— was checked and is false: the window's thirteen merges bring 43 commits, the
42 in-window non-merges plus `8cab72a` from 2026-08-08, which `788bb33` carried
in from the previous week.

#### The window is pinned to one clock, and stated in the file

`git log --since/--until` reads author dates in local time and `--date=short`
renders them the same way, which is the clock `audit_plan_state_history.py`
already uses. Following it keeps every date across these documents comparable.
A commit at 23:40 on a Sunday lands wherever that clock puts it. This is a
choice rather than a law, so each recap states its window in full — `2026-08-10
00:00:00 to 2026-08-16 23:59:59, local author time` — and nobody has to
re-derive it from a boundary commit.

#### A fact about the window comes from the window; a fact about today carries today's date

The obvious way to write *"what moved between states"* is to read `PLANS.md`.
That is wrong by five days: today's index says where a plan **ended up**. The
skill diffs `state_map()` across the index revisions inside the window instead
— the reuse of Stage 0's parser that this stage actually needed. Eight index
revisions in the recapped week, and the diff is the finding: **five plans
entered the index and none left**, in a week that shipped Plan 114's
conclusion, Plan 129's production rollout and four of Plan 131's five stages.
Every state change in the window was an arrival.

The same rule caught the sharpest thing in the recap. Plans 134 and 135 got
documents that week — in `5de59bc` and `cf30421` — and appear in no state
change at all, because neither was filed. A plan document in no table is
precisely the defect Plan 146 exists to fix, found by a recap on its first run.
Both have since been filed, which is a fact about 2026-08-21 and is written in
the recap with that date attached.

#### Regeneration, and what happens after a silent fortnight

**A recap is never regenerated.** It is a dated record of what was knowable on
the day it was written, and rewriting it against a later repo replaces a record
with a reconstruction — one that always looks better, because it knows how
things turned out. A wrong recap is corrected by appending a dated
`## Correction` section; the original paragraphs stay.

**Missing weeks: write the oldest, name the rest, stop.** The entry above left
this open between filling every gap in one run and naming them and stopping.
Neither, quite. One run writes one recap, always the oldest missing week, and
reports what is still owed. That is a cost rule with teeth — a week is ~19k
tokens, so "catch up on the last quarter" is a quarter of a million tokens
spent without being asked — and working from the far end means the gap always
shrinks and the filenames show how much is left. An empty `docs/recaps/` is not
a backlog: the first run simply recaps the last complete week.

#### The assertion deliberately not written

Six new assertions, each watched failing against a mutation of the real recap
before being trusted: a file named for the window's start rather than its end,
the `Unattributed commits` section deleted, the `**Window:**` field removed, an
`*(inferred)*` borrowed from the archive, every recap deleted, and one sha
digit changed. The dangling-link check needed no extension — it walks all of
`docs/`, so it covered `docs/recaps/` the moment the directory existed, and it
is what catches a recap linking to a plan document at the flat pre-Stage-3 path
that the window's own commits show in their `--stat`.

**What is not asserted is "every commit in a recap's window appears in that
recap"** — the check most worth wanting, and one that cannot be a permanent
test. A window's commit set is only well-defined at the moment the recap is
written. Measured on 2026-08-21: **30 commits sit on refs that are not on
`master`, 17 of them authored on 2026-07-21 on a Plan 125 branch that has been
unmerged for a month.** Merging it drops seventeen commits into an
already-recapped week, and a recap that was exactly right when written would
start failing for work its author could not have seen. Silencing that would
mean editing the recap to match — falsifying the record to protect the test.

So the reconciliation lives in the skill and runs at write time, against the
history the author actually read, which is the only moment the denominator
holds still. It ran on the first recap and came out exact: 55 of 55, with
`8cab72a` the single sha named and outside the window, explained in the text.
What the test file keeps is the durable half — every sha a recap names resolves
to a real commit, because a commit that exists keeps existing.

#### Measurements

`PLANS.md` is unchanged at 169 lines of its 250 budget, the archive still holds
108 rows, `--coverage` still reports 3 never-used numbers (44, 85, 104) and 0
unrecorded. Stage 6 touched no table, no plan document and no archive row —
`git status` after the first real run showed only `docs/recaps/2026-08-16.md`.

---

### Where an authored value may come from, 2026-08-21

Maintainer's decision, taken after Stage 6 landed. It refines the Stage 5
boundary above rather than reversing it, and **the plan document needs no
correction**: `plan_146_planning_system.md` says the `plans` skill *"will not
author a plan, summarise a result, or decide an order"*, and that is still
exactly true.

The question was whether the skill should carry sensible defaults for the
values it refuses to invent — a gate, a trigger, a `Lands` date, an archive
description. The answer is no, and the reason is the one Stage 5 measured:
Plan 123 sat in closeout from 2026-07-10 for six weeks because its row had a
date and nothing that could close it. A defaulted gate is that row with more
words.

**What changed is where an approved value is allowed to come from.** Stage 5
wrote *"Do not draft one for approval either — an approved draft is still your
sentence"*, which forbade the workflow the maintainer actually wants: the
reasoning happens **in the open session, before the skill is invoked**, the
proposal is argued with, and only an approved value is passed in.

That is not the shape Stage 6 separates. The hazard there is **one tool doing
both** — a thing that summarises work and moves rows can move a row because its
own summary said so, and the record then confirms itself. A proposal made in
front of the maintainer, with its evidence on screen, that they can reject
before any file is touched, is a decision point rather than a self-confirming
one. A proposal made *mid-write*, inside an operation that is already editing
files, is the forbidden shape — it reads as a formality, and the skill still
refuses it explicitly.

Two things keep the distinction from eroding into a rubber stamp:

- **A proposal carries where each value came from** — the plan document
  section, the commit, the measurement. A gate that can be traced is one that
  can be rejected on its evidence; a merely plausible sentence can only be
  waved through.
- **The skill records which way each value arrived**, supplied verbatim or
  approved from a proposal, and names the source in its report.

The soak-result operation stays narrowest, because it is where this is easiest
to abuse. Reading logs or dashboards and writing up what you found is still
forbidden outright — that is `plan-week`'s job and the separation is the point.
*"The soak passed"* with no run behind it is the sentence that operation exists
to refuse, whoever typed it.

**Exercised immediately, on Plan 146 itself.** Build order row 1 → closeout,
`Lands` **2026-09-14**, with a gate that names four consecutive weeks recapped
on the habit rather than in a backfill, plus one real archive performed on a
plan other than 146. Both criteria were reasoned out in session, proposed
against three alternatives, and approved before the skill ran. Closeout rather
than archive because no code is owed but the habit is unobserved: the `plans`
skill has performed one real transition and two reverted rehearsals, and
`plan-week` has recapped one real week and two near-empty backfills. Archiving
now would assert a habit nobody has watched.

The diff was one row leaving the build order, one row arriving in closeout, 17
renumbered `Order` cells and one status marker — nothing reflowed, and
`PLANS.md` unchanged at 169 lines.
