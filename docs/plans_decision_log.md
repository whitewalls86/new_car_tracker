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
[Plan 145](plan_145_april_cutover_reconciliation.md), which superseded Plan 132
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
it -- [Plan 140](plan_140_service_health_contract.md) Stage 2 and
[Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 2 -- with the full
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

**Plan [139](plan_139_test_suite_maintenance.md) occupies two rows rather than
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
[Plan 138](plan_138_public_surface_refresh.md)'s project-updates snapshot was
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
