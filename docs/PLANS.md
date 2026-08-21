# Cartracker - Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system
design patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-08-21)

Site is live at https://cartracker.info. All major pre-lakehouse foundations
are complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is now considered a
transition analytics endpoint rather than the future platform target.

**Why observability sits at the top of the build order.** Two incidents in four
days -- 2026-08-14 solver, 2026-08-18 Airflow apiserver -- were each found by a
human noticing downstream damage, not by an alert. Plan 136 covers the two
components that actually failed; Plan 140 covers the other twenty-four before
they do. Plan 140 is complete through Stage 3, Plan 144 hardened the deploy path
that touches all of them, and Plan 136 Stage 3 is waiting on a decay signal --
see the closeout table below.

**This section describes the system, not what happened to it.** For what shipped
when, see each plan's own document and the
[decision log](plans_decision_log.md). For what to pick up next, see the
[workability audit](#workability-audit----2026-08-21).


---

## Coordinating Roadmap

| Plan | Title | Status |
|------|-------|--------|
| [117](plan_117_storage_and_adaptive_refresh_roadmap.md) | Open lakehouse + adaptive refresh roadmap | Draft |

---

## How priority and effort work

The **build order is authoritative** when scores are close: it includes
dependencies, safe stopping points, and the cost of switching between systems.
The **priority score** is a 0-100 planning aid, not a promise of delivery date:

- **90-100 -- critical:** prevents current production or data-integrity loss.
- **75-89 -- high:** removes a known defect, unlocks another plan, or closes a
  time-sensitive public/operational gap.
- **55-74 -- medium:** strategic platform work with no immediate incident.
- **Below 55 -- conditional:** worthwhile only after its trigger or dependency.

Scores combine production impact, urgency, data-loss risk, dependency leverage,
and readiness. Effort is relative engineering scope including tests and deploy
evidence: **XS** (hours to 1 day), **S** (2-4 days), **M** (1-2 weeks), **L**
(2-4 weeks), and **XL** (multi-phase or more than 4 weeks). A required soak or
observation window is written separately and does not inflate coding effort.

## Current closeout -- finish before opening another large build

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

| Plan | Check | Lands | What it proves |
|---|---|---|---|
| [136](plan_136_solver_recycle_and_liveness.md) Stage 3 | `trawl` accumulates enough uptime for the solve rate to **bend** | **~2026-09-09**, 22 days from the 2026-08-18 04:28 UTC container start | That Stage 3 can choose a recycle interval from data rather than a guess. The Stage 2 soak answered the alert half and left open question 2 untouched: a 24-hour window early in a three-week cycle contains no decay to read. The 2026-08-14 outage followed **22 days** of uptime, so that is the yardstick. Rate at the close of the Stage 2 soak was 2.29/hour against ~2.4 predicted from `_CF_SESSION_TTL` -- a healthy baseline, and explicitly *not* the measurement. Nothing is owed until the number moves; if `trawl` restarts for any reason the clock restarts with it |
| [135](plan_135_storage_observability.md) criterion 5 | `cartracker_parquet_data` publishes a real series | 2026-08-23, `disk_usage` slow-tier walk at 04:00 UTC | That the per-path panel can answer *"what is filling `/mnt/data`?"* -- the question this plan was written about. The MinIO volume is the majority of that disk's 59 GiB and has still never completed a walk, so the panel is currently silent on the bulk of it. Confirmed still 0 series on 2026-08-20, exactly as designed before the first walk |
| [135](plan_135_storage_observability.md) Stage 5 | `prune_task_logs` completes its first scheduled run | 2026-08-23, `17 4 * * 0` | That Airflow's 30-day task-log retention is **enforced** rather than merely configured. The run reports run directories examined and deleted; `cartracker_airflow_logs` was 1.2M inodes and 87% of a 456s walk, so this is also what lets that volume move back to the daily tier |

None blocks implementation work. Record each result in its plan document, then
remove its row from this closeout table once it is green.

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

## Workability audit -- 2026-08-21

Every row below was checked against its stated blocker rather than its score.
Three blockers had gone stale, and the table's **top row has no executable step
until early September** -- which matters because the build order's own rule is
"do not start a lower row merely because it is smaller while a higher row has
an executable next step." That rule is vacuous while row 1 is un-startable, so
it is written down here instead of rediscovered each time.

### Workable today

| Row | Plan | Slice | Note |
|---:|---|---|---|
| 2 | 142 | Stage 0 -- freeze the Ubuntu-update window as fixtures | **The recommendation.** Documentation and inventory; its Plan 140 health-coverage gate closed 2026-08-21, and Plan 144 hardened the script Stage 2 consumes |
| 3 | 141 | Freeze log fixtures, fix `ct-403-log-spike` | The soak sharpened the target: 49 of 51 annotations, and the mechanism is an unanchored `\|= "403"` catching INFO lines from `shared.minio`. A parsing defect with a live reproduction |
| 4 | 140 **Stage 4** | Demote `http_health_sensor` to a gate | **XS, and its blocker is gone.** Was gated on the Stage 2 soak, which closed green |
| 6 | 145 | Stage 0d/0e, then the one backfill write path | 0a/0b/0c closed 2026-08-21. 0d is a genuine blocker *inside* the plan, not on it |
| 7 | 138 | Public surface refresh | Plan 143 completed 2026-08-20, so its stats contract is available |
| 11 | 69 | `terraform import` until `plan` shows no diff | Must land before row 12 |
| 13 | 139 **C** | Profile the dbt step with `--durations=20` | Measurement only, CI-only |
| -- | 136 | The `POST` verb on the socket proxy | Separable from the blocked observation window; see below |

### Blocked, with the actual blocker

| Row | Plan | Blocked on | Clears |
|---:|---|---|---|
| 1 | 136 **Stage 3** | A decay signal. `trawl` had 3d 13h uptime on 2026-08-21; the 2026-08-14 outage followed **22 days**, so a day-long window contains no decay to read | **~2026-09-09** |
| 5 | 134 | Plan 141 (row 3) landing first, then a one-week observation window | Row 3 + 7d |
| 8 | 125 | Gate C's two VM-scale failures still need local reproduction | Work, not waiting |
| 9, 10 | 112, 113 | Plan 125 Iceberg-native inputs, then an approved 112 result | Chained behind row 8 |
| 12 | 121 | Plan 69 (row 11) | Row 11 |
| 14 | 119 | Stable Plan 125 catalog/reader contracts | Row 8 |
| 15 | 139 **D** | "Several weeks" of Stage A coverage data; Stages A+B shipped 2026-08-18 | ~2026-09 |
| 16, 17 | 126, 127 | Plans 125 and 112/113 | Chained behind row 8 |
| -- | 130 | Plan 129's reversible options being exhausted | No trigger yet |

**Row 8 is the only blocked row whose blocker is work rather than time**, which
makes Plan 125 the row to pick if the calendar-bound ones are unappealing.

### Not work -- stale rows, now removed

Three watch-list rows asked for closeout that had **already happened**: 114, 115
and 128 were sitting in the watch list saying "record and move to completed"
while all three were already in the Completed table (2026-08-10, 2026-07-01,
2026-07-20). Removed 2026-08-21. Plan 115 is the cautionary one -- its own
document never got a status line and its only "deployed" string is inside a
list of *proposed* alerts, so the plan reads unfinished. The evidence is in the
schema: `V040__detail_scrape_circuit_breaker.sql`, applied 2026-07-01,
`success = true`. **Check the migration, not the prose.**

### Unmerged work not represented anywhere on this page

| Branch | Commits | Last commit |
|---|---:|---|
| `feature/plan-125-portability-audit` | 17 | 2026-07-21 |
| `plan-131-packed-cold-storage` | 1 | 2026-08-13 |

The Plan 125 branch is a month old and is Gate C work -- the same Gate C that
row 8 names as its next slice, so **whoever picks up row 8 should start there
rather than from master**. The Plan 131 commit records the stale-image run and
listing-rate variance against a plan the watch list already calls Complete.

## Default build order

This is the default single-maintainer sequence. Do not start a lower row merely
because it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---:|---|---|
| 1 | [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle and real liveness | **Stage 2 deployed 2026-08-20 (PR #223); its soak closed 2026-08-21 with the alert half green and open question 2 unanswered.** Next slice is still an *observation window, not code* — but a longer one than planned: the 24h baseline is flat and healthy, so the recycle interval has nothing to be chosen from until the rate bends or `trawl` nears the 22-day uptime that preceded the incident | 98 | M | Stage 0 verified; analytics freshness moved to Plan 143; 0b moved into Plan 140 Stage 2. **Not workable until ~2026-09-09** — `trawl` had 3d 13h uptime at 2026-08-21 17:37 UTC and the outage that motivated Stage 3 followed **22 days**, so the decay this stage exists to measure cannot appear before early September. The `POST` verb on the socket proxy is separable and *is* workable now |
| 2 | [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Freeze the successful Ubuntu-update window as fixtures, then build separate maintenance intent, truthful drain status, and the checked-in host procedure | 86 | M + first observed window | Reuse Plan 136 drain semantics. **Fully unblocked 2026-08-21** — the resume gate's requirement of soaked Plan 140 health coverage is now satisfied, and Plan 144 hardened the deploy script this plan's Stage 2 consumes |
| 3 | [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Freeze production-derived fixtures and baseline, then align parsing, labels, filters, and dashboard selectors; fix `ct-403-log-spike` as the first case | 85 | S + 24h soak | Does not block Plan 136; should precede Plan 134's warning-log observation window. Has a live false-positive to work from: see below |
| 4 | [140](plan_140_service_health_contract.md) **Stage 4** | Retire DAG sensors as the health signal | Demote `http_health_sensor` from notifier to gate, now that a stopped container pages on its own | 70 | XS | **Unblocked 2026-08-21** — the Stage 2 soak closed green and left the closeout table, which was this row's only gate. Stages 1-3 are deployed and verified. The `flaresolverr` fire test already showed the alert going Pending inside a minute, far ahead of any DAG run, so this is a demotion decision rather than new signal work � do not remove the sensors, they remain load-bearing for DAG correctness |
| 5 | [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | 88 | S | Plan 141 first; one-week soak before enforcement; pause if real failures need repair |
| 6 | [145](plan_145_april_cutover_reconciliation.md) | Deleting the April cutover backlog without losing data | Close Stage 0d (backdated-write safety, a blocker) and 0e, then build the one backfill write path | 84 | S | **Supersedes Plans 132 and 137.** Unblocked — Plan 133 deployed and verified. Gates 0a/0b/0c closed 2026-08-21. Goal is deletion of 1,299 legacy objects (13.79 GiB); recovery is loss minimisation, not the finish line. Stage 2 is also where `PACK_INDEX_CACHE_PACKS=48` gets its first effectiveness measurement |
| 7 | [138](plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, Plan 143 stats presentation, and project-updates snapshot | 84 | L | Plan 143 supplies the stats contract; land before the next major platform milestone |
| 8 | [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | 81 | XL | Plan 120 closeout; swap Plan 143's producer adapter while preserving its snapshot and metric contracts. **Start from `feature/plan-125-portability-audit`, not `master`** — 17 unmerged commits (last 2026-07-21) carry the replay and scale harnesses and the local reproduction of the VM OOM. See the note at the top of the plan document |
| 9 | [112](plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | 76 | L | Plan 125 stable Iceberg-native inputs |
| 10 | [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | 74 | M | Approved Plan 112 result; no live model dependency |
| 11 | [69](plan_69_terraform.md) | Terraform IaC | `terraform import` the existing VM/network/firewall until `plan` shows no diff against production | 66 | M | **Moved out of the backlog 2026-08-20** — its trigger is "a second environment is approved", and Plan 121 is that environment. Must land before 121, not after |
| 12 | [121](plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment, provisioned from Plan 69's modules | 63 | L | Plan 69 first, so staging and prod come from one module set instead of two hand-built hosts. Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 13 | [139](plan_139_test_suite_maintenance.md) **C** | Profile the 92s `tests/integration/dbt/` step | Run it with `--durations=20` in CI and record the per-test breakdown before proposing any change | 60 | S | Measurement only; CI-only work — do not pip-install dbt locally |
| 14 | [119](plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | 58 | L | Stable Plan 125 catalog and reader contracts |
| 15 | [139](plan_139_test_suite_maintenance.md) **D** | Intent markers and the coverage-gate decision | Move `report_dbt_run_results.py` into `dbt_runner/`, add the `oneoff` marker per test class, decide the gate and the `airflow/dags`+`dashboard` exclusion in writing | 52 | S | Several weeks of Stage A coverage data; opportunistic filler |
| 16 | [126](plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 17 | [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |

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

## Operational watch list

Plans whose implementation is done and whose *behaviour* still wants watching.
Closeout bookkeeping is not tracked here -- Plans 114, 115 and 128 sat in this
table asking to be "recorded as completed" long after they had been, and were
removed 2026-08-21; check the [Completed](#completed) table before adding a row
of that kind.

| Plan | State | Attention required |
|---|---|---|
| [129](plan_129_zstd_dictionary_compression.md) | Dictionary v1 in production; backfill/lifecycle monitoring | Watch metrics; no new design work unless the run deviates |
| [131](plan_131_packed_cold_storage.md) | **Complete** — April-July packed and pruned, Stage 5 lifecycle DAG running on schedule | Monitor only; no new design work |
| [135](plan_135_storage_observability.md) | **Complete 2026-08-18** — both disks visible, alerts proven, all log stores bounded, maintenance runbook live | Monitor scheduled storage and task-log maintenance; parsing/dashboard follow-up is Plan 141 |

## Paused or blocked

| Plan | Priority | Effort | Resume when |
|---|---:|---|---|
| [130](plan_130_parser_input_projection.md) | 45 | L | Plan 129 reversible options are exhausted and the parser taxonomy gap is closed |
| [114](plan_114_sectioned_html_artifact_audit.md) follow-on | 30 | L | New evidence overturns the measured negative storage result |

## Plan inventory

| Plan | Title | Status |
|------|-------|--------|
| [112](plan_112_refresh_policy_backtesting.md) | Iceberg + MLflow adaptive refresh backtesting | Paused after foundation proof |
| [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh integration | Draft |
| [114](plan_114_sectioned_html_artifact_audit.md) | Sectioned HTML artifact audit | Complete - sectioned storage rejected on measured results |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Detail unenriched circuit breaker | Implemented - closeout record pending |
| [119](plan_119_lakehouse_governance.md) | Lakehouse governance + catalog expansion | Draft |
| [120](plan_120_ci_lake_snapshot_delivery.md) | CI + local lake snapshot delivery | **Complete 2026-08-18** — Gate F production download and checksum round trip verified |
| [121](plan_121_staging_environment.md) | Staging environment | Draft |
| [124](plan_124_trawl_memory_guardrails.md) | Trawl browser solver memory guardrails | **Complete** - verified in production 2026-08-18; zero host-wide OOM since deployment |
| [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB to Iceberg analytics migration | Gates 0.5, 0, A, and B complete; Gate C measurement next |
| [126](plan_126_basic_event_streaming.md) | Basic event streaming foundation | Draft / future |
| [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Draft / future |
| [128](plan_128_false_block_detection.md) | Cloudflare challenge pages swallowed as successful detail scrapes | Implemented through Phase 4 - closeout record pending |
| [129](plan_129_zstd_dictionary_compression.md) | Trained zstd dictionary compression for bronze HTML | In production — dict v1 live, backfill running |
| [130](plan_130_parser_input_projection.md) | Parser-input projection (truncating raw HTML) | Draft — blocked on 129 + taxonomy gap |
| [131](plan_131_packed_cold_storage.md) | Packed cold storage for bronze HTML | **Complete 2026-08-18** — April-July packed and pruned: 3.61M objects → 288, 0 refused; Stage 5 lifecycle DAG green on its first scheduled run |
| [132](plan_132_unrecorded_artifact_recovery.md) | Recovering unrecorded bronze artifacts | **Superseded 2026-08-21 by [Plan 145](plan_145_april_cutover_reconciliation.md)** — its Stage 4 was completed by Plan 131's prune, its Stage 0c is a known failure rather than an open question, and its 36,241 orphans are one of three populations from the same April cutover |
| [133](plan_133_pack_read_path_hardening.md) | Pack read path hardening | **Complete 2026-08-20** — PR #219 (`5066bc1`) deployed to `ops`, `archiver`, `pack-worker`, and `processing`; post-deploy verification read 720 artifacts through the pack path across April-July with 0 failures. Unblocks [Plan 145](plan_145_april_cutover_reconciliation.md) Stage 2 |
| [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Draft — measurement-first rollout not started |
| [135](plan_135_storage_observability.md) | Storage observability | **Complete 2026-08-18** — both disks visible, alerts proven, all log stores bounded, runbook live, `df /` 79% → 51%; criterion 5's MinIO half publishes on the first Sunday slow-tier walk (2026-08-23) |
| [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle + real liveness detection | **Stages 0 and 2 deployed and verified in production (2026-08-18, 2026-08-20); Stage 1 transferred to Plan 143 before deployment** — prototype commit `584f100` established the fail-loud contract but not the accepted serving design. 0b shipped as Plan 140 Stage 2 on 2026-08-20, which also leaves Stage 4 a `docker-socket-proxy` to extend rather than a socket path to open. Stage 2 (PR #223, merge `50bba68`) adds two scraper-owned outcome counters, the `scraper` Prometheus job that never existed, `ct-solver-not-solving` + `ct-detail-fetch-failing` in `bool`-product form, and fixes D1's remaining partial-hour defect in the Plan 143 snapshot SQL. **Soak closed 2026-08-21 split: both rules held across 1,189 evaluations with zero state transitions and the `> bool 20` volume guard proved itself on 14 genuinely-zero overnight checkpoints, but open question 2 is unanswered — a healthy window 3.5 days into a 22-day failure horizon has no decay to read, so Stage 3 still cannot choose a recycle interval.** D1 closed green at the 21:00 build on 2026-08-20. Stages 3-4 not started |
| [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet disposition | **Superseded 2026-08-21 by [Plan 145](plan_145_april_cutover_reconciliation.md)** — splitting deletion into its own plan is what left it unscheduled; deletion is now Plan 145's stated goal with approval as a gate. Its read-only census carries forward as an input, but its April 20-27 dual-write window is **disproved** — the damage runs 2026-04-11 to 04-21 — so its whole-file safety classes must be re-derived before any deletion manifest is trusted |
| [138](plan_138_public_surface_refresh.md) | README and public portfolio surface refresh | Draft — audit complete; analytics acquisition/database removal transferred to Plan 143, while public presentation remains here |
| [139](plan_139_test_suite_maintenance.md) | Test suite construction and maintenance | **Stages A+B complete 2026-08-18** (PR #213) — coverage reported at 88%, CI path 333s → ~260s; Stages C/D remain queued as opportunistic filler; analytics producer coverage transferred through Plan 136 to Plan 143 |
| [140](plan_140_service_health_contract.md) | Service health contract | **Stages 1, 2 and 3 deployed and verified in production 2026-08-20** (PRs #216, #221, #222) — `cartracker_container_health` publishes three states for all 28 running services from a dedicated scrape-time exporter behind `docker-socket-proxy`, `ct-service-down` covers all 9 scrape jobs under an exact-set-equality test, and a Service Health row opens the Infrastructure dashboard. The deploy found two defects worth more than itself: a single-file bind mount pins an inode, so `git pull` + SIGHUP reloaded the *old* Prometheus config while logging success (routed to Plan 144); and the first alert expression false-paged in six minutes on a recovered container, fixed with `== bool`. **Soak closed green 2026-08-21**: zero state transitions for `ct-container-unhealthy` in a window where Grafana logged 51, 28 of 28 instances `Normal`, exporter never missed a scrape, and the three `0` readings each lasted one 15s sample against a `for: 5m`. The documented removed-container gap reproduced live and harmlessly — one evaluation in 5,154 saw 27 series while `dbt_runner` was recreated. Only Stage 4 remains |
| [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Draft — routed from Plan 135 closeout; parsing, labels, privacy policy, dashboards, and capacity soak not started |
| [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Draft — separate maintenance intent, truthful drain, checked-in apt/reboot procedure, and Plan 140-gated resume not started |
| [143](plan_143_analytics_serving_snapshot.md) | Analytics serving snapshot and reader consolidation | **Complete 2026-08-20** — PR #217 (`e5d3a46`) deployed the serving boundary; PR #218 (`a3cdd59`) corrected the Grafana ownership/cadence defects the first soak exposed. Corrected soak clean: 24 hourly publications, zero failed publishes, 3,580.9s worst-case freshness against a 4,500s threshold, zero lock conflicts, `/info` at 0.157s |
| [145](plan_145_april_cutover_reconciliation.md) | Deleting the April cutover backlog without losing data | **Draft 2026-08-21 — gates 0a/0b/0c closed, 0d/0e open.** Supersedes Plans 132 and 137. Goal is deleting 1,299 legacy Parquet objects (13.79 GiB); recovery is loss minimisation. Verified against production: April holds 0 surviving `.html.zst` objects, the 42,276-orphan predicate reproduces exactly from 32 sidecars, and orphans read back byte-identically through the pack path. ~224,000 legacy captures have no observation — real, not a join artifact (73.4% match at 5s, flat to 600s) — but **no listing was lost**: 100.0% appear in silver, only 138 never do. Incident window is 2026-04-11 to 04-21, peaking on the 21st, which **disproves Plan 137's April 20-27 boundary**. **Gate 0f collapsed the plan from L to S**: only 270 of 355,845 unmatched captures sit inside an observed price change, so bulk recovery is dropped and Stage 3 recovers ~11,600 rows rather than ~224,000. Also established that `artifact_id` does not survive the cutover and must never be used as a cross-system join key, and surfaced a separate `detail/active` null-price parser gap (96,909 April rows) routed out of scope |
| [144](plan_144_deploy_script_hardening.md) | Deploy script hardening | **Complete 2026-08-21** — PRs #224 and #225, deployed and verified at `dd9e207`. `--restart prometheus` saw a real `starting` → `healthy` transition in 6s and verified inode 519823 on both sides of the mount that produced defect 4 twice. `redeploy.sh pgadmin` confirmed `--no-deps` and fired the no-op detector, which is how we learned `pgadmin` could never have verified the gate: nothing about it had changed, so Compose recreated nothing. Grew from three defects to six without growing past XS. `--no-deps`, a health gate whose timeout is derived from the slowest healthcheck in `docker-compose.yml` and checked in CI, an intent-release rule that splits build failure (release) from partial recreation (hold), a `--restart` path that restarts single-file bind mounts and verifies the loaded inode, and a warning for peers that cached a recreated service's address. The deny-list moved to `healthcheck-exemptions.txt` so the poller and `TestServiceHealthCoverage` read one list. Grew from three defects to five: Plan 140 Stage 2 added the inode trap, Plan 136 D6 added the cached-address trap |
| [69](plan_69_terraform.md) | Terraform IaC | Draft — **moved from backlog to build-order row 13 on 2026-08-20** as a stated prerequisite of Plan 121. First slice is `terraform import` until `plan` shows no diff against production |

---

## Backlog

| Priority | Effort | Plan | Title | Resume trigger / blocker |
|---:|---|---|---|---|
| 55 | M | [66](plan_66_sql_injection.md) | SQL injection audit | Pull forward after any new public mutation surface or auth-boundary change |
| 52 | S | [122](plan_122_runtime_scraper_fetch_config.md) | Runtime scraper fetch configuration | Plan 136 telemetry shows timeout or solver tuning needs runtime control |
| 40 | L | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | IP flagging or single-host throughput becomes the measured constraint. **This is also the plan that would create genuine multi-host need**, and therefore the honest trigger for Plan 88 |
| 38 | M | [94](plan_94_api_docs.md) | API documentation hub | Public or partner API consumption makes consolidated docs useful |
| 30 | M | [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Re-scope after Plan 136's narrower restart-authority design is proven |
| 25 | XL | **88** | Kubernetes | Multi-host scheduling/availability needs exceed Compose, not merely service count. See the control-plane note below before re-scoring this |

---

## Sequencing Rationale

Moved to [plans_decision_log.md](plans_decision_log.md) on 2026-08-21. It was
134 lines -- the largest section in this file -- and it is consulted when the
*order* is questioned, not when someone asks what to work on next. The build
order's `Depends on / safe stopping point` column carries the per-row reason.

## Superseded

| Plan | Title | Reason |
|------|-------|--------|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |
| [90](plan_90_dbt_cleanup.md) | dbt decommission / dbt-duckdb migration | Superseded by Plan 102; DuckDB source layer done in Plan 96; the new forward migration is Plan 125 |
| [118](plan_118_dbt_spark_migration.md) | dbt migration from DuckDB to Spark-compatible execution | Superseded/refined by Plan 125, which uses the Iceberg proof from Plan 112 and makes DuckDB-to-Iceberg migration the explicit objective |
| **87** | Kafka event-driven layer | Superseded/refined by Plan 126, which keeps the Kafka-compatible streaming idea but roots it in the staging-event/outbox pattern |

---

## Completed

See [completed_plans.md](completed_plans.md) for the full archive. Keep this
small table newest-first: Plan 138's public project-updates snapshot consumes it
alongside the default build-order table.

| Plan | Completed | Public summary |
|---|---|---|
| [143](plan_143_analytics_serving_snapshot.md) | 2026-08-20 | Moved analytics metrics and the public statistics page onto a versioned snapshot published by the service that builds the data, removing recurring database reads from the public request path. |
| [133](plan_133_pack_read_path_hardening.md) | 2026-08-20 | Closed two gaps in how archived HTML is read back after compaction, so recovery jobs retry stranded work instead of abandoning it. |
| [114](plan_114_sectioned_html_artifact_audit.md) | 2026-08-10 | Measured sectioned HTML storage on production data, rejected it on storage economics, and identified dictionary compression and packing as the better reversible path. |
| [128](plan_128_false_block_detection.md) | 2026-07-20 | Fixed challenge-page misclassification, preserved cooldown state, reconciled cleanup behavior, and closed the historical-repair decision. |
| [111](plan_111_adaptive_detail_refresh.md) | 2026-07-06 | Built listing-state fingerprints, state runs, volatility features, and initial adaptive-refresh priority outputs. |
| [110](implementation_plan_110_storage_layout_hygiene.md) | 2026-07-06 | Normalized the storage foundation for the lakehouse and adaptive-refresh work. |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | 2026-07-01 | Stopped successfully scraped but unenriched listings from entering a pathological 15-minute retry loop. |
| [95](plan_95_portfolio_landing_page.md) | 2026-05-04 | Shipped the first public portfolio landing page with pipeline context and live statistics. |
