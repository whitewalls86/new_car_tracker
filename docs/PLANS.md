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
[decision log](plans_decision_log.md). For what to pick up next, read the
**Workable?** and **Blocked by** columns of the
[default build order](#default-build-order).


---

## Coordinating roadmap -- lakehouse and adaptive refresh only

[Plan 117](plan_117_storage_and_adaptive_refresh_roadmap.md) (Draft) is the
umbrella for **one arc**, not for this page. It sets the north star for moving
analytical history off loose Parquet + DuckDB and onto Iceberg with a catalog
layer, and for making adaptive-refresh experiments reproducible on top of it --
Plans 125, 112, 113, 119, 126 and 127, plus the completed work it grew out of
(110, 111, 120, 123).

It coordinates **nothing** about scraping, observability, deploys, storage
economics or the public surface, which is most of the current build order. Read
it when working the Iceberg arc; it is not the project roadmap and does not
decide the order of anything outside that arc.

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

Gates that are **verification, not work**: no code is owed on any row here. Two
are Plan 135's, ride weekly schedules, and land on the same Sunday. The third is
Plan 136 Stage 3's decay window, measured in weeks rather than days, and is why
the build order's top row has no executable step until September.

| Plan | Check | Lands | What it proves |
|---|---|---|---|
| [136](plan_136_solver_recycle_and_liveness.md) Stage 3 | `trawl` accumulates enough uptime for the solve rate to **bend** | **~2026-09-09**, 22 days from the 2026-08-18 04:28 UTC container start | That Stage 3 can choose a recycle interval from data rather than a guess. The Stage 2 soak answered the alert half and left open question 2 untouched: a 24-hour window early in a three-week cycle contains no decay to read. The 2026-08-14 outage followed **22 days** of uptime, so that is the yardstick. Rate at the close of the Stage 2 soak was 2.29/hour against ~2.4 predicted from `_CF_SESSION_TTL` -- a healthy baseline, and explicitly *not* the measurement. Nothing is owed until the number moves; if `trawl` restarts for any reason the clock restarts with it |
| [135](plan_135_storage_observability.md) criterion 5 | `cartracker_parquet_data` publishes a real series | 2026-08-23, `disk_usage` slow-tier walk at 04:00 UTC | That the per-path panel can answer *"what is filling `/mnt/data`?"* -- the question this plan was written about. The MinIO volume is the majority of that disk's 59 GiB and has still never completed a walk, so the panel is currently silent on the bulk of it. Confirmed still 0 series on 2026-08-20, exactly as designed before the first walk |
| [135](plan_135_storage_observability.md) Stage 5 | `prune_task_logs` completes its first scheduled run | 2026-08-23, `17 4 * * 0` | That Airflow's 30-day task-log retention is **enforced** rather than merely configured. The run reports run directories examined and deleted; `cartracker_airflow_logs` was 1.2M inodes and 87% of a 456s walk, so this is also what lets that volume move back to the daily tier |

None blocks implementation work. Record each result in its plan document, then
remove its row once it is green -- and record it **only** there. This section
had grown a paragraph of history for every row that left it, which is how a
three-row table came to need twenty lines of preamble describing gates that had
already closed.

## Default build order

This is the default single-maintainer sequence. Do not start a lower row merely
because it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Workable? | Blocked by | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---|---|---|---|---|
| 1 | [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle and real liveness | **Stage 2 deployed 2026-08-20 (PR #223); its soak closed 2026-08-21 with the alert half green and open question 2 unanswered.** Next slice is still an *observation window, not code* — but a longer one than planned: the 24h baseline is flat and healthy, so the recycle interval has nothing to be chosen from until the rate bends or `trawl` nears the 22-day uptime that preceded the incident | **N** | `trawl` decay window [~2026-09-09] | 98 | M | Stage 0 verified; analytics freshness moved to Plan 143; 0b moved into Plan 140 Stage 2. **Not workable until ~2026-09-09** — `trawl` had 3d 13h uptime at 2026-08-21 17:37 UTC and the outage that motivated Stage 3 followed **22 days**, so the decay this stage exists to measure cannot appear before early September. The `POST` verb on the socket proxy is separable and *is* workable now |
| 2 | [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Freeze the successful Ubuntu-update window as fixtures, then build separate maintenance intent, truthful drain status, and the checked-in host procedure | **Y** | -- | 86 | M + first observed window | Reuse Plan 136 drain semantics. **Fully unblocked 2026-08-21** — the resume gate's requirement of soaked Plan 140 health coverage is now satisfied, and Plan 144 hardened the deploy script this plan's Stage 2 consumes |
| 3 | [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Freeze production-derived fixtures and baseline, then align parsing, labels, filters, and dashboard selectors; fix `ct-403-log-spike` as the first case | **Y** | -- | 85 | S + 24h soak | Does not block Plan 136; should precede Plan 134's warning-log observation window. Has a live false-positive to work from: `ct-403-log-spike` produced **49 of the 51** alert annotations over the Plan 140 soak window, and the mechanism is an unanchored `\|= "403"` catching INFO lines from `shared.minio`. It is **not** diurnal — an earlier note said so from too small a sample |
| 4 | [146](plan_146_planning_system.md) | The planning system | Stage 0 -- reconcile the plans whose status three tables disagree on, then backfill the missing record from git before restructuring | **Y** | -- | 74 | M | **Found by the Plan 144 closeout 2026-08-21.** `PLANS.md` has four surfaces asserting plan status and no rule for which wins: Plan 135 is declared complete in prose and recorded in neither completion record, Plan 114 is completed *and* blocked, and the Plan inventory covers 30 of 72 plan files. Stage 3 is a `plans` skill, deliberately last -- automating a structure still under argument encodes the argument |
| 5 | [140](plan_140_service_health_contract.md) **Stage 4** | Retire DAG sensors as the health signal | Demote `http_health_sensor` from notifier to gate, now that a stopped container pages on its own | **Y** | -- | 70 | XS | **Unblocked 2026-08-21** — the Stage 2 soak closed green and left the closeout table, which was this row's only gate. Stages 1-3 are deployed and verified. The `flaresolverr` fire test already showed the alert going Pending inside a minute, far ahead of any DAG run, so this is a demotion decision rather than new signal work � do not remove the sensors, they remain load-bearing for DAG correctness |
| 6 | [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | **N** | Plan 141, then a 7d window | 88 | S | Plan 141 first; one-week soak before enforcement; pause if real failures need repair |
| 7 | [145](plan_145_april_cutover_reconciliation.md) | Deleting the April cutover backlog without losing data | Close Stage 0d (backdated-write safety, a blocker) and 0e, then build the one backfill write path | **Y** | -- | 84 | S | **Supersedes Plans 132 and 137.** Unblocked — Plan 133 deployed and verified. Gates 0a/0b/0c closed 2026-08-21. Goal is deletion of 1,299 legacy objects (13.79 GiB); recovery is loss minimisation, not the finish line. Stage 2 is also where `PACK_INDEX_CACHE_PACKS=48` gets its first effectiveness measurement |
| 8 | [138](plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, Plan 143 stats presentation, and project-updates snapshot | **Y** | -- | 84 | L | Plan 143 supplies the stats contract; land before the next major platform milestone |
| 9 | [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | **Y** | -- | 81 | XL | Plan 120 closeout; swap Plan 143's producer adapter while preserving its snapshot and metric contracts. **Start from `feature/plan-125-portability-audit`, not `master`** — 17 unmerged commits (last 2026-07-21) carry the replay and scale harnesses and the local reproduction of the VM OOM. See the note at the top of the plan document |
| 10 | [112](plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | **N** | Plan 125 Gate D | 76 | L | Plan 125 stable Iceberg-native inputs |
| 11 | [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | **N** | Plan 112 | 74 | M | Approved Plan 112 result; no live model dependency |
| 12 | [69](plan_69_terraform.md) | Terraform IaC | `terraform import` the existing VM/network/firewall until `plan` shows no diff against production | **Y** | -- | 66 | M | **Moved out of the backlog 2026-08-20** — its trigger is "a second environment is approved", and Plan 121 is that environment. Must land before Plan 121, not after |
| 13 | [121](plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment, provisioned from Plan 69's modules | **N** | Plan 69 | 63 | L | Plan 69 first, so staging and prod come from one module set instead of two hand-built hosts. Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 14 | [139](plan_139_test_suite_maintenance.md) **C** | Profile the 92s `tests/integration/dbt/` step | Run it with `--durations=20` in CI and record the per-test breakdown before proposing any change | **Y** | -- | 60 | S | Measurement only; CI-only work — do not pip-install dbt locally |
| 15 | [119](plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | **N** | Plan 125 | 58 | L | Stable Plan 125 catalog and reader contracts |
| 16 | [139](plan_139_test_suite_maintenance.md) **D** | Intent markers and the coverage-gate decision | Move `report_dbt_run_results.py` into `dbt_runner/`, add the `oneoff` marker per test class, decide the gate and the `airflow/dags`+`dashboard` exclusion in writing | **N** | 139 Stage A coverage data (~2026-09) | 52 | S | Several weeks of Stage A coverage data; opportunistic filler |
| 17 | [126](plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | **N** | Plans 125, 112, 113 | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 18 | [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | **N** | Plan 126 | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |

The **Workable?** and **Blocked by** columns replace the prose that used to sit
here. Sequencing arguments -- why 140 led 136 on a lower score, why 142 precedes
141, why 139 occupies two rows -- are settled by delivery and live in
[plans_decision_log.md](plans_decision_log.md).

Two forward-looking inheritances are worth keeping in front of whoever picks up
Plan 136. Stage 4's restart authority is **one added verb on the existing**
`docker-socket-proxy` **grant**, not a second socket path, because Plan 140
Stage 2 introduced that shape deliberately. And the `== bool` lesson from Plan
140's six-minute false page applies directly to 136's solver-outcome alerts:
they are the same appear-and-disappear shape, which is why both rules are
written as bool products.

## Operational watch list

Plans whose implementation is done and whose *behaviour* still wants watching.
Closeout bookkeeping is not tracked here -- Plans 114, 115 and 128 sat in this
table asking to be "recorded as completed" long after they had been, and were
removed 2026-08-21; check the [Completed](#completed) table before adding a row
of that kind.

| Plan | State | Attention required |
|---|---|---|
| [129](plan_129_zstd_dictionary_compression.md) | Dictionary v1 in production; backfill/lifecycle monitoring | Watch metrics; no new design work unless the run deviates |
| [131](plan_131_packed_cold_storage.md) | **Complete** — April-July packed and pruned, Stage 5 lifecycle DAG running on schedule | Monitor only; no new design work. One unmerged commit sits on `plan-131-packed-cold-storage` (2026-08-13) recording the stale-image run and listing-rate variance |
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
