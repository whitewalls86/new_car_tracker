# Cartracker — Plans & Roadmap

**This file is the index and nothing else.** Every plan has its own document in
`docs/plans/`; this page says only which plans exist, what state each is in, and
in what order to work them. For system design see [ARCHITECTURE.md](ARCHITECTURE.md).

**Line budget: 250 lines.** If this file exceeds it, something has become
narrative and belongs in [plans_decision_log.md](planning/plans_decision_log.md).
[Plan 146](plans/plan_146_planning_system.md) sets the budget and
`tests/test_planning_docs.py` enforces it.

### One fact, one owner

| Fact | Where it lives |
|---|---|
| What a plan is, why, and its evidence | the plan's own `docs/plans/plan_NNN_*.md` |
| Which plans are active, in what order | [build order](#default-build-order), below |
| What is deployed and waiting on evidence | [closeout](#current-closeout), below |
| What has not started, and what would start it | [backlog](#backlog), below |
| What is finished | [completed_plans.md](planning/completed_plans.md) |
| Why the order is what it is; what happened when | [plans_decision_log.md](planning/plans_decision_log.md) |

**Every plan sits in exactly one table** — closeout, build order, backlog,
superseded, or the archive. A plan in two is a bug; so is a plan in none. **If
this index and a plan document disagree, the plan document wins.**

**Every row carries the condition that removes it.** Closeout rows name a date
and a gate, backlog rows name a trigger, build-order rows name what unblocks
them. A row leaves the moment its condition is met and its result goes to the
plan document — *only* there. No summary paragraph stays behind.

**Cross-references use plan numbers, never row positions.** Positions shift
when a row is inserted; plan numbers do not.

---

## Current State (as of 2026-08-30)

Site is live at https://cartracker.info. All major pre-lakehouse foundations are
complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

**`master` is at `610f77e`, and the migrations run to `V049`.**

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is a transition
analytics endpoint rather than the future platform target.

[Plan 117](plans/plan_117_storage_and_adaptive_refresh_roadmap.md) is the umbrella for
**one arc** — moving analytical history onto Iceberg with a catalog layer, and
making adaptive-refresh experiments reproducible on top of it (Plans 125, 112,
113, 119, 126, 127). It coordinates nothing about scraping, observability,
deploys, storage economics or the public surface, which is most of what follows.

---

## How priority and effort work

The **build order is authoritative** when scores are close: it accounts for
dependencies, safe stopping points, and the cost of switching between systems.
The **priority score** is a 0-100 planning aid, not a delivery promise:

- **90-100 — critical:** prevents current production or data-integrity loss.
- **75-89 — high:** removes a known defect, unlocks another plan, or closes a
  time-sensitive public/operational gap.
- **55-74 — medium:** strategic platform work with no immediate incident.
- **Below 55 — conditional:** worthwhile only after its trigger or dependency.

Effort is relative engineering scope including tests and deploy evidence:
**XS** (hours to 1 day), **S** (2-4 days), **M** (1-2 weeks), **L** (2-4 weeks),
**XL** (multi-phase or more than 4 weeks). A required soak or observation window
is written separately and does not inflate coding effort.

---

## Current closeout

Deployed work whose **evidence is still pending. No code is owed on any row
here** — a plan that owes code belongs in the build order, with the wait
recorded as its blocker. Record each result in the plan document, then delete
the row, and record it *only* there.

| Plan | Lands | Gate — what removes this row |
|---|---|---|
| [142](plans/plan_142_planned_host_maintenance.md) | **2026-09-30** | Another maintenance window run through the checked-in procedure. Stage 4's first reviewed window ran 2026-08-31 and completed, and it still surfaced two defects — `mounts_expected` could never pass, leaving `validating` with no exit; `restore-apt-automation` left security updates switched off. Both are fixed and deployed. One window proves the procedure executes, not that it is durable |
| [129](plans/plan_129_zstd_dictionary_compression.md) | **2026-09-01** *(review date set by Plan 146; the gate has no scheduled run)* | Dictionary v1 is live and the Stage 4 backfill is running. Row leaves when the backfill reports zero remaining level-3 objects and no metric deviation. Reversible throughout — no data is discarded and every artifact stays independently decompressable |
| [160](plans/plan_160_promtail_contract_checker_reliability.md) | **2026-09-13** | Two weeks' observation for any further indication of checker flakiness, starting from the merge of [PR #294](https://github.com/whitewalls86/new_car_tracker/pull/294) (merged 2026-08-30). Row leaves when no `promtail-config` run in the window has produced a false "Promtail dropped it", and the inconclusive verdict has either stayed absent or appeared rarely enough to read as the residual race rather than an unfixed one. Local before/after was 12 failures in 20 runs → 0 failures and 1 inconclusive; the first CI run after the fix replayed clean with no retries. A run that fails and names the same line on all three attempts is a real contract regression, not this gate |
| [146](plans/plan_146_planning_system.md) | **2026-09-14** | `docs/recaps/` holds a file for every complete week from 2026-08-17 to 2026-09-13 with no gap — four weeks recapped on the habit, not backfilled — and the `plans` skill has performed at least one archive on a plan other than 146, leaving `PLANS.md`, the archive and its stated row count in agreement. A missing week means the catch-up rule failed, and the gap shows on the filesystem |
| [149](plans/plan_149_linear_execution_layer.md) | **2026-09-15** | Stage 0 bootstrapped 2026-08-24 — workspace, team `CAR`, seven-state workflow and eight Cycle 1 issues are recorded as Stage 0 evidence. Row leaves when the Cycle measures table holds real post-cycle reads for all three cycles and the Stage 3 keep/change/remove decision is written into the plan document. Rollback is disconnecting the GitHub integration; Linear never owns repository state |
| [136](plans/plan_136_solver_recycle_and_liveness.md) | **2026-09-17** | Stage 3b's 48h soak was accepted 2026-08-27 as slowed-but-not-bounded — `restarts=0`, zero oom-kills and a 99.87% solve rate across the window, but the memory sawtooth still drifts up (10-min peak 658 → 1058 → 1318 MiB across the three days; evidence in plan_136 §"3b soak verdict — 2026-08-27"). Re-measure the trawl memory curve on 2026-09-17: if it has bounded, Stage 3 finishes here and the plan archives; if it is still climbing, a standing bi-weekly/monthly recycle (3c/3d) joins the status quo |

## Default build order

The default single-maintainer sequence. Do not start a lower row merely because
it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Workable? | Blocked by | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---|---|---|---|---|
| 1 | [162](plans/plan_162_testing_census_and_restructure.md) | Testing census and CI restructure | Stage 6 — build `container_health` a test home, then reach every route | **Y** | -- | 75 | L | **Stages 0–5b complete; Stage 5 CAR-49 and Stage 5b CAR-55, both 2026-09-01.** Stage 6 is the next slice; 68 waivers remain, 12 route and 56 Layer 2 |
| 2 | [134](plans/plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Stage 2 — enforcement, one endpoint per deploy at least 48h apart, in ascending blast radius: compact, then staging, then silver | N | Stage 1 observation window to 2026-09-06 | 88 | S | Stage 1 deployed 2026-08-30 and is a safe stopping point |
| 3 | [138](plans/plan_138_public_surface_refresh.md) | Public surface refresh | Stage 1e weekly recap projection — 1d's build-time projection pattern pointed at `docs/recaps/`, rendering 31 files to static HTML with a link classifier that handles all three `docs/` link classes | **Y** | -- | 84 | L | **Plan 143 completed 2026-08-20**, so the stats contract exists and Stage 4/PR D are unblocked. **Its project-updates snapshot now reads [completed_plans.md](planning/completed_plans.md)**, since Plan 146 removed this file's duplicate Completed table. **Stages 1e and 3d were added 2026-08-31** — publish `docs/recaps/` through the same build-time projection; per-service subdomains were scoped out to [Plan 165](plans/plan_165_service_subdomain_routing.md). Land before the next major platform milestone |
| 4 | [164](plans/plan_164_cycle_close_ritual.md) | Cycle-close ritual | Stage 0 — write the close order down, then Stage 3's git-ref and worktree hygiene; both are independent of Linear and buildable now | **Y** | -- | 70 | S | Stages 1 and 2 need a *closed* cycle to read and so cannot run before 2026-09-07; Stage 3 is independently shippable and survives Plan 149's keep/change/remove decision either way. Safe stopping point is after any stage — the package is ordered, not atomic |
| 5 | [154](plans/plan_154_container_log_coverage.md) **Stage 0** | Container log coverage | Stage 0 — classify all 28 expected-running services and measure candidate log volume before admission | **Y** | -- | 70 | S + 7d observation | Plan 141 is complete; stop after Stage 0 unless measurements justify admitting new streams |
| 6 | [151](plans/plan_151_distributed_tracing_and_runtime_topology_audit.md) | Distributed tracing and runtime topology audit | Stage 0 — define the bounded instrumentation experiment, telemetry contract, resource budgets, and Plan 142 declared-graph export | **Y** | -- | 72 | M + 7d observation | Plan 142 remains authoritative safety policy; begin metrics-only, and add Tempo only if aggregate telemetry cannot answer a recorded diagnostic question |
| 7 | [125](plans/plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | **Y** | -- | 81 | XL | Plan 120 closeout; swap Plan 143's producer adapter while preserving its snapshot and metric contracts. **Start from `feature/plan-125-portability-audit`, not `master`** — 17 unmerged commits (last 2026-07-21) carry the replay and scale harnesses and the local reproduction of the VM OOM |
| 8 | [152](plans/plan_152_scheduled_worker_lifecycle.md) | Scheduled worker lifecycle and one-shot execution | Stage 0 — classify every Compose service by lifecycle, then select the narrow launch authority and its refusal boundary | **Y** | -- | 64 | S | **Trigger fired** — Plan 142 Stage 1 is live. A one-shot that outlived its work was counted as admitted work for 14 hours on 2026-08-29 and was the first blocker on the deploy hang. Plan 159 is triggered by this landing |
| 9 | [112](plans/plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | **N** | Plan 125 Gate D | 76 | L | Plan 125 stable Iceberg-native inputs |
| 10 | [113](plans/plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | **N** | Plan 112 | 74 | M | Approved Plan 112 result; no live model dependency |
| 11 | [155](plans/plan_155_log_dashboards.md) **Stage 0** | Log dashboards and aggregate triage | Stage 0 — define the bounded dashboard questions against Plan 141's landed labels and fields | **Y** | -- | 62 | S | Plan 141 is complete. Plan 154 supplies additional streams but does not block Stage 0; keep behind Plan 162 so it does not delay the next feature milestone |
| 12 | [119](plans/plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | **N** | Plan 125 | 58 | L | Stable Plan 125 catalog and reader contracts |
| 13 | [126](plans/plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | **N** | Plans 125, 112, 113 | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 14 | [156](plans/plan_156_block_page_detection.md) | Block-page detection beyond Cloudflare | Extend the marker set in `shared/challenge.py` so the parser and the scraper's solver-outcome counter agree, keeping the `initial-activity-data` safety gate, and route an Akamai block to the `skip` path that leaves `blocked_cooldown` intact | **Y** | -- | 42 | S | Plan 147 landed 2026-08-30; the marker-set extension and its tests are a safe stopping point before the routing change |
| 15 | [127](plans/plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | **N** | Plan 126 | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |
| 16 | [69](plans/plan_69_terraform.md) | Terraform IaC | `terraform import` the existing VM/network/firewall until `plan` shows no diff against production | **Y** | -- | 66 | M | **Moved out of the backlog 2026-08-20** — its trigger was "a second environment is approved", and Plan 121 is that environment. Must land before Plan 121, not after |
| 17 | [121](plans/plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment, provisioned from Plan 69's modules | **N** | Plan 69 | 63 | L | Plan 69 first, so staging and prod come from one module set instead of two hand-built hosts. Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 18 | [150](plans/plan_150_analytics_product_and_bi_serving_layer.md) | Analytics product and BI serving layer | Stage 0 research — inventory the existing dbt gold layer, define candidate analytical products and audiences, compare serving and BI options, and recommend a bounded architecture | **Y** | -- | 68 | XL, research-gated | Stage 0 ends with a written tool and architecture decision; Plan 150 remains behind Plans 126, 127, 69 and 121, and no warehouse or BI commitment is made before that decision |
| 19 | [163](plans/plan_163_documented_code_quality_fixes.md) | Documented code quality fixes | A register, not a project — pull an item only when already in that file for another reason | **Y** | -- | 5 | XS each | **Deliberately last, and the lowest priority in the index; it must never displace scheduled work.** Inherits the live remainder of Plans 73 and 106, and records what they already delivered so it is not redone |

## Backlog

Not started. **Every row names the trigger that would move it into the build
order** — a row with no trigger is a wish, and wishes do not leave tables.

| Plan | Title | Priority | Effort | Trigger |
|---|---|---:|---|---|
| [159](plans/plan_159_unhealthy_container_escalation.md) | Unhealthy containers outside the expected-service set | 66 | S | **Plan 152 lands.** It removes the one-shot lifecycle that produced the only observed instance; what exposure remains after it is what this plan is actually for, and scoping before then would be guessing at a population that is about to change. Open question 1 — how many containers sit outside the expected-service set — is a cheap `docker inspect` read that can be taken at any time and may shrink the plan |
| [157](plans/plan_157_april_reprocessing_enrichment.md) | April reprocessing enrichment | 57 | M | **Plan 125 is complete** — this writes row-level updates into `silver_normalized/observations`, safe only once it is an Iceberg table with merge-on-read deletes and snapshot isolation. Plan 100 migrated pre-2026-04-21 April silver from a legacy schema missing seven dealer columns; Plan 145's retained reparse recovers a measured mean of 12.19 fields per pre-cutoff row across an estimated ~2M rows. Inputs frozen at `recovery/plan145/parsed/` (329 MB) — **do not prune** |
| [66](plans/plan_66_sql_injection.md) | SQL injection audit | 55 | M | Any new public mutation surface or auth-boundary change |
| [122](plans/plan_122_runtime_scraper_fetch_config.md) | Runtime scraper fetch configuration | 52 | S | Plan 136 telemetry shows timeout or solver tuning needs runtime control |
| [165](plans/plan_165_service_subdomain_routing.md) | Service subdomain routing | 48 | M | **Plan 69 lands.** Plan 121 already targets `dev.cartracker.info`, so a second hostname is arriving on that track; running this first would make Plan 69 import a DNS and routing shape that is about to change. Scoped out of Plan 138 on 2026-08-31 — subdomains touch only authenticated services, which are Plan 138's non-goals, and would double its Stage 5 route matrix. `cookie_domains = [".cartracker.info"]` is already set, so the SSO half is done; the cost is per-host DNS, per-host Caddy blocks, and retiring five subpath settings |
| [130](plans/plan_130_parser_input_projection.md) | Parser-input projection | 45 | L | Plan 129's reversible options are exhausted **and** the parser taxonomy gap is closed. Largest measured storage win and the only irreversible one |
| [166](plans/plan_166_container_pid1_reaping.md) | Container PID 1 does not reap adopted orphans | 42 | S | **Plan 136 Stage 3's verdict lands (2026-09-17).** If it adopts a standing `trawl` recycle, that caps half this leak as a side effect and Stage 1 shrinks to `dbt_runner` alone. `init: true` also changes what receives `SIGTERM` on `docker stop`, so this wants to land clear of a Plan 142 host window rather than inside one. Measured 2026-08-31: 264 zombies, 141 of them one-per-hour from `dbt_runner` since container start, unbounded in uptime and reset invisibly by every deploy |
| [79](plans/plan_79_multi_instance.md) | Multi-instance detail scraping | 40 | L | IP flagging or single-host throughput becomes the measured constraint. **This is also the plan that would create genuine multi-host need**, and therefore the honest trigger for Plan 88 |
| [94](plans/plan_94_api_docs.md) | API documentation hub | 38 | M | Public or partner API consumption makes consolidated docs useful |
| [64](plans/plan_64_pgbouncer.md) | Connection pooling — PgBouncer | 35 | M | Postgres connection exhaustion becomes the measured constraint. Plan 140's Airflow connection budget is the instrument that would show it |
| [167](plans/plan_167_solver_config_default_truth.md) | Checked-in solver defaults name the wrong container | 32 | S | **Plan 136 Stage 3's verdict lands (2026-09-17).** Stage 2 decides whether `trawl` and `redis-trawl` stay profile-gated, which changes the population Plan 136 is measuring, so it must not land inside that window |
| [108](plans/plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | 30 | M | Re-scope after Plan 136's narrower restart-authority design is proven |
| [117](plans/plan_117_storage_and_adaptive_refresh_roadmap.md) | Open lakehouse + adaptive refresh roadmap | 30 | -- | Umbrella, never built directly. Archived when its arc (125 → 112/113 → 119 → 126/127) lands |
| **88** | Kubernetes | 25 | XL | Multi-host scheduling or availability needs exceed Compose — **not merely service count**. Plan 79 is the honest trigger |
| [70](plans/plan_70_type_annotations.md) | Type annotations | 20 | M | Opportunistic; pull forward with any large refactor of the modules being annotated |

## Superseded

Terminal, like the archive, but **not finished** — these were replaced, not
delivered, and merging them into `completed_plans.md` would make that file claim
work that never happened.

| Plan | Title | Superseded by |
|---|---|---|
| [73](plans/plan_73_scraper_refactor.md) | Scraper code review and refactor | [Plan 163](plans/plan_163_documented_code_quality_fixes.md), 2026-08-30 — its own decision point asked whether the split was worth it or the current state sufficient, and the answer arrived without it: `processors/` holds six modules, `advance_search_rotation` no longer exists, and `app.py` is 317 lines. What survives is job management still in `app.py` and an empty `routers/` package |
| [106](plans/plan_106_code_review_cleanup.md) | Code review cleanup | [Plan 163](plans/plan_163_documented_code_quality_fixes.md), 2026-08-30 — A1, A2, B4 and B5 all shipped under other work, and **C1 has inverted**: it asked that `/ready` return 200 everywhere, but archiver and dbt_runner now return 503 deliberately as Plan 131's job-in-flight contract. What survives is the `dashboard/db.py` divergence and the unverified `json.loads` guards |
| [103](plans/plan_103_test_coverage.md) | Test coverage gaps | [Plan 161](plans/plan_161_testing_contract.md) and [Plan 162](plans/plan_162_testing_census_and_restructure.md), 2026-08-30 — it targeted a coverage percentage chosen 2026-04-29, which is exactly what Plan 161's contract now decides and Plan 162 measures. Its Priority 1 work shipped without it: `tests/ops/routers/test_info.py` exists with 9 tests |
| [107](plans/plan_107_quality_to_90.md) | Quality drive to 90 | [Plan 161](plans/plan_161_testing_contract.md) and [Plan 162](plans/plan_162_testing_census_and_restructure.md), 2026-08-30 — a self-scored rubric last edited 2026-04-29. Its Track A1 shipped without it: `shared/query_loader.py` is adopted by six modules. The testing third of the rubric is Plan 161's to define and Plan 162's to measure; the rest was Plan 106's |
| [132](plans/plan_132_unrecorded_artifact_recovery.md) | Recovering unrecorded bronze artifacts | [Plan 145](plans/plan_145_april_cutover_reconciliation.md), 2026-08-21 — its Stage 4 was completed by Plan 131's prune, its Stage 0c is a known failure rather than an open question, and its 36,241 orphans are one of three populations from the same April cutover |
| [137](plans/plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet disposition | [Plan 145](plans/plan_145_april_cutover_reconciliation.md), 2026-08-21 — splitting deletion into its own plan is what left it unscheduled. Its read-only census carries forward, but its April 20-27 dual-write window is **disproved**: the damage runs 2026-04-11 to 04-21 |
| [118](plans/plan_118_dbt_spark_migration.md) | dbt migration from DuckDB to Spark-compatible execution | [Plan 125](plans/plan_125_duckdb_to_iceberg_migration.md), which uses the Iceberg proof from Plan 112 and makes DuckDB-to-Iceberg the explicit objective |
| [90](plans/plan_90_dbt_cleanup.md) | dbt decommission / dbt-duckdb migration | Plan 102; the DuckDB source layer landed in Plan 96, and the forward migration is Plan 125 |
| [89](plans/plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97 and 96 |
| **87** | Kafka event-driven layer | [Plan 126](plans/plan_126_basic_event_streaming.md), which keeps the Kafka-compatible streaming idea but roots it in the staging-event/outbox pattern |
| [53](plans/plan_53_dashboard_cleanup.md) | Dashboard cleanup/optimization | Plan 101 (2026-04-29). Its "Done" section is Plan 50's file split; the rest was restructured wholesale |
| [77](plans/plan_77_sql_tests.md) | SQL query tests | Plan 84 — the plan document says so itself |
| [29](plans/plan_29_n8n_api.md) | Set up the n8n API | Plan 102 — n8n is fully removed |
| [83](plans/plan_83_n8n_workflow_viewer.md) | n8n workflow viewer | Plan 102 — there are no workflows left to view |
| **5** | n8n webhook triggers | Plan 102. Never started; recovered from index history by Plan 146 Stage 0 |
| **52** | Carousel hint backlog strategy | Plan 93 — "carousel filtered against `search_configs`" is exactly its goal. Never started; recovered by Plan 146 Stage 0 |
| **55** | Dashboard review | Plan 101 — a review of a dashboard that was then restructured wholesale. Never started; recovered by Plan 146 Stage 0 |
| **56** | Analytics next steps | Plan 117 — an open-ended placeholder whose roadmap is that arc. Never started; recovered by Plan 146 Stage 0 |

## Completed

[**completed_plans.md**](planning/completed_plans.md) — 118 rows, newest first, one row
per plan. It is the only record of what is finished; this file keeps no copy.
Dates reconstructed by Plan 146 Stage 1 are labelled *observed*, *corroborated*
or *inferred* so a guess is never mistaken for a record.

One-time reconciliation record:
[plan_state_reconciliation.md](planning/plan_state_reconciliation.md).
