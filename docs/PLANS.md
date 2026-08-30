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

## Current State (as of 2026-08-25)

Site is live at https://cartracker.info. All major pre-lakehouse foundations are
complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

**Production is at `bb06054`, deployed the evening of 2026-08-25.** PRs #243,
#245, #246, #247, #248 and #249 are all live, and `V043` is applied.

Two soaks are running, both clocked from the deploy rather than from any merge:

- **Plan 136 Stage 3b** — 48 hours from 19:50:55, accepts **2026-08-27**.
- **Plan 141 Stage 4** — 24 hours from 19:52, accepts **2026-08-26**.

Plan 142's Phase B window ran the same evening and closed clean: Stage 0 items
6 and 7 are done, and the maintenance-pool hold ran 20:14:57 → 21:14:57,
draining 44 tasks in 74.5s with zero failures.

Plan 134's one-week observation window unblocks when Plan 141 Stage 4 accepts,
but it owes code first and stays in the build order until that lands.

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
| [129](plans/plan_129_zstd_dictionary_compression.md) | **2026-09-01** *(review date set by Plan 146; the gate has no scheduled run)* | Dictionary v1 is live and the Stage 4 backfill is running. Row leaves when the backfill reports zero remaining level-3 objects and no metric deviation. Reversible throughout — no data is discarded and every artifact stays independently decompressable |
| [146](plans/plan_146_planning_system.md) | **2026-09-14** | `docs/recaps/` holds a file for every complete week from 2026-08-17 to 2026-09-13 with no gap — four weeks recapped on the habit, not backfilled — and the `plans` skill has performed at least one archive on a plan other than 146, leaving `PLANS.md`, the archive and its stated row count in agreement. A missing week means the catch-up rule failed, and the gap shows on the filesystem |
| [149](plans/plan_149_linear_execution_layer.md) | **2026-09-15** | Stage 0 bootstrapped 2026-08-24 — workspace, team `CAR`, seven-state workflow and eight Cycle 1 issues are recorded as Stage 0 evidence. Row leaves when the Cycle measures table holds real post-cycle reads for all three cycles and the Stage 3 keep/change/remove decision is written into the plan document. Rollback is disconnecting the GitHub integration; Linear never owns repository state |
| [136](plans/plan_136_solver_recycle_and_liveness.md) | **2026-09-17** | Stage 3b's 48h soak was accepted 2026-08-27 as slowed-but-not-bounded — `restarts=0`, zero oom-kills and a 99.87% solve rate across the window, but the memory sawtooth still drifts up (10-min peak 658 → 1058 → 1318 MiB across the three days; evidence in plan_136 §"3b soak verdict — 2026-08-27"). Re-measure the trawl memory curve on 2026-09-17: if it has bounded, Stage 3 finishes here and the plan archives; if it is still climbing, a standing bi-weekly/monthly recycle (3c/3d) joins the status quo |

## Default build order

The default single-maintainer sequence. Do not start a lower row merely because
it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Workable? | Blocked by | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---|---|---|---|---|
| 1 | [142](plans/plan_142_planned_host_maintenance.md) **Stage 4** | Scoped operational coordination and host maintenance | Fix the four Stage 2/3 defects the 2026-08-29 scoping found — deferred-reboot kernel target, one-off manifest capture, the `oauth2-proxy` release-gate exemption, the client's API default — then deploy `ops` and `container-health` and run the window from the Stage 4 run sheet | **Y** | -- | 86 | M + first observed window | Stage 3 is built but **not deployed**: both images predate it while V044-V047 are applied. Scoping the window is what found this; the plan document holds all five blockers. Plan 145 Stage 6 must finish before any coordination request |
| 2 | [134](plans/plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | **Y** | -- | 88 | S | Plan 141 completed 2026-08-26; the one-week observation begins when Plan 134's warning-only predicates deploy, then enforcement follows if the evidence is clean |
| 3 | [138](plans/plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, Plan 143 stats presentation, and project-updates snapshot | **Y** | -- | 84 | L | Plan 143 supplies the stats contract; land before the next major platform milestone. **Its project-updates snapshot now reads [completed_plans.md](planning/completed_plans.md)**, since Plan 146 removed this file's duplicate Completed table |
| 4 | [147](plans/plan_147_scrape_state_ownership.md) | Scrape state ownership — separating fetch from enrichment | `V048` expand migration plus the fetch-backoff view rebuild — a no-op for every existing row | **Y** | -- | 75 | S | Nothing. Found by Plan 142 Stage 0, which hit the loop while scoping a maintenance pause. **Supersedes Plan 115's mechanism while preserving its goal** — the guard moves next to the fetch so new causes cannot reopen the loop. Expand/contract, so Stage 1 reverts by reverting the commit; only Stage 4's `V049` drop is irreversible. Landing it lets Plan 142 drop `scrape_detail_pages` from its held set |
| 5 | [154](plans/plan_154_container_log_coverage.md) **Stage 0** | Container log coverage | Stage 0 — classify all 26 expected-running services and measure candidate log volume before admission | **Y** | -- | 70 | S + 7d observation | Plan 141 is complete; stop after Stage 0 unless measurements justify admitting new streams |
| 6 | [151](plans/plan_151_distributed_tracing_and_runtime_topology_audit.md) | Distributed tracing and runtime topology audit | Stage 0 — define the bounded instrumentation experiment, telemetry contract, resource budgets, and Plan 142 declared-graph export | **Y** | -- | 72 | M + 7d observation | Plan 142 remains authoritative safety policy; begin metrics-only, and add Tempo only if aggregate telemetry cannot answer a recorded diagnostic question |
| 7 | [125](plans/plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | **Y** | -- | 81 | XL | Plan 120 closeout; swap Plan 143's producer adapter while preserving its snapshot and metric contracts. **Start from `feature/plan-125-portability-audit`, not `master`** — 17 unmerged commits (last 2026-07-21) carry the replay and scale harnesses and the local reproduction of the VM OOM |
| 8 | [112](plans/plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | **N** | Plan 125 Gate D | 76 | L | Plan 125 stable Iceberg-native inputs |
| 9 | [113](plans/plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | **N** | Plan 112 | 74 | M | Approved Plan 112 result; no live model dependency |
| 10 | [139](plans/plan_139_test_suite_maintenance.md) **Stage C** | Profile the 92s `tests/integration/dbt/` step | Run it with `--durations=20` in CI and record the per-test breakdown before proposing any change | **Y** | -- | 60 | S | Measurement only; CI-only work — do not pip-install dbt locally |
| 11 | [155](plans/plan_155_log_dashboards.md) **Stage 0** | Log dashboards and aggregate triage | Stage 0 — define the bounded dashboard questions against Plan 141's landed labels and fields | **Y** | -- | 62 | S | Plan 141 is complete. Plan 154 supplies additional streams but does not block Stage 0; keep behind Plan 139 Stage C so it does not delay the next feature milestone |
| 12 | [119](plans/plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | **N** | Plan 125 | 58 | L | Stable Plan 125 catalog and reader contracts |
| 13 | [139](plans/plan_139_test_suite_maintenance.md) **Stage D** | Intent markers and the coverage-gate decision | Move `report_dbt_run_results.py` into `dbt_runner/`, add the `oneoff` marker per test class, decide the gate and the `airflow/dags`+`dashboard` exclusion in writing | **N** | 139 Stage A coverage data [~2026-09] | 52 | S | Several weeks of Stage A coverage data; opportunistic filler. Stage E then evaluates Plan 142-informed CI impact selection in advisory mode before any new skip policy. **Also the trigger for backlog Plans 103 and 107**, which overlap it and are re-scoped or superseded when it lands |
| 14 | [126](plans/plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | **N** | Plans 125, 112, 113 | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 15 | [156](plans/plan_156_block_page_detection.md) | Block-page detection beyond Cloudflare | Extend the marker set in `shared/challenge.py` so the parser and the scraper's solver-outcome counter agree, keeping the `initial-activity-data` safety gate, and route an Akamai block to the `skip` path that leaves `blocked_cooldown` intact | **N** | Plan 147 | 42 | S | Plan 147 lands first; the marker-set extension and its tests are a safe stopping point before the routing change |
| 16 | [127](plans/plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | **N** | Plan 126 | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |
| 17 | [69](plans/plan_69_terraform.md) | Terraform IaC | `terraform import` the existing VM/network/firewall until `plan` shows no diff against production | **Y** | -- | 66 | M | **Moved out of the backlog 2026-08-20** — its trigger was "a second environment is approved", and Plan 121 is that environment. Must land before Plan 121, not after |
| 18 | [121](plans/plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment, provisioned from Plan 69's modules | **N** | Plan 69 | 63 | L | Plan 69 first, so staging and prod come from one module set instead of two hand-built hosts. Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 19 | [150](plans/plan_150_analytics_product_and_bi_serving_layer.md) | Analytics product and BI serving layer | Stage 0 research — inventory the existing dbt gold layer, define candidate analytical products and audiences, compare serving and BI options, and recommend a bounded architecture | **Y** | -- | 68 | XL, research-gated | Stage 0 ends with a written tool and architecture decision; Plan 150 remains behind Plans 126, 127, 69 and 121, and no warehouse or BI commitment is made before that decision |

## Backlog

Not started. **Every row names the trigger that would move it into the build
order** — a row with no trigger is a wish, and wishes do not leave tables.

| Plan | Title | Priority | Effort | Trigger |
|---|---|---:|---|---|
| [152](plans/plan_152_scheduled_worker_lifecycle.md) | Scheduled worker lifecycle and one-shot execution | 64 | S | Plan 142 Stage 1 lands, then schedule a production-behavior window to separate monthly packing and daily disk measurement from continuously running services |
| [157](plans/plan_157_april_reprocessing_enrichment.md) | April reprocessing enrichment | 57 | M | **Plan 125 is complete** — this writes row-level updates into `silver_normalized/observations`, safe only once it is an Iceberg table with merge-on-read deletes and snapshot isolation. Plan 100 migrated pre-2026-04-21 April silver from a legacy schema missing seven dealer columns; Plan 145's retained reparse recovers a measured mean of 12.19 fields per pre-cutoff row across an estimated ~2M rows. Inputs frozen at `recovery/plan145/parsed/` (329 MB) — **do not prune** |
| [66](plans/plan_66_sql_injection.md) | SQL injection audit | 55 | M | Any new public mutation surface or auth-boundary change |
| [122](plans/plan_122_runtime_scraper_fetch_config.md) | Runtime scraper fetch configuration | 52 | S | Plan 136 telemetry shows timeout or solver tuning needs runtime control |
| [130](plans/plan_130_parser_input_projection.md) | Parser-input projection | 45 | L | Plan 129's reversible options are exhausted **and** the parser taxonomy gap is closed. Largest measured storage win and the only irreversible one |
| [79](plans/plan_79_multi_instance.md) | Multi-instance detail scraping | 40 | L | IP flagging or single-host throughput becomes the measured constraint. **This is also the plan that would create genuine multi-host need**, and therefore the honest trigger for Plan 88 |
| [94](plans/plan_94_api_docs.md) | API documentation hub | 38 | M | Public or partner API consumption makes consolidated docs useful |
| [64](plans/plan_64_pgbouncer.md) | Connection pooling — PgBouncer | 35 | M | Postgres connection exhaustion becomes the measured constraint. Plan 140's Airflow connection budget is the instrument that would show it |
| [103](plans/plan_103_test_coverage.md) | Test coverage | 32 | M | Plan 139 Stage D settles the coverage gate; re-scope or supersede then. **Not marked superseded** — it overlaps Plan 139, and overlapping is not the same as superseded |
| [107](plans/plan_107_quality_to_90.md) | Quality to 90% | 32 | M | As Plan 103. Both were recorded nowhere until Plan 146 Stage 0 |
| [108](plans/plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | 30 | M | Re-scope after Plan 136's narrower restart-authority design is proven |
| [117](plans/plan_117_storage_and_adaptive_refresh_roadmap.md) | Open lakehouse + adaptive refresh roadmap | 30 | -- | Umbrella, never built directly. Archived when its arc (125 → 112/113 → 119 → 126/127) lands |
| [73](plans/plan_73_scraper_refactor.md) | Scraper code review and refactor | 28 | M | Deferred to "after Plan 72", which is archived — re-scope against the current scraper before starting |
| [106](plans/plan_106_code_review_cleanup.md) | Code review cleanup | 25 | M | Sourced from `CODE_REVIEW_2026-03-20.md`; re-audit before starting, the codebase has moved |
| **88** | Kubernetes | 25 | XL | Multi-host scheduling or availability needs exceed Compose — **not merely service count**. Plan 79 is the honest trigger |
| [70](plans/plan_70_type_annotations.md) | Type annotations | 20 | M | Opportunistic; pull forward with any large refactor of the modules being annotated |

## Superseded

Terminal, like the archive, but **not finished** — these were replaced, not
delivered, and merging them into `completed_plans.md` would make that file claim
work that never happened.

| Plan | Title | Superseded by |
|---|---|---|
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

[**completed_plans.md**](planning/completed_plans.md) — 114 rows, newest first, one row
per plan. It is the only record of what is finished; this file keeps no copy.
Dates reconstructed by Plan 146 Stage 1 are labelled *observed*, *corroborated*
or *inferred* so a guess is never mistaken for a record.

One-time reconciliation record:
[plan_state_reconciliation.md](planning/plan_state_reconciliation.md).
