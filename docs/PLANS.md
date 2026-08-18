# Cartracker - Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system
design patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-08-17)

Site is live at https://cartracker.info. All major pre-lakehouse foundations
are complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is now considered a
transition analytics endpoint rather than the future platform target.

**Now:** Finish Plan 135 and the small verification-only closeouts already in
flight. Then take Plan 136 through observable liveness and a drain-aware
scheduled recycle before granting the automatic circuit breaker restart
authority. The next data-integrity chain is Plan 133 -> Plan 132. Plan 138 should
land before the next major platform milestone, after which Plan 125 resumes at
its remaining Gate C production measurement and Gate D reader migration -- not
at the already-proven early gates.

Plan 112 remains intentionally paused until Plan 125 supplies stable
Iceberg-native inputs. Plans 114, 115, and 128 have completed their intended
work and no longer belong in the executable queue. Plans 120 and 124 are built
but still owe production verification. Plans 129 and 131 are production systems
under rollout/closeout rather than new builds.

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

| Order | Plan | Remaining work | Priority | Effort | Exit / dependency |
|---:|---|---|---:|---|---|
| 0.1 | [135](plan_135_storage_observability.md) | Finish both-disk capacity/inode visibility, alerts, bounded logs, disk-breakdown snapshots, and the runbook | 100 | S remaining | All seven success criteria pass; then move to completed |
| 0.2 | [124](plan_124_trawl_memory_guardrails.md) | Verify the deployed limits with `docker inspect`, `docker stats`, and a normal scrape cycle | 97 | XS | No new OOM evidence; update the plan status |
| 0.3 | [131](plan_131_packed_cold_storage.md) | Verify the recurring lifecycle DAG and close Stage 5 documentation | 94 | S | Lifecycle is single-flight, deploy-aware, measured, and alertable |
| 0.4 | [120](plan_120_ci_lake_snapshot_delivery.md) | Complete the remaining VM/Gate F verification | 82 | S | Required before Plan 125 relies on the snapshot path as a release gate |

## Default build order

This is the default single-maintainer sequence. Do not start a lower row merely
because it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---:|---|---|
| 1 | [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle and real liveness | Stages 1-2 metrics/freshness and solver counters, then Stage 3 drain-aware weekly recycle | 98 | M | Plan 135 monitoring conventions; soak counters before Stage 4 auto-restart |
| 2 | [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | 88 | S | One-week soak before enforcement; pause if real failures need repair |
| 3 | [133](plan_133_pack_read_path_hardening.md) | Pack read-path hardening | Pack-aware existence check and month-sized sidecar-cache fix | 92 | S | Re-run April/May read-path verification; unlocks Plan 132 Stage 2 |
| 4 | [132](plan_132_unrecorded_artifact_recovery.md) | Recover unrecorded bronze artifacts | Run Stage 0 gates, build the manifest, then reparse a bounded cohort | 91 | L | Plan 133 before Stage 2; no destructive action in the audit stages |
| 5 | [138](plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, public stats and project-updates snapshots | 84 | L | Independent; land before the next major platform milestone |
| 6 | [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | 81 | XL | Plan 120 closeout; preserve Plan 136 freshness semantics during reader migration |
| 7 | [112](plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | 76 | L | Plan 125 stable Iceberg-native inputs |
| 8 | [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | 74 | M | Approved Plan 112 result; no live model dependency |
| 9 | [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet disposition | Codify the read-only baseline and row-complete disposition manifest | 72 | XL | Reuse Plan 132 provenance/backfill safety; deletion remains separately approved |
| 10 | [121](plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment | 63 | L | Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 11 | [119](plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | 58 | L | Stable Plan 125 catalog and reader contracts |
| 12 | [126](plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 13 | [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |

Plan [139](plan_139_test_suite_maintenance.md) (test suite maintenance) is
deliberately unranked here: it is a skeleton of measurements, and only step 2 —
covering `ops/metrics/duckdb_gauges.py` at 25% — has a real dependency, since
Plan 136 Stage 1 builds its staleness convention on that module. Pull that step
forward with Plan 136; the rest is opportunistic.

Plan 136 Stage 4 is deliberately not part of the first slice: it grants restart
authority and should start only after Stage 2's outcome counters have established
a trustworthy baseline. Plan 134's observation window may run while Plans 133 or
136 proceed, but the endpoint-by-endpoint 500 rollout returns to this order when
the evidence is ready.

## Operational monitoring and completed implementation awaiting closeout

| Plan | State | Attention required |
|---|---|---|
| [114](plan_114_sectioned_html_artifact_audit.md) | Audit complete; sectioned storage rejected | Record as completed; its findings feed Plans 129-131 |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Production bugfix implemented | Record deployment/verification and move to completed |
| [128](plan_128_false_block_detection.md) | Phases 1-4 implemented; no historical repair chosen | Record final verification and move to completed |
| [129](plan_129_zstd_dictionary_compression.md) | Dictionary v1 in production; backfill/lifecycle monitoring | Watch metrics; no new design work unless the run deviates |
| [131](plan_131_packed_cold_storage.md) | April-June packed and pruned; Stage 5 recurring lifecycle in closeout | Complete order 0.3 above |

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
| [120](plan_120_ci_lake_snapshot_delivery.md) | CI + local lake snapshot delivery | Built through Gate F - VM verification pending |
| [121](plan_121_staging_environment.md) | Staging environment | Draft |
| [124](plan_124_trawl_memory_guardrails.md) | Trawl browser solver memory guardrails | Implemented - VM verification pending |
| [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB to Iceberg analytics migration | Gates 0.5, 0, A, and B complete; Gate C measurement next |
| [126](plan_126_basic_event_streaming.md) | Basic event streaming foundation | Draft / future |
| [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Draft / future |
| [128](plan_128_false_block_detection.md) | Cloudflare challenge pages swallowed as successful detail scrapes | Implemented through Phase 4 - closeout record pending |
| [129](plan_129_zstd_dictionary_compression.md) | Trained zstd dictionary compression for bronze HTML | In production — dict v1 live, backfill running |
| [130](plan_130_parser_input_projection.md) | Parser-input projection (truncating raw HTML) | Draft — blocked on 129 + taxonomy gap |
| [131](plan_131_packed_cold_storage.md) | Packed cold storage for bronze HTML | In production — **April-June packed and pruned, verified 2026-08-17**: 2.70M objects → 222, 447.5 GiB raw → 7.01 GiB, June at 82.8% physical; July not packed; Stage 3 deployed, Stage 4 run ([run sheet](runbook_plan_131_stage_3_4.md)); Stage 5 in progress |
| [132](plan_132_unrecorded_artifact_recovery.md) | Recovering unrecorded bronze artifacts | Draft — Stage 0 gate not run |
| [133](plan_133_pack_read_path_hardening.md) | Pack read path hardening | Draft — two non-blocking defects found verifying 131 Stage 3; do before 132 Stage 2 |
| [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Draft — measurement-first rollout not started |
| [135](plan_135_storage_observability.md) | Storage observability | In progress — complete the seven closeout criteria before Plan 136 |
| [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle + real liveness detection | Draft — written after the 2026-08-14 8h trawl outage no alert caught |
| [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet recovery and disposition | Draft — read-only inventory complete; no deletion authorized |
| [138](plan_138_public_surface_refresh.md) | README and public portfolio surface refresh | Draft — audit complete; implementation not started |
| [139](plan_139_test_suite_maintenance.md) | Test suite construction and maintenance | Skeleton — measured 2026-08-17, decisions not yet made |

---

## Backlog

| Priority | Effort | Plan | Title | Resume trigger / blocker |
|---:|---|---|---|---|
| 55 | M | [66](plan_66_sql_injection.md) | SQL injection audit | Pull forward after any new public mutation surface or auth-boundary change |
| 52 | S | [122](plan_122_runtime_scraper_fetch_config.md) | Runtime scraper fetch configuration | Plan 136 telemetry shows timeout or solver tuning needs runtime control |
| 40 | L | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | IP flagging or single-host throughput becomes the measured constraint |
| 38 | M | [94](plan_94_api_docs.md) | API documentation hub | Public or partner API consumption makes consolidated docs useful |
| 32 | L | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stops being stable or a second environment is approved |
| 30 | M | [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Re-scope after Plan 136's narrower restart-authority design is proven |
| 25 | XL | **88** | Kubernetes | Multi-host scheduling/availability needs exceed Compose, not merely service count |

---

## Sequencing Rationale

**Plans 135-138 operational/data-integrity sequence** - Plan 135 closes the
storage blind spot first. Plan 136 then establishes truthful freshness and
solver-outcome signals before any automatic restart is trusted. Plan 134 starts
its warning-only observation window early because enforcement depends on a week
of evidence. Plan 133 is a small, explicit prerequisite for Plan 132's packed
artifact reparse; Plan 132 should prove the recovery path before Plan 137 reuses
the same provenance and backfill safety at much larger scale. Plan 138 is
independent, but belongs before the next major platform milestone so the public
surface and its source-controlled work feed start from an accurate baseline.

**Plans 126-127 after the lakehouse/adaptive-refresh substrate** - The old
Plan 87 Kafka placeholder is superseded by Plan 126. The natural streaming seam
is the existing staging-event/outbox pattern, not direct app-to-broker writes.
Plan 126 should first prove Kafka-compatible event transport, replay, and a
low-risk consumer while preserving Airflow/batch parity. Plan 127 can then use
those events for adaptive scrape-control feedback once Plan 125 provides the
stable analytics substrate and Plans 112/113 clarify refresh-policy promotion.

**Plan 79 whenever needed** - IP flagging is not currently active.
Prerequisites all exist. Provision Oracle Cloud VMs and fan out the DAG when
needed.

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
Plan 125 now moves dbt/analytics from DuckDB toward Iceberg-native tables. Once
that substrate is stable, Plan 112 resumes policy backtesting and Plan 113
deploys only an approved, pinned policy config into ops claim logic. Plans 114,
121, and 119 can follow in whichever order is most useful: raw HTML retention
research, staging environment, and governance/catalog expansion.

**Plan 138 is independent public-surface work.** It should land before another
major platform milestone adds more documentation drift. Its copy must describe
Plan 125 as a proven migration track, not as the current production serving path,
and it does not block or change any lakehouse, storage, or liveness plan.

---

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
| [114](plan_114_sectioned_html_artifact_audit.md) | 2026-08-10 | Measured sectioned HTML storage on production data, rejected it on storage economics, and identified dictionary compression and packing as the better reversible path. |
| [128](plan_128_false_block_detection.md) | 2026-07-20 | Fixed challenge-page misclassification, preserved cooldown state, reconciled cleanup behavior, and closed the historical-repair decision. |
| [111](plan_111_adaptive_detail_refresh.md) | 2026-07-06 | Built listing-state fingerprints, state runs, volatility features, and initial adaptive-refresh priority outputs. |
| [110](implementation_plan_110_storage_layout_hygiene.md) | 2026-07-06 | Normalized the storage foundation for the lakehouse and adaptive-refresh work. |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | 2026-07-01 | Stopped successfully scraped but unenriched listings from entering a pathological 15-minute retry loop. |
| [95](plan_95_portfolio_landing_page.md) | 2026-05-04 | Shipped the first public portfolio landing page with pipeline context and live statistics. |
