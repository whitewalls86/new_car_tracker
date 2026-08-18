# Cartracker - Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system
design patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-08-18)

Site is live at https://cartracker.info. All major pre-lakehouse foundations
are complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is now considered a
transition analytics endpoint rather than the future platform target.

**Now:** Plan 120's final Gate F production verification is complete, and Plan
139 Stages A+B have taken the CI path every plan below pays from 333s to a
stable ~260s, with coverage now reported on every run. Plan 136 Stage 0 shipped
and was **verified against production** on 2026-08-18 — Airflow itself reports
the 20+20 pool, the shared anchor did not leak it to the other three services,
and `ct-pipeline-failures` now renders exactly two named instances where it
previously rendered a third reading `DAG [no value] failed`. Plan 140
takes the lead from there: give every service a healthcheck, then build the
container-health metric **once**, covering all of them, and make a coverage gap
fail CI rather than production. Plan 136 then resumes at Stage 1 for truthful
gauge freshness and Stage 2's solver-outcome counters, which is the layer no
healthcheck can supply — the solver was healthy for all eight hours. Only after
those counters have a baseline does Stage 4 get restart authority. Plan 142 then
turns the same drain and health primitives into a safe, explicit whole-host
maintenance workflow. Plan 141 then makes the newly bounded log pipeline and
its dashboards share one tested schema before Plan 134 begins its warning-log
observation window. The next data-integrity chain is Plan 133 -> Plan 132.
Plan 138 should land before the next major platform milestone, after which Plan
125 resumes at its remaining Gate C production measurement and Gate D reader
migration -- not at the already-proven early gates.

**Two incidents in four days (2026-08-14 solver, 2026-08-18 Airflow apiserver)
were each found by a human noticing downstream damage, not by an alert.** That
is why observability work occupies the top of this order. Plan 136 covers the two
components that actually failed; Plan 140 covers the other twenty-four before
they do.

Plan 112 remains intentionally paused until Plan 125 supplies stable
Iceberg-native inputs. Plans 114, 115, and 128 have completed their intended
work and no longer belong in the executable queue. Plans 124 and 131 closed out
2026-08-18. Plan 120 is built but still owes production verification. Plan 129
is a production system under rollout rather than a new build.

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
verified on 2026-08-18. What remains is **verification, not work**: no code is
owed on either row, and both land on the same Sunday because both ride a weekly
schedule.

| Plan | Check | Lands | What it proves |
|---|---|---|---|
| [135](plan_135_storage_observability.md) criterion 5 | `cartracker_parquet_data` publishes a real series | 2026-08-23, `disk_usage` slow-tier walk at 04:00 UTC | That the per-path panel can answer *"what is filling `/mnt/data`?"* -- the question this plan was written about. The MinIO volume is the majority of that disk's 59 GiB and has still never completed a walk, so the panel is currently silent on the bulk of it. Carry-forward is behaving as designed and `failed` is correctly 0 |
| [135](plan_135_storage_observability.md) Stage 5 | `prune_task_logs` completes its first scheduled run | 2026-08-23, `17 4 * * 0` | That Airflow's 30-day task-log retention is **enforced** rather than merely configured. The run reports run directories examined and deleted; `cartracker_airflow_logs` was 1.2M inodes and 87% of a 456s walk, so this is also what lets that volume move back to the daily tier |

Neither blocks Plan 136 or Plan 140. Record both results in
[plan_135_storage_observability.md](plan_135_storage_observability.md), and move
its row out of the operational table once they are green.

## Default build order

This is the default single-maintainer sequence. Do not start a lower row merely
because it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---:|---|---|
| 1 | [140](plan_140_service_health_contract.md) | Service health contract | Deploy and soak the 18 new healthchecks, then build the container-health metric and alerts; keep the explicit `oauth2-proxy` image exception loud (Stage 3 CI coverage is built) | 87 | M + soak | Plan 136 Stage 0 verified in production 2026-08-18 — nothing left blocking; soak Stage 1 before alerting on it |
| 2 | [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle and real liveness | Stages 1-2 metrics/freshness and solver counters, then Stage 3 drain-aware weekly recycle | 98 | M | Stage 0 verified; 0b moved into Plan 140 Stage 2; soak counters before Stage 4 auto-restart |
| 3 | [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Freeze the successful Ubuntu-update window as fixtures, then build separate maintenance intent, truthful drain status, and the checked-in host procedure | 86 | M + first observed window | Reuse Plan 136 drain semantics; final resume gate requires soaked Plan 140 health coverage |
| 4 | [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Freeze production-derived fixtures and baseline, then align parsing, labels, filters, and dashboard selectors | 85 | S + 24h soak | Does not block Plan 136; should precede Plan 134's warning-log observation window |
| 5 | [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | 88 | S | Plan 141 first; one-week soak before enforcement; pause if real failures need repair |
| 6 | [133](plan_133_pack_read_path_hardening.md) | Pack read-path hardening | Pack-aware existence check and month-sized sidecar-cache fix | 92 | S | Re-run April/May read-path verification; unlocks Plan 132 Stage 2 |
| 7 | [132](plan_132_unrecorded_artifact_recovery.md) | Recover unrecorded bronze artifacts | Run Stage 0 gates, build the manifest, then reparse a bounded cohort | 91 | L | Plan 133 before Stage 2; no destructive action in the audit stages |
| 8 | [138](plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, public stats and project-updates snapshots | 84 | L | Independent; land before the next major platform milestone |
| 9 | [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | 81 | XL | Plan 120 closeout; preserve Plan 136 freshness semantics during reader migration |
| 10 | [112](plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | 76 | L | Plan 125 stable Iceberg-native inputs |
| 11 | [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | 74 | M | Approved Plan 112 result; no live model dependency |
| 12 | [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet disposition | Codify the read-only baseline and row-complete disposition manifest | 72 | XL | Reuse Plan 132 provenance/backfill safety; deletion remains separately approved |
| 13 | [121](plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment | 63 | L | Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 14 | [139](plan_139_test_suite_maintenance.md) **C** | Profile the 92s `tests/integration/dbt/` step | Run it with `--durations=20` in CI and record the per-test breakdown before proposing any change | 60 | S | Measurement only; CI-only work — do not pip-install dbt locally |
| 15 | [119](plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | 58 | L | Stable Plan 125 catalog and reader contracts |
| 16 | [139](plan_139_test_suite_maintenance.md) **D** | Intent markers and the coverage-gate decision | Move `report_dbt_run_results.py` into `dbt_runner/`, add the `oneoff` marker per test class, decide the gate and the `airflow/dags`+`dashboard` exclusion in writing | 52 | S | Several weeks of Stage A coverage data; opportunistic filler |
| 17 | [126](plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 18 | [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |

**Plan [139](plan_139_test_suite_maintenance.md) occupies two rows rather than
one**, because scoring it as a single plan (62) hid that its first two stages
were two lines of YAML and a dev dependency. That argument paid off: **Stages
A+B shipped 2026-08-18** (PR #213) and left the build order. The critical path
is now a stable ~260s against a 333s baseline, and coverage reports on every run
at 88% with no gate. Only the scheduling edge earned it — the pip-cache
hypothesis was wrong and was reverted under Stage B's own verification rule.
Stages C and D never had that argument and take their turn by score.

The step that previously looked urgent — covering `ops/metrics/duckdb_gauges.py`
at 25% — **has moved into Plan 136 Stage 1**, and so appears in no Plan 139 row.
Plan 136 does not depend on that module, it rewrites it: Stage 1a sets the gauges
to `NaN` on refresh failure. Tests written against today's silent-stale behavior
would encode exactly what Stage 1 deletes, so they belong with the change.

Plan 136 Stage 4 is deliberately not part of the first slice: it grants restart
authority and should start only after Stage 2's outcome counters have established
a trustworthy baseline. Plan 134's observation window may run while Plans 133 or
136 proceed, but the endpoint-by-endpoint 500 rollout returns to this order when
the evidence is ready.

**Plan 140 now leads despite scoring 87 against Plan 136's 98**, because the
argument that put them adjacent resolved further once Stage 0 was underway.
The earlier reasoning was switching cost — 136 Stage 0b builds a container-health
metric and 140 Stage 2 generalizes it, so they are one set of files and one
mental model. Reading 140 against the compose file settles it more sharply than
that: **Docker reports no health status at all for a container without a
healthcheck**, and only 7 of 31 services have one. A metric built at Stage 0b
would be blank for the other 24, and a service with no healthcheck would be
indistinguishable from a healthy one — 140's own words, that it "would have
caught the apiserver incident and missed the solver incident."

So 0b is not sequenced before 140; it **is** 140 Stage 2, and it is strictly
better there: three states rather than two, with `-1` making "no healthcheck
configured" loud instead of absent. Stage 1 adds the healthchecks first, so the
metric covers everything the day it ships. Plan 136's Stage 0a and 0c landed on
2026-08-18 and were the only parts 140 was waiting on.

**Plan 142 follows Plan 136 despite scoring 86 against Plan 141's 85.** This is
dependency order, not an emergency ranking. Plan 140 must first make the resume
gate trustworthy, and Plan 136 Stage 3 establishes the drain-aware safe-boundary
pattern Plan 142 should reuse. Once those exist, host maintenance is recurring
production safety work with a fully observed first-window failure record, so it
belongs ahead of dashboard-contract cleanup. It remains below Plan 136 because
there is no current unpatched emergency and no application endpoint should gain
host package or reboot authority.

## Operational monitoring and completed implementation awaiting closeout

| Plan | State | Attention required |
|---|---|---|
| [114](plan_114_sectioned_html_artifact_audit.md) | Audit complete; sectioned storage rejected | Record as completed; its findings feed Plans 129-131 |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Production bugfix implemented | Record deployment/verification and move to completed |
| [128](plan_128_false_block_detection.md) | Phases 1-4 implemented; no historical repair chosen | Record final verification and move to completed |
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
| [132](plan_132_unrecorded_artifact_recovery.md) | Recovering unrecorded bronze artifacts | Draft — Stage 0 gate not run |
| [133](plan_133_pack_read_path_hardening.md) | Pack read path hardening | Draft — two non-blocking defects found verifying 131 Stage 3; do before 132 Stage 2 |
| [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Draft — measurement-first rollout not started |
| [135](plan_135_storage_observability.md) | Storage observability | **Complete 2026-08-18** — both disks visible, alerts proven, all log stores bounded, runbook live, `df /` 79% → 51%; criterion 5's MinIO half publishes on the first Sunday slow-tier walk (2026-08-23) |
| [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle + real liveness detection | **Stage 0 complete and verified in production 2026-08-18** (PR #214) — Airflow parses the 20+20 pool, no anchor leak, 14 of 100 connections; `ct-pipeline-failures` now renders exactly 2 named instances and no `[no value]` twin. 0b reassigned to Plan 140 Stage 2; Stages 1-4 not started |
| [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet recovery and disposition | Draft — read-only inventory complete; no deletion authorized |
| [138](plan_138_public_surface_refresh.md) | README and public portfolio surface refresh | Draft — audit complete; implementation not started |
| [139](plan_139_test_suite_maintenance.md) | Test suite construction and maintenance | **Stages A+B complete 2026-08-18** (PR #213) — coverage reported at 88%, CI path 333s → ~260s; Stages C/D remain queued as opportunistic filler; `duckdb_gauges` coverage transferred to Plan 136 Stage 1 |
| [140](plan_140_service_health_contract.md) | Service health contract | **Stage 1 implemented for every probeable service and Stage 3 built 2026-08-18** — 25 of 31 services now have healthchecks; five are deliberate one-shot/profile exemptions and distroless `oauth2-proxy` is an explicit unresolved hole. Deploy/soak and Stage 2 remain |
| [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Draft — routed from Plan 135 closeout; parsing, labels, privacy policy, dashboards, and capacity soak not started |
| [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Draft — separate maintenance intent, truthful drain, checked-in apt/reboot procedure, and Plan 140-gated resume not started |

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

**Plans 135/136/140/142/141 operational sequence** - Plan 135 closed the storage
blind spot. Plan 136 next establishes truthful freshness and solver-outcome
signals before any automatic restart is trusted, and Plan 140 generalizes its
health floor. Plan 142 reuses those health and drain primitives for deliberate
whole-host maintenance without granting application code reboot authority. Plan
141 then makes log parsing and dashboard semantics a tested contract. Plan 134
starts its warning-only observation window after 141 because
enforcement depends on a week of trustworthy evidence. Plan 133 is a small,
explicit prerequisite for Plan 132's packed
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
