# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system design patterns (schema layout, hot+staging, MinIO tiers, testing strategy, drain endpoints), see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-04-21)

Site is live at https://cartracker.info. Auth (Plan 82), data migration (Plan 81), CI/CD (Plans 62+63), integration testing (Plan 84), MinIO artifact store (Plan 97), and the processing service (Plan 93) are complete. V018–V025 migrations shipped. Airflow is running with all maintenance DAGs live plus the `results_processing` DAG (Plan 71 step 11). Ops coordination endpoints (`advance_rotation`, `claim-batch`, `release`) are implemented.

Architecture has transitioned to a MinIO-first design: Postgres holds only hot operational state (current inventory, VIN mappings, work queues, tracked models); MinIO holds the complete observation record via a staging buffer (`staging.silver_observations`) flushed to partitioned Parquet. The remaining transition sequence is: V019 → n8n cutover → Plan 96 → Plan 90.

---

## Active

| Plan | Title | Notes |
|------|-------|-------|
| [71](plan_71_airflow.md) | Airflow migration (steps 8–9, 12–15) | Steps 1–6 done. Steps 7 (V018), 10 (Plan 93), 11 (`results_processing` DAG) done. Next: steps 8–9 (`scrape_listings` + `scrape_detail_pages` DAGs), then V019, n8n cutover, scraper slimming. |

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| 1 | [99](plan_99_price_observations_per_source.md) | Per-source price observations | Nothing — unblocked. Changes `ops.price_observations` PK to `(listing_id, source)`; required for V019 staleness view correctness. |
| 2 | **V019** | View migration | Plan 99. Rewrites `ops_vehicle_staleness` and `ops_detail_scrape_queue` as plain Postgres views reading HOT tables; inlines `stg_blocked_cooldown` backoff formula; removes dbt dependency from the scrape queue. Prerequisite for n8n cutover. |
| 3 | [71](plan_71_airflow.md) | Airflow migration (steps 8–9, 12–15) | Steps 8–9 (`scrape_listings` + `scrape_detail_pages` DAGs) unblocked. Steps 12–15 (V019, scraper slimming, n8n cutover) blocked on V019. |
| 3 | **Silver flush DAG** | Flush `staging.silver_observations` → MinIO Parquet | Plan 93 deployed and writing to staging table |
| 4 | [100](plan_100_historical_data_migration.md) | Historical data migration to MinIO | Silver flush DAG running. Migrates legacy n8n tables (raw_artifacts, artifact_processing, srp/detail/carousel observations) to MinIO Parquet with artifact ID remapping. Prerequisite for Plan 96 and Plan 90. |
| 5 | [96](plan_96_silver_layer.md) | Silver layer validation + DuckDB analytics | Plan 100 complete (full historical record in MinIO) |
| 6 | [90](plan_90_dbt_cleanup.md) | dbt decommission | Plan 96 validation gates |
| — | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | `scrape_detail_pages` Airflow DAG live (Plan 71 step 9); resume when IP flagging requires it |
| — | **86** | Grafana observability stack | Airflow live so real DAG metrics exist to observe |
| — | **87** | Kafka event-driven layer | Airflow DAGs producing events + multiple consumers exist |
| — | **88** | Kubernetes | 5+ services under management |
| — | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stable |
| — | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency with DB-backed auth and Caddy as sole gatekeeper |
| — | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services |
| — | [95](plan_95_portfolio_landing_page.md) | Portfolio landing page | Replace `/info` README dump |

---

## Sequencing Rationale

**Plan 99 first** — `ops.price_observations` currently stores one row per listing regardless of source. The V019 staleness view needs to distinguish when a detail scrape last ran vs when an SRP scan last ran — that requires per-source rows. Plan 99 changes the PK to `(listing_id, source)` and wires `source` through the processing service write paths. Must ship before V019 so the view reads the correct table shape.

**V019 next** — the scrape queue view (`ops_detail_scrape_queue`) currently depends on dbt models. V019 rewrites it as a plain Postgres view reading the per-source HOT table, removing the dbt dependency. Must happen before n8n is decommissioned and before Plan 90 drops dbt, because both paths would break the queue otherwise.

**`scrape_listings` + `scrape_detail_pages` DAGs now** — all ops endpoints exist, MinIO write path exists. Shadow-run alongside n8n immediately. `scrape_detail_pages` going live unblocks Plan 79 with no other dependencies.

**Silver flush DAG** — `staging.silver_observations` is a buffer; a scheduled DAG flushes rows to partitioned Parquet (`source` + `date`) in MinIO and DELETEs flushed rows. Must be running before Plan 96 validation can begin.

**Plan 96 before Plan 90** — Plan 90 drops dbt and legacy Postgres observation tables. Those drops are irreversible. Plan 96's five validation checks are the explicit go/no-go gate: don't decommission dbt until silver has been running in production for at least 2 weeks and all checks pass.

**Plan 79 whenever needed** — IP flagging not currently active. The technical prerequisites (MinIO write path, ops claim endpoints, `scrape_detail_pages` DAG) either exist or are being built now. When IP flagging returns, provision Oracle Cloud VMs, add Airflow connections, and fan out the DAG. No other plans need to be complete first.

---

## In-Progress / Nearly Complete

| Plan | Title | Status |
|------|-------|--------|
| [71](plan_71_airflow.md) | Airflow migration | Steps 1–7, 10–11 done; steps 8–9, 12–15 pending |
| [92](plan_92_service_drain.md) | Service drain `/ready` endpoints | archiver + dbt_runner done; scraper (Plan 71 step 13) + processing (Plan 93) pending |
| [91](plan_91_uuid_type_cleanup.md) | UUID column type fixes | Scope collapsed to 2 columns; absorbed into V018 |

---

## Superseded

| Plan | Title | Reason |
|---|---|---|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Recent completions:
- **93** — Processing service: SRP/detail/carousel write paths, silver staging buffer, tracked_models, V021–V025 (2026-04-21)
- **97** — MinIO-first artifact store; `ops.artifacts_queue` live; V017 deployed (2026-04-20)
- **98** — Bronze data architecture; schema complete in V017 (2026-04-20)
- **84** — Integration testing: 71 SQL smoke tests + dbt logic coverage + ops API tests (2026-04-16)
- **82** — DB-backed auth with access requests (PRs #64–#67, 2026-04-14)
- **81** — Data migration local → cloud (2026-04-14)
- **80** — 403 cooldown with exponential backoff
- **78** — FlareSolverr + curl_cffi impersonation
- **62+63** — CI/CD + Flyway schema migrations
