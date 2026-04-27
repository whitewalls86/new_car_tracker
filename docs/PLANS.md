# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system design patterns (schema layout, hot+staging, MinIO tiers, testing strategy, drain endpoints), see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-04-27)

Site is live at https://cartracker.info. Auth (Plan 82), data migration (Plan 81), CI/CD (Plans 62+63), integration testing (Plan 84), MinIO artifact store (Plan 97), processing service (Plan 93), and Plan 99 are complete. V018–V030 migrations shipped. Airflow is running with all maintenance DAGs live, the `results_processing` DAG, the `flush_silver_observations` + `flush_staging_events` DAGs, and the new `scrape_listings` + `scrape_detail_pages` scrape DAGs (merged, pending unpause).

The ops staleness view (`ops_vehicle_staleness`) and scrape queue (`ops_detail_scrape_queue`) are now plain Postgres views reading HOT tables directly (V029). The dbt ops models have been deleted. `customer_id IS NULL` is the enrichment signal replacing the old dbt `dealer_unenriched` join.

The processing service now has 52 integration tests covering all write paths end-to-end. Dashboard has Airflow DAG run visibility (`airflow_dag_runs.sql`) and pipeline health queries updated to use `ops.artifacts_queue` directly.

The remaining transition sequence is: restart `ops` + `scraper`, unpause `scrape_listings` + `scrape_detail_pages`, validate shadow period, disable n8n schedules (Plan 71 step 14) → decommission n8n (step 15) → Plan 100 → Plan 96 → Plan 90.

---

## Active

| Plan | Title | Notes |
|------|-------|-------|
| [71](plan_71_airflow.md) | Airflow migration (steps 14–15) | Steps 1–13 done. Next: disable n8n schedules (step 14), decommission n8n (step 15). Shadow period first. |

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| 1 | [71](plan_71_airflow.md) | Airflow migration (steps 14–15) | Steps 1–13 complete. Shadow period running. Steps 14–15 (n8n cutover + decommission) unblocked — validate DAGs in shadow then cut over. |
| 2 | [100](plan_100_historical_data_migration.md) | Historical data migration to MinIO | Silver flush DAGs running (unblocked). Migrates legacy n8n tables to MinIO Parquet with artifact ID remapping. Prerequisite for Plan 96 and Plan 90. |
| 3 | [96](plan_96_silver_layer.md) | Silver layer validation + DuckDB analytics | Plan 100 complete (full historical record in MinIO) |
| 4 | [90](plan_90_dbt_cleanup.md) | dbt decommission | Plan 96 validation gates (2+ weeks of silver production data) |
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

**`scrape_listings` + `scrape_detail_pages` DAGs now** — all ops endpoints exist, MinIO write path exists, V029 views are live. Shadow-run alongside n8n immediately. `scrape_detail_pages` going live unblocks Plan 79 with no other dependencies.

**Plan 100 before Plan 96** — Plan 96 validates that silver contains the complete observation history. That history only exists once legacy Postgres tables are migrated to MinIO. Plan 100 must complete first; until then Plan 96 validation would only cover post-2026-04-21 data.

**Plan 96 before Plan 90** — Plan 90 drops dbt and legacy Postgres observation tables. Those drops are irreversible. Plan 96's five validation checks are the explicit go/no-go gate: don't decommission dbt until silver has been running in production for at least 2 weeks and all checks pass.

**Plan 79 whenever needed** — IP flagging not currently active. The technical prerequisites (MinIO write path, ops claim endpoints, `scrape_detail_pages` DAG) either exist or are being built now. When IP flagging returns, provision Oracle Cloud VMs, add Airflow connections, and fan out the DAG. No other plans need to be complete first.

---

## In-Progress / Nearly Complete

| Plan | Title | Status |
|------|-------|--------|
| [71](plan_71_airflow.md) | Airflow migration | Steps 1–13 merged; steps 14–15 (n8n cutover + decommission) pending |
| [92](plan_92_service_drain.md) | Service drain `/ready` endpoints | archiver + dbt_runner + scraper done; processing (Plan 93) pending |
| [91](plan_91_uuid_type_cleanup.md) | UUID column type fixes | Scope collapsed to 2 columns; absorbed into V018 |

---

## Superseded

| Plan | Title | Reason |
|---|---|---|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Recent completions:
- **71 (steps 8–9, 13)** — `scrape_listings` + `scrape_detail_pages` DAGs merged; `advance_rotation` gap check on `search_configs.last_queued_at`; scraper gains `/ready`, loses `advance_rotation`. 52 processing integration tests. Dashboard Airflow DAG run panel + pipeline queries updated to `ops.artifacts_queue`. V030 migration (2026-04-27)
- **99** — Per-source staleness: `customer_id` added to `ops.price_observations` (V028); enrichment flag replaces dbt dealer join (2026-04-27)
- **V029** — Plain Postgres ops views: `ops_vehicle_staleness` + `ops_detail_scrape_queue` rewritten as HOT-table-direct views; dbt ops models deleted; n8n cutover now unblocked (2026-04-27)
- **Silver flush DAGs** — `flush_silver_observations` + `flush_staging_events` DAGs live; staging buffer → MinIO Parquet on schedule (PR #86, 2026-04-21)
- **93** — Processing service: SRP/detail/carousel write paths, silver staging buffer, tracked_models, V021–V025 (2026-04-21)
- **97** — MinIO-first artifact store; `ops.artifacts_queue` live; V017 deployed (2026-04-20)
