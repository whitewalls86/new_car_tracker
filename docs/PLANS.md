# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system design patterns (schema layout, hot+staging, MinIO tiers, testing strategy, drain endpoints), see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-04-29)

Site is live at https://cartracker.info. All major plans complete: auth (82), data migration (81), CI/CD (62+63), integration testing (84), MinIO artifact store (97), processing service (93), Plan 99, Plan 100, Plan 96 (dbt-duckdb), Plan 86 (Grafana), Plan 101 (dashboard restructure). V033 is the latest applied migration.

Airflow owns all scraping and maintenance. n8n is deactivated. dbt runs on DuckDB against MinIO silver. The dashboard is a pure analytics product backed by DuckDB mart tables. Grafana owns pipeline/infrastructure observability.

**Now:** Cleaning up the n8n-era codebase (Plan 102) — removing dead tables, dead endpoints, dead infrastructure.

---

## Active

| Plan | Title | Notes |
|------|-------|-------|
| [102](plan_102_decommission.md) | Full decommission — n8n, legacy Postgres, dead service code | 4 tracks: n8n infra, scraper cleanup, ops service cleanup, archiver + event wiring |

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| — | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Resume when IP flagging requires it |
| — | **87** | Kafka event-driven layer | event stubs in processing/events.py; Plan 102 Track 4 wires artifacts_queue_events first |
| — | **88** | Kubernetes | 5+ services under management |
| — | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stable |
| — | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency with DB-backed auth and Caddy as sole gatekeeper |
| — | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services |
| — | [95](plan_95_portfolio_landing_page.md) | Portfolio landing page | Replace `/info` README dump |

---

## Sequencing Rationale

**Plan 102 before Plan 87** — Track 4 of Plan 102 wires `staging.artifacts_queue_events` properly. Plan 87 (Kafka) builds on that foundation — the event stubs in `processing/events.py` become real Kafka producer calls.

**Plan 79 whenever needed** — IP flagging not currently active. Prerequisites all exist. Provision Oracle Cloud VMs and fan out the DAG when needed.

---

## Superseded

| Plan | Title | Reason |
|---|---|---|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |
| [90](plan_90_dbt_cleanup.md) | dbt decommission / dbt-duckdb migration | Superseded by Plan 102; DuckDB source layer done (Plan 96); table drops + dead code removal covered in Plan 102 |

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Recent completions:
- **101** — Dashboard restructure: all 3 phases complete. Deleted 21 legacy SQL files + pipeline_health.py; migrated dashboard to DuckDB; new Data Health page (4 dbt models); Grafana sidebar link (2026-04-29)
- **86** — Grafana observability stack: Prometheus + statsd-exporter + postgres-exporter + node-exporter + 3 provisioned dashboards; V033 (metrics_user); Grafana at /grafana behind admin auth (2026-04-29)
- **71** — Airflow migration: COMPLETE. All DAGs live (scrape_listings, scrape_detail_pages, results_processing, all maintenance). n8n deactivated. Steps 14–15 absorbed into Plan 102 (infrastructure removal) (2026-04-29)
- **96** — dbt-duckdb analytics: silver Parquet source wired up, all models updated for DuckDB target, CI passing, server validated (169k rows, deal scores live) (2026-04-28)
- **100** — Historical data migration: ~13.7M legacy observations (srp/detail/carousel) migrated to MinIO Parquet; artifact ID remap applied; `ops.artifacts_queue_artifact_id_seq` advanced to 3741859 (2026-04-27)
