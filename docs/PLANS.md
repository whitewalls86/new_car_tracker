# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system design patterns (schema layout, hot+staging, MinIO tiers, testing strategy, drain endpoints), see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-05-04)

Site is live at https://cartracker.info. All major plans complete: auth (82), data migration (81), CI/CD (62+63), integration testing (84), MinIO artifact store (97), processing service (93), Plan 99, Plan 100, Plan 96 (dbt-duckdb), Plan 86 (Grafana), Plan 101 (dashboard restructure), Plan 102 (full decommission), Plan 95 (portfolio landing page). V036 is the latest applied migration.

Airflow owns all scraping and maintenance. n8n is fully removed. dbt runs on DuckDB against MinIO silver. The dashboard is a pure analytics product backed by DuckDB mart tables. Grafana owns pipeline/infrastructure observability. The `/info` landing page is a purpose-built portfolio page with live DB stats.

**Now:** No active plans. See Backlog for upcoming work.

---

## Active

*None.*

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| — | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Resume when IP flagging requires it |
| — | **87** | Kafka event-driven layer | event stubs in processing/events.py |
| — | **88** | Kubernetes | 5+ services under management |
| — | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stable |
| — | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency with DB-backed auth and Caddy as sole gatekeeper |
| — | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services |

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
- **102** — Full decommission: n8n infra removed, dead scraper/ops endpoints deleted, archiver cleaned up, artifacts_queue_events wired; V034–V036 applied (2026-04-29)
- **95** — Portfolio landing page: /info replaced with bespoke Jinja2 template; hero, live DB stats, services grid, highlights, links (2026-05-04)
- **101** — Dashboard restructure: all 3 phases complete. Deleted 21 legacy SQL files + pipeline_health.py; migrated dashboard to DuckDB; new Data Health page (4 dbt models); Grafana sidebar link (2026-04-29)
- **86** — Grafana observability stack: Prometheus + statsd-exporter + postgres-exporter + node-exporter + 3 provisioned dashboards; V033 (metrics_user); Grafana at /grafana behind admin auth (2026-04-29)
- **71** — Airflow migration: COMPLETE. All DAGs live (scrape_listings, scrape_detail_pages, results_processing, all maintenance). n8n removed (Plan 102) (2026-04-29)
