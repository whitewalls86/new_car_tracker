# Cartracker - Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system
design patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-07-06)

Site is live at https://cartracker.info. All major pre-lakehouse foundations
are complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is now considered a
transition analytics endpoint rather than the future platform target.

**Now:** Plans 110 and 111 are complete as the storage/feature foundation for
adaptive refresh. The next arc is the Databricks-style lakehouse track: Delta +
MLflow backtesting, production adaptive refresh rollout, dbt migration away
from DuckDB, and governance/catalog expansion.

---

## Coordinating Roadmap

| Plan | Title | Status |
|------|-------|--------|
| [117](plan_117_storage_and_adaptive_refresh_roadmap.md) | Databricks-style lakehouse + adaptive refresh roadmap | Draft |

---

## Active

| Plan | Title | Status |
|------|-------|--------|
| [112](plan_112_refresh_policy_backtesting.md) | Delta + MLflow adaptive refresh backtesting | Draft |
| [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh integration | Draft |
| [114](plan_114_sectioned_html_artifact_audit.md) | Sectioned HTML artifact audit | Draft |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Detail unenriched circuit breaker | Draft |
| [118](plan_118_dbt_spark_migration.md) | dbt migration from DuckDB to Spark/Databricks-compatible execution | Draft |
| [119](plan_119_lakehouse_governance.md) | Lakehouse governance + catalog expansion | Draft |

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| - | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Resume when IP flagging requires it |
| - | **87** | Kafka event-driven layer | event stubs in processing/events.py |
| - | **88** | Kubernetes | 5+ services under management |
| - | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stable |
| - | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency with DB-backed auth and Caddy as sole gatekeeper |
| - | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services |
| - | [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Docker socket mount + ops Dockerfile change needed |

---

## Sequencing Rationale

**Plan 102 before Plan 87** - Track 4 of Plan 102 wires
`staging.artifacts_queue_events` properly. Plan 87 (Kafka) builds on that
foundation; the event stubs in `processing/events.py` become real Kafka
producer calls.

**Plan 79 whenever needed** - IP flagging is not currently active.
Prerequisites all exist. Provision Oracle Cloud VMs and fan out the DAG when
needed.

**Plans 110-119 lakehouse/adaptive-refresh sequence** - Plans 110 and 111 are
the completed foundation: storage normalization and adaptive-refresh feature
outputs. Plan 117 resets the forward roadmap toward a Databricks-style local
lakehouse. Plan 112 audits the feature outputs, proves Delta/MLflow
reproducibility, and selects a backtested policy. Plan 113 deploys only an
approved, pinned policy config into ops claim logic. Plan 118 moves dbt away
from DuckDB toward Spark/Databricks-compatible execution. Plan 119 expands
governance/catalog work around real workflows and policy promotion.

---

## Superseded

| Plan | Title | Reason |
|------|-------|--------|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |
| [90](plan_90_dbt_cleanup.md) | dbt decommission / dbt-duckdb migration | Superseded by Plan 102; DuckDB source layer done in Plan 96; the new forward migration is Plan 118 |

---

## Completed

See [completed_plans.md](completed_plans.md) for the full list. Recent
completions:

- **111** - Adaptive refresh feature foundation: listing-state fingerprints,
  state runs, volatility features, and initial priority outputs ready for audit
  in Plan 112 (2026-07-06)
- **110** - Storage layout hygiene: normalized storage foundation for the
  lakehouse/adaptive-refresh arc (2026-07-06)
- **102** - Full decommission: n8n infrastructure removed, dead scraper/ops
  endpoints deleted, archiver cleaned up, artifacts_queue_events wired
  (2026-04-29)
- **95** - Portfolio landing page: `/info` replaced with bespoke Jinja2
  template, live DB stats, service grid, highlights, and links (2026-05-04)
- **101** - Dashboard restructure: dashboard migrated to DuckDB-backed
  analytics, Data Health page added, Grafana sidebar linked (2026-04-29)
- **86** - Grafana observability stack: Prometheus, exporters, and provisioned
  dashboards behind admin auth (2026-04-29)
- **71** - Airflow migration: complete. All DAGs live; n8n removed by Plan 102
  (2026-04-29)
