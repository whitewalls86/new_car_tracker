# Plan 71: Airflow DAG Translation

**Status:** Not started
**Priority:** Medium — portfolio value, no production impact

Translate the core n8n pipeline workflows into Apache Airflow DAGs as a parallel implementation. Not a replacement — n8n stays running in production. This is a portfolio demonstration showing understanding of DAG-based orchestration, the standard in enterprise data engineering.

## Why Airflow matters
Airflow is the most common data pipeline orchestrator in job descriptions. n8n and Airflow solve similar problems but differently:

| | n8n | Airflow |
|--|-----|---------|
| Concept | Event-driven automation | DAG-based pipeline orchestration |
| Built for | General service automation | Data pipelines specifically |
| Scheduling | Cron-like triggers | Cron + backfill + catchup |
| Dependencies | Node connections | Task dependencies in Python code |
| History | Execution log | Full DAG run history, queryable by date |
| Backfill | Not supported | Built-in |

## DAGs to implement
- **scrape_listings** — mirrors "Scrape Listings" n8n workflow
- **scrape_detail_pages** — mirrors "Scrape Detail Pages" workflow
- **dbt_build** — mirrors "Build DBT" sub-workflow
- **orphan_checker** — mirrors "Orphan Checker" workflow

## Deployment
Airflow runs locally via Docker (official `apache/airflow` image). Sits alongside the existing stack, reads from the same Postgres database. No production traffic goes through it initially.

## Notes
- Airflow has a free Docker Compose quickstart — low barrier to get running
- DAGs are Python files — directly demonstrates Python skills alongside orchestration skills
- The translation exercise itself is valuable: mapping n8n visual flows to Python code requires understanding both systems deeply
