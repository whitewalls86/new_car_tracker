# Plan 71: Airflow Migration

**Status:** Planned
**Priority:** Medium — strong portfolio signal; replaces n8n entirely

## Overview

Replace n8n with Apache Airflow. The n8n workflows are opaque JSON blobs — not reviewable in PRs, not unit-testable, not recognizable to hiring managers. Airflow DAGs are Python code: readable, diffable, testable, and on nearly every DE job description.

Estimated effort: **40–55 hours**. Several workflows that look complex in n8n collapse to Airflow primitives (Sensors, callbacks, native retry config).

---

## Airflow Primitives That Replace n8n Patterns

| n8n Pattern | Airflow Equivalent |
|---|---|
| Poll loop (Job Poller V2, health checks) | `HttpSensor` / custom `BaseSensorOperator` |
| While-loop with counter (Check Deploy Intent) | `PokeIntervalSensor` with `timeout` |
| Retry with wait (Build DBT 409 logic) | `retries=5, retry_delay=timedelta(seconds=60)` |
| Sub-workflow (Update n8n Runs Table) | Native Airflow run history — workflow disappears |
| Error Handler workflow | `on_failure_callback` on each DAG |
| Schedule trigger | `schedule` param on DAG |

---

## DAG Inventory

| DAG | Replaces | Notes |
|---|---|---|
| `scrape_listings` | Scrape Listings | Advance rotation → get configs → fan-out scrape jobs |
| `scrape_detail_pages` | Scrape Detail Pages V2 | Claim queue → batch → call scraper API |
| `results_processing` | Results Processing + Job Poller V2 | HttpSensor awaits jobs; per-artifact ok/retry/skip branching |
| `dbt_build` | Build DBT | HttpOperator + retry logic for 409 lock conflicts |
| `cleanup_artifacts` | Cleanup Artifacts | Archive → cleanup → mark-deleted chain |
| `cleanup_parquet` | Cleanup Parquet | Simple: find expired → API call → DB update |
| `orphan_checker` | Orphan Checker | Parallel SQL updates across 5 tables |
| `delete_stale_emails` | Delete Stale Request Emails | Single SQLExecuteOperator |

Workflows that disappear: **Job Poller V2** (becomes a Sensor inside `results_processing`), **Update n8n Runs Table** (Airflow metadata DB handles this natively), **Error Handler** (becomes `on_failure_callback`), **Check Service Health / Containers Up / Check Deploy Intent** (become Sensors).

---

## Architecture

- Airflow runs in Docker Compose alongside the existing stack (official `apache/airflow` image)
- Uses the **existing Postgres instance** as the Airflow metadata DB (separate schema)
- DAGs live in `airflow/dags/` — Python files, checked into git, reviewed in PRs
- Shared DB helpers in `shared/` reused across DAG tasks
- n8n stays running until all DAGs are validated, then decommissioned

---

## Rollout Order

1. **Airflow service** — add to `docker-compose.yml`, Flyway migration for metadata schema
2. **`dbt_build` DAG** — simplest standalone DAG; validates Airflow → dbt_runner connection
3. **`scrape_listings` DAG** — schedule-driven, linear flow, low risk
4. **`orphan_checker` + `delete_stale_emails` + `cleanup_parquet`** — maintenance DAGs, safe to run in parallel with n8n
5. **`cleanup_artifacts` DAG** — more complex, validate thoroughly before cutover
6. **`scrape_detail_pages` DAG** — validate against n8n output before switching
7. **`results_processing` DAG** — most complex; run shadowed against n8n until row counts match
8. **Disable n8n schedules** — cutover; n8n container stays up briefly as fallback
9. **Decommission n8n** — remove from docker-compose, archive workflow JSONs

---

## What Stays the Same

- All scraper API endpoints (`/scrape_results`, `/scrape/detail/batch`, `/jobs/completed`)
- dbt_runner HTTP interface
- Deploy intent flag and ops service
- Postgres schema — no migrations needed for the migration itself
