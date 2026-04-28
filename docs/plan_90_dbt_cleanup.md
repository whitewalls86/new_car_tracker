# Plan 90: dbt Migration to dbt-duckdb

**Status:** Superseded by Plan 101
**Previously titled:** "dbt Decommission"

> **Superseded (2026-04-27):** The DuckDB source layer was implemented in Plan 96. The dashboard restructure (including `dashboard/db.py` connection switch) and the definition of what the dashboard becomes are now owned by [Plan 101](plan_101_dashboard_restructure.md). The Postgres observation table drops (Flyway migration) remain valid work but are gated on Plan 96 production validation completing. See Plan 101 for the full picture.

---

## Overview

With the processing service (Plan 93) writing all observations to MinIO silver, dbt's Postgres source tables (`srp_observations`, `detail_observations`) are no longer populated. Rather than decommissioning dbt, this plan migrates it to use DuckDB as the execution engine, reading directly from MinIO silver Parquet via `httpfs`.

dbt remains the transformation, testing, lineage, and documentation layer. The execution backend changes; nothing else does.

---

## The Decision

**Option A — Full decommission (previously recommended, now rejected):** Replace all dbt models with ad-hoc DuckDB queries. Loses lineage tracking, data quality tests, schema documentation, and dbt docs — high-value for both operational correctness and portfolio purposes.

**Option B — Migrate to dbt-duckdb (selected):** Swap the execution engine from Postgres to DuckDB. DuckDB reads MinIO silver Parquet via httpfs. Existing models and tests survive with minor SQL compatibility fixes. `dbt_runner`, the `dbt_build` Airflow DAG, Layer 2 CI tests, and dbt docs all stay intact.

---

## Prerequisites

- Plan 96 complete: silver validated, dbt-duckdb sources confirmed against production data
- At least 2 weeks of silver data in production

---

## What Changes

### dbt Dockerfile

Swap adapter:

```dockerfile
# before
RUN pip install dbt-postgres==1.8.2 --no-cache-dir

# after
RUN pip install dbt-duckdb==1.10.1 --no-cache-dir
```

### profiles.yml — new duckdb target

```yaml
cartracker:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: /tmp/cartracker.duckdb
      extensions:
        - httpfs
        - parquet
      settings:
        s3_endpoint: "minio:9000"
        s3_url_style: path
        s3_use_ssl: "false"
        s3_region: us-east-1
        s3_access_key_id: "{{ env_var('MINIO_ROOT_USER') }}"
        s3_secret_access_key: "{{ env_var('MINIO_ROOT_PASSWORD') }}"
    ci:
      type: duckdb
      path: /tmp/cartracker_ci.duckdb
      extensions:
        - httpfs
        - parquet
      settings:
        s3_endpoint: "{{ env_var('DBT_S3_ENDPOINT', 'minio:9000') }}"
        s3_url_style: path
        s3_use_ssl: "false"
        s3_region: us-east-1
        s3_access_key_id: "{{ env_var('MINIO_ROOT_USER') }}"
        s3_secret_access_key: "{{ env_var('MINIO_ROOT_PASSWORD') }}"
```

### docker-compose.yml — dbt_runner env vars

Add MinIO credentials to `dbt_runner`:

```yaml
dbt_runner:
  environment:
    MINIO_ROOT_USER: ${MINIO_ROOT_USER}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
```

### dbt Sources

Source definitions move from Postgres tables to external MinIO Parquet. See Plan 96 for the source YAML and model `FROM` clause patterns.

### SQL Compatibility Fixes

Run existing models against DuckDB and fix as needed. Known incompatibilities to watch:

| Issue | Postgres | DuckDB | Fix |
|---|---|---|---|
| Implicit casting | Lenient | Strict | Add explicit `CAST()` or enable `old_implicit_casting: true` temporarily |
| `GROUP BY` | Allows uniquely-determined non-grouped cols | Requires all non-aggregated cols | Add missing columns or wrap in `ANY_VALUE()` |
| `to_date()` | Supported | Not supported | Replace with `strptime()` |
| Integer division | `1/2 = 0` | `1/2 = 0.5` | Use `//` for integer division where needed |

---

## What Gets Removed (Postgres cleanup)

These tables are now fully redundant — data is preserved in silver Parquet.

| Table | Reason |
|---|---|
| `srp_observations` | Observations in silver |
| `detail_observations` | Observations in silver |
| `detail_carousel_hints` | Carousel observations in silver |
| `raw_artifacts` | Replaced by `artifacts_queue` (Plan 97) |
| `artifact_processing` | Replaced by `artifacts_queue` (Plan 97) |

`dbt_intents` is also removed — no longer populated.

---

## Gold Layer and Dashboard

Mart models materialize as DuckDB tables (in-memory or persisted to `/tmp/cartracker.duckdb`). The dashboard (`dashboard/db.py`) switches from `psycopg2` → `duckdb`, reading from the DuckDB file for analytical queries and retaining a direct Postgres connection via `postgres_scan()` for operational HOT table data (`price_observations`, `vin_to_listing`).

This is the only part of the codebase outside `dbt/` that changes.

---

## What Stays Unchanged

- `dbt_runner` service (reconfigured, not removed)
- `dbt_build` Airflow DAG
- All dbt models (minor SQL edits only)
- All dbt tests and `schema.yml` contracts
- Layer 2 CI tests — dbt test assertions run identically against DuckDB
- dbt docs

---

## Rollout Order

1. Add `duckdb` target to `profiles.yml`; update `dbt/Dockerfile` to `dbt-duckdb==1.10.1`
2. On a feature branch, run all existing models against DuckDB in dev — catalogue SQL compat failures
3. Fix compat issues model by model; keep existing tests passing throughout
4. Redefine dbt sources as external MinIO Parquet sources (see Plan 96)
5. Confirm all dbt tests pass against production silver data
6. Add MinIO env vars to `dbt_runner` in `docker-compose.yml`
7. Run Layer 2 CI tests — confirm passing
8. Migrate `dashboard/db.py` from psycopg2 → duckdb
9. Drop legacy Postgres source tables (Flyway migration)
10. Deploy to production
