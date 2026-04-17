# Plan 90: dbt Decommission

**Status:** Planned — blocked on Plan 96 validation
**Priority:** Medium — do not start until silver has 2+ weeks of production data
**Previously titled:** "dbt Intermediate Cleanup"

---

## Overview

With the processing service (Plan 93) writing all observations to MinIO silver as the primary store, dbt's source tables (`srp_observations`, `detail_observations`) are no longer populated. This plan decides dbt's fate and migrates the analytics layer accordingly.

---

## The Decision

**Option A — Full decommission (recommended):** Replace all dbt models with DuckDB queries against MinIO silver and the Postgres HOT tables. Remove `dbt_runner` from docker-compose. Remove the `dbt_build` Airflow DAG.

**Option B — Reduced dbt:** Keep dbt for complex derived models (deal scores, market benchmarks). Point dbt at silver via DuckDB FDW or materialized Postgres tables populated from silver. More moving parts, harder to justify.

**Recommended: Option A.** Dashboard charts map cleanly to DuckDB queries. With Airflow handling orchestration, dbt adds complexity without adding value. The portfolio signal from dbt is already captured in git history — the story becomes "I replaced a batch transformation layer with a streaming write path and event-log analytics."

---

## Prerequisites

- Plan 96 complete: silver validated, DuckDB queries confirmed correct against production data
- At least 2 weeks of silver data in production
- All dashboard charts confirmed serviceable from DuckDB queries

---

## What Gets Removed

| Component | Replacement |
|---|---|
| All dbt models | DuckDB queries against silver |
| `dbt_runner` service | Removed from docker-compose |
| `dbt_build` Airflow DAG | Removed |
| `dbt_intents` table | Removed |
| Layer 2 dbt logic tests in CI | Replaced by DuckDB query validation tests |
| `srp_observations` Postgres table | Dropped (data preserved in silver) |
| `detail_observations` Postgres table | Dropped (data preserved in silver) |
| `detail_carousel_hints` Postgres table | Dropped (carousel observations in silver) |
| `raw_artifacts` table | Dropped (replaced by `artifacts_queue` in Plan 97) |
| `artifact_processing` table | Dropped (replaced by `artifacts_queue` in Plan 97) |

---

## What the Analytics Layer Looks Like After

DuckDB pointed at MinIO silver + Postgres HOT tables via `postgres_scan()`.

```sql
-- Vehicle snapshot (replaces mart_vehicle_snapshot)
SELECT
    p.vin, p.listing_id, p.price, p.make, p.model, p.last_seen_at,
    attrs.trim, attrs.year, attrs.fuel_type, attrs.body_style
FROM postgres_scan('postgresql://...', 'public', 'price_observations') p
LEFT JOIN (
    SELECT DISTINCT ON (vin) vin, trim, year, fuel_type, body_style
    FROM read_parquet('s3://bucket/silver/observations/year=*/month=*/*.parquet',
                      hive_partitioning=true)
    WHERE source = 'detail'
    ORDER BY vin, fetched_at DESC
) attrs USING (vin);

-- Deal score (replaces mart_deal_scores)
SELECT
    p.vin, p.price, p.make, p.model,
    market.median_price,
    round(p.price::numeric / NULLIF(market.median_price, 0), 2) AS price_to_market_ratio
FROM postgres_scan('postgresql://...', 'public', 'price_observations') p
JOIN (
    SELECT make, model, median(price)::integer AS median_price
    FROM read_parquet('s3://bucket/silver/observations/year=*/month=*/*.parquet',
                      hive_partitioning=true)
    WHERE source = 'detail'
      AND listing_state = 'active'
      AND price IS NOT NULL
    GROUP BY make, model
) market USING (make, model);
```

---

## Rollout Order

1. Confirm all dashboard queries can be served from DuckDB (test in staging against production silver data)
2. Remove dbt models one layer at a time, running dashboard spot-checks after each deletion
3. Drop `srp_observations`, `detail_observations`, `detail_carousel_hints` Postgres tables
4. Drop `raw_artifacts`, `artifact_processing` tables
5. Remove `dbt_runner` from docker-compose
6. Remove `dbt_build` DAG from Airflow
7. Remove Layer 2 dbt tests from CI; add DuckDB query validation tests
