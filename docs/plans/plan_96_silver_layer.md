# Plan 96: Silver Layer — Primary Observation Store

**Status:** COMPLETE (2026-04-28, `feature/duck-dbt`)
**Priority:** High — Plan 93 makes silver the primary write path; this plan validates it and establishes the analytics query surface that gates Plan 90

---

## Overview

Plan 93 implements the silver write path: `silver_writer.py` writes all parsed observations to MinIO as append-only hive-partitioned Parquet after each Postgres commit. Silver is the **permanent, complete record** of every observation ever processed — not a secondary archive sitting alongside Postgres observation tables.

This plan covers validation after Plan 93 ships, the DuckDB analytics query surface that replaces dbt, and the reprocessing capability that bronze+silver enables.

---

## Position in the Architecture

```
Scraper → MinIO bronze/html/         (raw HTML — Plan 97)
               ↓
     Processing service (Plan 93)
          ↓                    ↓
   MinIO silver/           Postgres HOT tables
   observations/           price_observations
   (permanent record)      vin_to_listing
   (primary — this plan)   (current state only)
```

Postgres HOT tables hold only current state and can be reconstructed from silver if needed. Silver is authoritative.

---

## Silver Schema

Single partition tree, all observation types unified:

```
silver/observations/year=.../month=.../part-*.parquet
```

| Column | Type | Notes |
|---|---|---|
| artifact_id | bigint | Source artifact |
| listing_id | text | |
| vin | text | nullable — null for carousel before VIN discovery |
| price | integer | nullable — null for unlisted |
| make | text | |
| model | text | |
| mileage | integer | nullable — detail only |
| listing_state | text | 'active' \| 'unlisted' |
| source | text | 'srp' \| 'detail' \| 'carousel' |
| fetched_at | timestamptz | When the artifact was fetched |
| written_at | timestamptz | When the silver row was written |

No separate `srp_observations` / `detail_observations` partitions. `source` is the discriminator.

---

## Validation (after Plan 93 ships, a few days of production data)

**1. Completeness check**
Count `artifacts_queue` rows with `status='complete'` in a date range. Silver observation count should be ≥ that (most artifacts produce multiple rows). Allow for logged `silver_write_failures`.

**2. Postgres consistency**
Every `listing_id` in `price_observations` should have at least one silver row with `fetched_at <= price_observations.last_seen_at`. No active vehicle should be in Postgres without a silver record.

**3. Spot check**
Pick 10 artifact_ids; DuckDB query confirms rows exist with correct fields and expected `source` values.

**4. Unlisted integrity**
`listing_id` values absent from `price_observations` (deleted as unlisted) should have a silver row with `listing_state='unlisted'`.

**5. `silver_write_failures` monitoring**
If consistently > 0 in `/process/batch` responses, investigate and fix before proceeding to Plan 90.

---

## dbt-duckdb Source Configuration

Plan 90 migrates dbt to use DuckDB as its execution engine rather than decommissioning it. This section documents how dbt sources are redefined to point at MinIO silver Parquet, and confirms that existing models can run against them.

### Source YAML

Replace the existing Postgres source tables with an external MinIO source:

```yaml
version: 2

sources:
  - name: silver
    description: "Parsed observations — hive-partitioned Parquet in MinIO"
    tables:
      - name: observations
        description: "All parsed observations from SRP, detail, and carousel pages"
        meta:
          external_location: >
            read_parquet(
              's3://bronze/silver/observations/**/*.parquet',
              hive_partitioning=true
            )
        columns:
          - name: artifact_id
          - name: listing_id
          - name: vin
          - name: source
          - name: listing_state
          - name: fetched_at
          - name: price
          - name: make
          - name: model
          - name: trim
          - name: year
          - name: mileage
          - name: obs_year
          - name: obs_month
          - name: obs_day
```

### Model FROM clause pattern

Existing models that previously referenced `srp_observations` or `detail_observations` are rewritten to filter `{{ source('silver', 'observations') }}` by `source` column:

```sql
-- previously: FROM {{ source('public', 'srp_observations') }}
-- now:
FROM {{ source('silver', 'observations') }}
WHERE source = 'srp'

-- previously: FROM {{ source('public', 'detail_observations') }}
-- now:
FROM {{ source('silver', 'observations') }}
WHERE source = 'detail'
```

### Postgres HOT tables via postgres_scan()

Mart models that need current operational state join against Postgres directly:

```sql
-- mart_vehicle_snapshot pattern
SELECT
    p.vin, p.listing_id, p.price, p.make, p.model, p.last_seen_at,
    attrs.trim, attrs.year, attrs.fuel_type, attrs.body_style
FROM {{ source('ops', 'price_observations') }} p   -- postgres_scan() under the hood
LEFT JOIN (
    SELECT DISTINCT ON (vin) vin, trim, year, fuel_type, body_style
    FROM {{ source('silver', 'observations') }}
    WHERE source = 'detail'
    ORDER BY vin, fetched_at DESC
) attrs USING (vin)
```

The `ops` source is defined separately pointing at the Postgres HOT tables via dbt-duckdb's `postgres_scan()` integration.

---

## New-Config Backfill

When a new `search_configs` entry is added, silver provides the historical record to mine for vehicles that were seen before the config existed:

```sql
-- Find carousel observations for the new make/model never yet detail-scraped
SELECT DISTINCT listing_id, max(fetched_at) AS last_seen
FROM read_parquet('s3://bucket/silver/observations/year=*/month=*/*.parquet',
                  hive_partitioning=true)
WHERE source = 'carousel'
  AND lower(make) = lower($make)
  AND lower(model) = lower($model)
  AND listing_id NOT IN (
      SELECT listing_id FROM postgres_scan('postgresql://...', 'public', 'vin_to_listing')
  )
GROUP BY listing_id;
```

Results are upserted into `price_observations` so the scrape queue picks them up for detail scraping.

---

## Reprocessing Capability

With bronze (raw HTML, Plan 97) and silver (parsed observations) both in MinIO, the replay loop is:

1. Parser bug identified and fixed in `processing/processors/`
2. Query silver to identify affected artifact_ids and time range
3. Run one-off reprocessing: read HTML from bronze, parse with fixed parser, write corrected rows to silver
4. Optionally: update `price_observations` Postgres HOT table from corrected silver rows

This is the primary operational justification for the silver layer. The bronze HTML archive pays for storage; silver makes corrected output queryable without a full Postgres rebuild.

---

## Retention

Silver is retained indefinitely. Parsed observation rows are small (~hundreds of bytes vs. tens of KB for raw HTML). The complete historical record is the point. No cleanup workflow needed.

Bronze HTML follows the existing retention policy. Since Plan 97 routes files directly to MinIO, the archiver's copy step is replaced by a simple deletion workflow.

---

## Gate for Plan 90

Plan 90 (dbt migration to dbt-duckdb) must not start until all five validation checks above pass, silver has at least 2 weeks of production data, and the dbt-duckdb source configuration above has been confirmed to execute correctly against production silver data on a feature branch.
