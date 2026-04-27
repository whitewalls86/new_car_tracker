# Plan 100: Historical Data Migration to MinIO

**Status:** PLANNED
**Priority:** High — prerequisite for Plan 90 (dbt + legacy table drop)
**Depends on:** Plan 93 (processing service, COMPLETE), Plan 97 (MinIO artifact store, COMPLETE)
**Blocks:** Plan 96 (silver layer validation), Plan 90 (dbt cleanup)

---

## Overview

The new processing pipeline (Plan 93) writes to MinIO as its system of record. The legacy n8n
pipeline wrote to Postgres tables (`raw_artifacts`, `artifact_processing`, `srp_observations`,
`detail_observations`, `detail_carousel_hints`). Plan 90 will drop those tables, but before that
can happen their data must be migrated to MinIO so the full observation history is preserved.

MinIO is the only target. The Postgres staging event tables (`staging.artifacts_queue_events`,
`staging.price_observation_events`, etc.) are transient flush buffers — writing to them and
waiting for the flush DAG would be slow and indirect. This plan writes directly to MinIO Parquet
in the same partition layout used by the live pipeline.

---

## Artifact ID Remapping

The legacy `raw_artifacts` table and `ops.artifacts_queue` both use a `bigserial` starting at 1.
Their ID spaces overlap. Since artifact_id is used as a join key across all observation and event
Parquet files, collisions would make lineage queries ambiguous.

**Strategy:** reassign all legacy artifact IDs to a non-overlapping range at migration time.

1. At script start, read the current high-water mark:
   ```sql
   SELECT MAX(artifact_id) FROM ops.artifacts_queue;
   -- e.g. returns 8412
   ```

2. Build a remap: assign each `raw_artifacts` row a new ID starting at `max + 1`, ordered by
   original `artifact_id` (preserving relative chronology):
   ```python
   remap = {old_id: offset + i for i, old_id in enumerate(sorted(raw_artifact_ids), start=1)}
   ```

3. Apply `remap[old_id]` everywhere in the migration output — artifact metadata, all observation
   Parquet, all event Parquet.

4. After migration completes, advance the `ops.artifacts_queue` sequence past the remapped range
   so future inserts never collide:
   ```sql
   SELECT setval('ops.artifacts_queue_artifact_id_seq', <max_remapped_id>);
   ```

**Important:** never use `TRUNCATE ops.artifacts_queue RESTART IDENTITY` after this point. The
sequence high-water mark is load-bearing for the global artifact ID guarantee.

---

## Cutoff Date

The Airflow processing service went live on **2026-04-21**. MinIO already contains silver data
from that date forward. Migrate only legacy rows with `fetched_at < '2026-04-21'` to avoid
duplicates. The two datasets join cleanly at that boundary.

---

## MinIO Output Layout

All paths match the exact schemas used by the flush DAGs in production.

| MinIO prefix | Source table(s) | Partition by | Schema file |
|---|---|---|---|
| `silver/observations/source=.../obs_year=.../obs_month=.../obs_day=.../` | `srp_observations`, `detail_observations`, `detail_carousel_hints` | source + fetched_at | `flush_silver_observations.py` |
| `ops/artifacts_queue_events/year=.../month=.../` | `artifact_processing` + `raw_artifacts` | processed_at | `flush_staging_events.py` |
| `ops/price_observation_events/year=.../month=.../` | derived from all three observation tables | fetched_at | `flush_staging_events.py` |
| `ops/vin_to_listing_events/year=.../month=.../` | first `(listing_id, vin)` per pair from srp + detail | first fetched_at | `flush_staging_events.py` |

All three observation sources land in a single **unified** `silver/observations` table with the `source` column
(`'srp'` / `'detail'` / `'carousel'`) as a partition key. This matches what the `flush_silver_observations` DAG
writes for live data.

Migration files use a `legacy-<source>-YYYY-MM-{i}.parquet` basename so idempotency checks can
detect completed months without a separate marker file.

---

## Schema Mappings

All schemas match the PyArrow schemas defined in the archiver flush processors.
See `scripts/migrate_legacy_to_minio.py` for the exact field mapping.

### silver/observations — from `srp_observations`, `detail_observations`, `detail_carousel_hints`

Schema matches `archiver/processors/flush_silver_observations.py _SCHEMA`.
`source` column distinguishes rows: `'srp'` / `'detail'` / `'carousel'`.

Key field notes:
- `vin` — normalized: 17-char alphanumeric only (`[A-HJ-NPR-Z0-9]{17}`), else NULL
- `listing_state` — `'active'` for srp/carousel; from `detail_observations.listing_state` for detail
- `canonical_detail_url` — from `srp_observations.canonical_detail_url`; for detail via JOIN `raw_artifacts.url`; for carousel constructed as `https://www.cars.com/vehicledetail/{listing_id}/`
- `written_at` — fixed to migration run timestamp (not a per-row timestamp)
- `raw_vehicle_json` — not migrated; structured fields cover analytics needs

### ops/artifacts_queue_events — from `artifact_processing` JOIN `raw_artifacts`

Schema matches `archiver/processors/flush_staging_events.py _ARTIFACTS_QUEUE_EVENTS_SCHEMA`.

Key field notes:
- `minio_path` — NULL for pre-MinIO artifacts (legacy scraper used local `filepath`); populated for any row where `raw_artifacts.minio_path` was set during the shadow period
- `event_id` — synthetic negative IDs (`< -1_000_000_000_000`) to distinguish migration rows
- `listing_id` — `raw_artifacts.listing_id` cast from UUID to text
- `processor` column from `artifact_processing` is dropped (not in target schema)

### ops/price_observation_events — derived from observation tables

One row per observation row. Schema matches `_PRICE_OBSERVATION_EVENTS_SCHEMA`.
- `event_type` = `'upserted'` for srp/carousel; `'upserted'` or `'deleted'` for detail based on `listing_state = 'unlisted'`

### ops/vin_to_listing_events — first VIN mapping per `(listing_id, vin)` pair

Single aggregated pass over srp + detail (carousel has no VINs).
- `event_type` = `'mapped'` for all rows
- `previous_listing_id` = NULL (remap history not reconstructable from legacy data)

---

## Migration Script Design

The script is a standalone Python CLI (`scripts/migrate_legacy_to_minio.py`).

**Requirements:**
- Month-by-month reads from Postgres — never loads a full table into memory
- Writes Parquet directly to MinIO using the same PyArrow schemas as the flush DAGs
- Idempotent: uses source-prefixed basenames (`legacy-srp-YYYY-MM-0.parquet`) so completed
  months can be detected by glob without a separate marker file
- Progress logging: rows read, rows written, current month, elapsed time
- Dry-run mode (`--dry-run`): reads and maps without writing to MinIO
- Negative synthetic `event_id` values (`< -1_000_000_000_000`) distinguish migration rows from production rows

**Processing order:**
1. Build artifact ID remap (`SELECT artifact_id FROM raw_artifacts ORDER BY artifact_id`)
2. Load VIN map from `analytics.int_listing_to_vin` (for carousel enrichment)
3. Write `silver/observations` (source=srp) + `ops/price_observation_events` (srp) month by month
4. Write `silver/observations` (source=detail) + `ops/price_observation_events` (detail) month by month
5. Write `silver/observations` (source=carousel) + `ops/price_observation_events` (carousel) month by month
   - Carousel rows: VIN enriched from `analytics.int_listing_to_vin`; make/model parsed from `body` field
6. Write `ops/artifacts_queue_events` from `artifact_processing` JOIN `raw_artifacts` month by month
7. Write `ops/vin_to_listing_events` (single aggregated pass over srp + detail)
8. Run `setval` to advance `ops.artifacts_queue_artifact_id_seq` past all remapped IDs

---

## Validation

After the script completes:

```sql
-- 1. Row count audit: Postgres vs MinIO (run against Postgres)
SELECT
    'srp' AS source,
    date_trunc('month', fetched_at) AS month,
    COUNT(*) AS legacy_rows
FROM srp_observations WHERE fetched_at < '2026-04-21'
GROUP BY 1, 2
UNION ALL
SELECT 'detail', date_trunc('month', fetched_at), COUNT(*)
FROM detail_observations WHERE fetched_at < '2026-04-21'
GROUP BY 1, 2
UNION ALL
SELECT 'carousel', date_trunc('month', fetched_at), COUNT(*)
FROM detail_carousel_hints WHERE fetched_at < '2026-04-21'
GROUP BY 1, 2
ORDER BY 1, 2;
-- Compare to DuckDB scan of silver/observations/ grouped by source + obs_year + obs_month
```

```sql
-- 2. Spot-check via DuckDB against MinIO
SELECT * FROM read_parquet('s3://bronze/silver/observations/**/*.parquet', hive_partitioning=true)
WHERE listing_id = '<known_id>'
ORDER BY fetched_at;
```

```sql
-- 3. Confirm sequence advanced past remapped range
SELECT last_value FROM ops.artifacts_queue_artifact_id_seq;
-- Must be >= (MAX(artifact_id FROM raw_artifacts WHERE fetched_at < '2026-04-21') + pre-migration max)
```

---

## Prerequisites

- [ ] Audit row counts and date ranges across all four legacy tables (run the audit query first)
- [ ] Confirm MinIO bucket/prefix layout matches what flush DAGs write (no partition mismatch)
- [ ] Silver flush DAG is running (live data continues flowing to MinIO during migration)

## Out of Scope

- `raw_vehicle_json` from `srp_observations` — not migrated; structured fields cover analytics needs
- Full VIN remap history — not reconstructable from legacy data; `vin_events` captures first-seen only
- Writing to Postgres staging tables — MinIO is the direct target throughout
- HOT table backfills (`ops.price_observations`, `ops.vin_to_listing`) — HOT tables are operational only; the live pipeline populates them as new scrapes run

## Deployment Sequencing

```
This plan (100)
  → Plan 96 (silver layer validation — now has full historical record to validate against)
    → Plan 90 (drop legacy Postgres tables)
```

Plan 100 must complete and validate before Plan 90 runs. Plan 96 validation gates Plan 90.
