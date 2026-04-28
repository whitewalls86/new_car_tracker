# Plan 100: Historical Data Migration to MinIO

**Status:** COMPLETE (2026-04-27)
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

All paths follow the existing partition convention used by the flush DAGs.

| MinIO prefix | Source table(s) | Partition by |
|---|---|---|
| `bronze/artifacts/year=.../month=.../` | `raw_artifacts` | fetched_at |
| `bronze/artifact_events/year=.../month=.../` | `artifact_processing` + `raw_artifacts` | processed_at |
| `silver/srp/year=.../month=.../` | `srp_observations` | fetched_at |
| `silver/detail/year=.../month=.../` | `detail_observations` | fetched_at |
| `silver/carousel/year=.../month=.../` | `detail_carousel_hints` | fetched_at |
| `silver/price_events/year=.../month=.../` | `srp_observations` + `detail_observations` + `detail_carousel_hints` | fetched_at |
| `silver/vin_events/year=.../month=.../` | `srp_observations` + `detail_observations` | fetched_at |

---

## Schema Mappings

### bronze/artifacts — from `raw_artifacts`

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[raw_artifacts.artifact_id]` |
| `original_artifact_id` | `raw_artifacts.artifact_id` (preserved for debugging) |
| `artifact_type` | `raw_artifacts.artifact_type` |
| `url` | `raw_artifacts.url` |
| `minio_path` | `raw_artifacts.minio_path` |
| `fetched_at` | `raw_artifacts.fetched_at` |
| `status` | `raw_artifacts.status` |

### bronze/artifact_events — from `artifact_processing` JOIN `raw_artifacts`

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[artifact_processing.artifact_id]` |
| `artifact_type` | `raw_artifacts.artifact_type` |
| `minio_path` | `raw_artifacts.minio_path` |
| `fetched_at` | `raw_artifacts.fetched_at` |
| `listing_id` | `raw_artifacts.listing_id` |
| `run_id` | `raw_artifacts.run_id` |
| `processor` | `artifact_processing.processor` |
| `status` | `artifact_processing.status` |
| `message` | `artifact_processing.message` |
| `meta` | `artifact_processing.meta` |
| `event_at` | `artifact_processing.processed_at` |

### silver/srp — from `srp_observations`

Mirrors the `staging.silver_observations` schema with `source = 'srp'`.

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[srp_observations.artifact_id]` |
| `listing_id` | `srp_observations.listing_id` |
| `vin` | normalized: 17-char alphanumeric only, else NULL |
| `source` | `'srp'` |
| `listing_state` | `'active'` |
| `fetched_at` | `srp_observations.fetched_at` |
| `price` | `srp_observations.price` |
| `make` | `srp_observations.make` |
| `model` | `srp_observations.model` |
| `trim` | `srp_observations.trim` |
| `year` | `srp_observations.year` |
| `mileage` | `srp_observations.mileage` |
| `msrp` | `srp_observations.msrp` |
| `stock_type` | `srp_observations.stock_type` |
| `fuel_type` | `srp_observations.fuel_type` |
| `body_style` | `srp_observations.body_style` |
| `financing_type` | `srp_observations.financing_type` |
| `seller_zip` | `srp_observations.seller_zip` |
| `seller_customer_id` | `srp_observations.seller_customer_id` |
| `page_number` | `srp_observations.page_number` |
| `position_on_page` | `srp_observations.position_on_page` |
| `trid` | `srp_observations.trid` |
| `isa_context` | `srp_observations.isa_context` |
| `canonical_detail_url` | `srp_observations.canonical_detail_url` |

Note: `raw_vehicle_json` is not migrated — structured fields above cover the analytics surface.

### silver/detail — from `detail_observations`

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[detail_observations.artifact_id]` |
| `listing_id` | `detail_observations.listing_id` |
| `vin` | normalized: 17-char alphanumeric only, else NULL |
| `source` | `'detail'` |
| `listing_state` | `detail_observations.listing_state` |
| `fetched_at` | `detail_observations.fetched_at` |
| `price` | `detail_observations.price` |
| `make` | `detail_observations.make` |
| `model` | `detail_observations.model` |
| `trim` | `detail_observations.trim` |
| `year` | `detail_observations.year` |
| `mileage` | `detail_observations.mileage` |
| `msrp` | `detail_observations.msrp` |
| `stock_type` | `detail_observations.stock_type` |
| `fuel_type` | `detail_observations.fuel_type` |
| `body_style` | `detail_observations.body_style` |
| `dealer_name` | `detail_observations.dealer_name` |
| `dealer_zip` | `detail_observations.dealer_zip` |
| `customer_id` | `detail_observations.customer_id` |
| `canonical_detail_url` | via JOIN `raw_artifacts.url` |

### silver/carousel — from `detail_carousel_hints`

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[detail_carousel_hints.artifact_id]` |
| `listing_id` | `detail_carousel_hints.listing_id` |
| `source_listing_id` | `detail_carousel_hints.source_listing_id` |
| `source` | `'carousel'` |
| `listing_state` | `'active'` |
| `fetched_at` | `detail_carousel_hints.fetched_at` |
| `price` | `detail_carousel_hints.price` |
| `mileage` | `detail_carousel_hints.mileage` |
| `year` | `detail_carousel_hints.year` |
| `body` | `detail_carousel_hints.body` |
| `condition` | `detail_carousel_hints.condition` |

### silver/price_events

One row per observation row — the write-side audit trail reconstructed from legacy data.

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[...artifact_id]` |
| `listing_id` | observation `listing_id` |
| `vin` | normalized vin |
| `price` | observation `price` |
| `make` | observation `make` (NULL for carousel) |
| `model` | observation `model` (NULL for carousel) |
| `event_type` | `'upserted'` for srp/carousel; `'upserted'` or `'deleted'` for detail (from `listing_state`) |
| `source` | `'srp'` / `'detail'` / `'carousel'` |
| `event_at` | observation `fetched_at` |

### silver/vin_events

First observation of each `(listing_id, vin)` pair — approximates the mapping event history.
Only rows where a valid VIN is present.

| Parquet column | Source |
|---|---|
| `artifact_id` | `remap[...artifact_id]` |
| `vin` | normalized vin |
| `listing_id` | observation `listing_id` |
| `event_type` | `'mapped'` |
| `event_at` | `MIN(fetched_at)` per `(listing_id, vin)` |

Note: remapping history (a VIN moving to a new listing_id) cannot be reconstructed from the
legacy data — the legacy system did not record remap events. Only the earliest mapping per pair
is emitted.

---

## Migration Script Design

The script is a standalone Python CLI (`scripts/migrate_legacy_to_minio.py`).

**Requirements:**
- Chunked reads from Postgres (by month) — never loads a full table into memory
- Writes Parquet directly to MinIO using the same client/bucket config as the processing service
- Idempotent: checks for existing partition files before writing; skips completed months
- Progress logging: rows read, rows written, current month, elapsed time
- Dry-run mode: reads and maps without writing to MinIO

**Processing order:**
1. Build remap table (load all `raw_artifacts` IDs into memory — this is artifact metadata only,
   not observation rows, so size is manageable)
2. Write `bronze/artifacts/` and `bronze/artifact_events/`
3. Write `silver/srp/` month by month
4. Write `silver/detail/` month by month
5. Write `silver/carousel/` month by month
6. Write `silver/price_events/` (derived from steps 3–5, can reuse same monthly chunks)
7. Write `silver/vin_events/` (derived pass over srp + detail)
8. Run `setval` to advance `ops.artifacts_queue_artifact_id_seq`

---

## Validation

After the script completes:

```sql
-- 1. Row count audit per source per month
SELECT
    date_trunc('month', fetched_at) AS month,
    COUNT(*) AS legacy_rows
FROM srp_observations
WHERE fetched_at < '2026-04-21'
GROUP BY 1 ORDER BY 1;
-- Compare to Parquet row counts via DuckDB scan of silver/srp/
```

```sql
-- 2. Spot-check: known listing_id present in silver
-- (run via DuckDB against MinIO)
SELECT * FROM read_parquet('s3://silver/srp/**/*.parquet')
WHERE listing_id = '<known_id>'
ORDER BY fetched_at;
```

```sql
-- 3. Confirm sequence advanced past remapped range
SELECT last_value FROM ops.artifacts_queue_artifact_id_seq;
-- Must be >= max remapped artifact_id
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
- HOT table backfill (`ops.price_observations`) — already at 99%+ coverage from the live pipeline;
  `customer_id` backfill handled separately (see feature/postgres-hot-views branch)

## Deployment Sequencing

```
This plan (100)
  → Plan 96 (silver layer validation — now has full historical record to validate against)
    → Plan 90 (drop legacy Postgres tables)
```

Plan 100 must complete and validate before Plan 90 runs. Plan 96 validation gates Plan 90.
