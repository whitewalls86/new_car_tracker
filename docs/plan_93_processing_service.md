# Plan 93: Processing Service Core

**Status:** Restart — design supersedes previous in-progress version
**Priority:** High — Phase 4 of Plan 71 (Airflow Migration)
**Depends on:** Plan 97 (MinIO artifact store + `artifacts_queue`)
**Parent plan:** Plan 71 (Airflow Migration)

---

## Overview

The processing service owns the full artifact processing lifecycle:
- Claim unprocessed artifacts from `artifacts_queue`
- Read artifact files from MinIO
- Parse HTML using copied parsers
- Write to MinIO silver `observations` (primary — permanent record)
- Upsert Postgres HOT tables: `price_observations`, `vin_to_listing`
- Manage `artifacts_queue` status (complete / retry / skip)

This plan supersedes the previous Plan 93 design, which assumed local disk reads, `artifact_processing` as the work queue, and Postgres `srp_observations`/`detail_observations` as primary observation tables. Those assumptions are replaced by the architecture in Plans 97 and 96.

---

## Design Decisions

### MinIO silver is the primary write

Every parsed observation goes to MinIO silver. The Postgres HOT tables (`price_observations`, `vin_to_listing`) are updated as a secondary step — they represent current state only, not history. Silver is the permanent record.

Silver write failure is non-fatal: logged, counted in the `/process/batch` response, does not roll back Postgres writes. Postgres write failure is fatal: artifact marked `retry`.

### `artifacts_queue` replaces `raw_artifacts` + `artifact_processing`

The service claims artifacts by flipping `status='pending'` → `'processing'`. On success: `'complete'`. On parse failure: `'retry'`. On intentional skip: `'skip'`. These are the only two tables the service touches for work tracking.

### Pre-upsert VIN lookup

Before any upsert to `price_observations`, the service batch-queries `vin_to_listing` for all listing_ids in the artifact. This ensures:
- Carousel hints with a known VIN get it populated at write time
- VINs that have moved listing_ids are correctly identified and deduplicated

### Unlisted = DELETE, not UPDATE

When a detail page confirms a vehicle is unlisted, the row is deleted from `price_observations`. Presence in the table is the active state. No `listing_state` column needed.

### Carousel filtering at write time

Carousel hints are parsed for make/model at processing time. Only hints matching an active `search_configs` entry are upserted into `price_observations`. All carousel observations go to MinIO silver regardless of make/model match — this enables new-config backfill (mining silver for historically seen vehicles when a new search config is added).

### Single endpoint, type dispatch internally

`POST /process/batch` processes both `results_page` and `detail_page` artifacts in one call. Dispatch is on `artifacts_queue.artifact_type`. Optional `?artifact_type=` filter for targeted draining.

### Parser strategy: copy, not move

`scraper/processors/parse_detail_page.py` and `scraper/processors/results_page_cards.py` are copied to `processing/processors/`. Scraper copies remain untouched through the shadow period. Parser changes must land in both locations until the scraper parsers are deleted in Plan 71 Phase 7.

### Emit stubs for Kafka readiness

```python
# processing/events.py — log-only today; Plan 87 swaps in real implementations
def emit_price_updated(vin: str, price: int, listing_id: str, source: str): ...
def emit_listing_removed(vin: str | None, listing_id: str): ...
def emit_vin_mapped(listing_id: str, vin: str): ...
```

### `/ready` endpoint

Wraps the batch handler body in `active_job()` from `shared/job_counter.py`. Returns `{"ready": false, "reason": "batch in progress"}` while a batch is running. Plan 92 scaffolding.

---

## Postgres HOT Table Schemas

### `price_observations`

Current live inventory. Keyed by `listing_id` with nullable `vin`. Unique constraint on `vin` where not null ensures one row per VIN when VIN is known.

```sql
CREATE TABLE price_observations (
    listing_id        text         PRIMARY KEY,
    vin               text,
    price             integer,
    make              text,
    model             text,
    last_seen_at      timestamptz  NOT NULL,
    last_artifact_id  bigint       NOT NULL
);

CREATE UNIQUE INDEX ON price_observations (vin) WHERE vin IS NOT NULL;
```

### `vin_to_listing`

Authoritative VIN→listing_id mapping. Updated whenever a VIN is confirmed for a listing. The `listing_id` index supports reverse lookups during the pre-upsert VIN resolution step.

```sql
CREATE TABLE vin_to_listing (
    vin          text         PRIMARY KEY,
    listing_id   text         NOT NULL,
    mapped_at    timestamptz  NOT NULL,
    artifact_id  bigint       NOT NULL
);

CREATE INDEX ON vin_to_listing (listing_id);
```

---

## MinIO Audit Log: `artifacts_queue_events`

Append-only Parquet event log — one row per status transition. This is the
durable, replayable record of every artifact's lifecycle. Postgres
`artifacts_queue` holds only the current hot state; this dataset holds the
full history.

```
ops/artifacts_queue_events/
    year=.../month=.../status=.../artifact_type=.../<uuid>.parquet
```

| Column | Type | Notes |
|---|---|---|
| artifact_id | int64 | FK to `artifacts_queue.artifact_id` |
| status | string | Value at time of event: `pending` \| `processing` \| `complete` \| `retry` \| `skip` |
| event_at | timestamp[us, UTC] | When the transition occurred |
| minio_path | string | S3 URI of the raw HTML artifact |
| artifact_type | string | `results_page` \| `detail_page` |
| fetched_at | timestamp[us, UTC] | When the artifact was scraped |
| listing_id | string (nullable) | Populated for detail_page artifacts |
| run_id | string (nullable) | Scraper run that produced the artifact |

**Write responsibilities:**
- `status='pending'` — written by the **scraper** (via `shared.minio.write_artifact_event()`) immediately after the `artifacts_queue` INSERT
- All other statuses (`processing`, `complete`, `retry`, `skip`) — written by the **processing service** on every status transition, alongside the Postgres UPDATE

**Why not a single UPDATE per artifact?** Each status transition is a new Parquet row. This means the full lifecycle of any artifact can be reconstructed by reading all rows for a given `artifact_id` ordered by `event_at` — no Postgres required. This is what makes replayability and auditability possible at Plan 96 and beyond.

`write_artifact_event()` lives in `shared/minio.py` and uses `get_s3fs()` + pyarrow. Non-fatal in both callers — a failed event write logs a warning and does not roll back the Postgres insert or status update.

---

## MinIO Silver Schema

Unified Parquet, all observation types in one partition tree. No separate srp/detail partitions — `source` column is the discriminator.

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

---

## File Structure

```
processing/
  app.py
  db.py                               # asyncpg pool (same pattern as scraper/db.py)
  queries.py                          # SQL constants loaded from sql/
  processors/
    __init__.py
    parse_detail_page.py              # copied from scraper/processors/
    results_page_cards.py             # copied from scraper/processors/
  writers/
    __init__.py
    srp_writer.py
    detail_writer.py
    silver_writer.py                  # primary observation write to MinIO
  events.py                           # emit stubs
  routers/
    __init__.py
    batch.py                          # POST /process/batch
    artifact.py                       # POST /process/artifact/{artifact_id}
  sql/
    # --- claim/release ---
    claim_artifacts.sql               # claim N from artifacts_queue by status + type
    mark_artifact_status.sql          # complete / retry / skip
    release_detail_claims.sql

    # --- vin lookup ---
    batch_lookup_vin_to_listing.sql   # pre-upsert VIN resolution for all listing_ids in artifact

    # --- price_observations ---
    upsert_price_observation.sql
    delete_price_observation.sql      # unlisted path

    # --- vin_to_listing ---
    upsert_vin_to_listing.sql

    # --- search config filter ---
    get_active_search_configs.sql     # used for carousel make/model filtering
```

---

## SRP Write Path

```
1. Claim up to N 'pending'/'retry' results_page artifacts from artifacts_queue

2. For each artifact:
   a. Read HTML from MinIO (artifacts_queue.minio_path)
   b. parse_cars_results_page_html_v3(html) → listings[]
   c. On parse failure: mark status='retry'; continue

3. Batch-lookup vin_to_listing for all listing_ids in this artifact

4. For each listing:
   a. Resolve vin = listing.vin OR lookup result (whichever is present)
   b. Upsert price_observations:
      - listing_id PRIMARY KEY
      - vin from step a (may be null)
      - price, make, model, last_seen_at, last_artifact_id
   c. If vin present: upsert vin_to_listing (recency-only: only update if mapped_at newer)

5. Write all listings to MinIO silver (source='srp')
   Failure: log + increment silver_write_failures counter; do NOT roll back step 4

6. Mark artifact status='complete' in artifacts_queue

7. write_artifact_event(artifact_id, status='complete', ...)

8. Emit stubs (after commit):
   emit_price_updated(vin, price, listing_id, 'srp')  per listing with price + vin
   emit_vin_mapped(listing_id, vin)                    if vin_to_listing entry was new
```

---

## Detail Write Path — Active (`listing_state != 'unlisted'`)

```
1. Claim up to N 'pending'/'retry' detail_page artifacts from artifacts_queue

2. For each artifact:
   a. Read HTML from MinIO
   b. parse_cars_detail_page_html_v1(html, url) → primary{}, carousel[], dealers[], meta{}
   c. On parse failure: mark status='retry'; continue

3. Batch-lookup vin_to_listing for primary listing_id + all carousel listing_ids

4. Primary observation:
   a. Resolve vin = primary.vin OR lookup result
   b. Check if vin already exists at a different listing_id in price_observations:
      - If yes (vehicle changed listing IDs): update that row's listing_id; delete old
        listing_id-keyed row if it was VIN-less (was a carousel placeholder)
      - If no: standard upsert
   c. Upsert price_observations (make, model, mileage, price, last_seen_at)
   d. If vin present: upsert vin_to_listing

5. Dealers: upsert dealers table (COALESCE — never overwrite real data with NULL)

6. Carousel:
   a. Parse make/model from each hint body
   b. Filter hints against active search_configs (make/model match)
   c. Matching hints: upsert price_observations (listing_id key, vin from lookup or NULL)
   d. All hints (matched + unmatched): write to MinIO silver (source='carousel')

7. Write primary observation to MinIO silver (source='detail')
   Failure: log + increment counter; do NOT roll back steps 4-5

8. Release detail_scrape_claims: UPDATE status='processed' WHERE listing_id = primary.listing_id

9. Mark artifact status='complete' in artifacts_queue

10. write_artifact_event(artifact_id, status='complete', ...)

11. Emit stubs (after commit):
    emit_price_updated(vin, price, listing_id, 'detail')
    emit_vin_mapped(listing_id, vin)  if vin_to_listing entry was new
```

## Detail Write Path — Unlisted (`listing_state = 'unlisted'`)

```
1-2. Same claim + read + parse

3. DELETE FROM price_observations WHERE listing_id = primary.listing_id

4. Write to MinIO silver (source='detail', listing_state='unlisted', price=NULL)
   Failure: log + increment counter; do NOT roll back step 3

5. Release detail_scrape_claims for this listing_id

6. Mark artifact status='complete' in artifacts_queue

7. write_artifact_event(artifact_id, status='complete', ...)

8. Emit: emit_listing_removed(vin, listing_id)
```

### VIN collision / relisting

When a detail scrape of listing BBB discovers VIN001, and VIN001 already has a `price_observations` row from listing AAA (previous listing):
- Update existing VIN001 row: set `listing_id='BBB'`, `last_seen_at=now()`
- Delete AAA row if it was a VIN-less placeholder (`vin IS NULL`)
- Upsert `vin_to_listing`: VIN001 → BBB

---

## Airflow DAG

`airflow/dags/results_processing.py` — structure unchanged from Plan 71:

```
deploy_intent_sensor
  → processing_up (health sensor)
  → process_batch (POST /process/batch, wait for response)
  → [conditional] dbt trigger (pending Plan 90 decision)
```

`/process/batch` response shape:
```json
{
  "srp_count": 14,
  "detail_count": 87,
  "retry_count": 2,
  "skip_count": 0,
  "silver_write_failures": 0
}
```

The conditional dbt trigger remains in the DAG until Plan 90 decommissions dbt. If both counts are 0, no downstream trigger fires.

---

## Unit Tests

`tests/processing/`

### Parser tests
Mirror of `tests/scraper/processors/` tests, importing from `processing.processors.*`. Catches drift between the scraper and processing copies immediately.

### `tests/processing/writers/test_srp_writer.py`

| Test | Checks |
|---|---|
| `test_listing_with_vin_upserts_price_obs` | VIN-bearing listing produces price_observations payload |
| `test_listing_without_vin_uses_lookup` | VIN resolved from vin_to_listing lookup when absent from parse result |
| `test_srp_source_is_srp` | `source='srp'` on all silver payloads |
| `test_vin_to_listing_recency_guard` | Older SRP does not overwrite newer vin_to_listing mapping |

### `tests/processing/writers/test_detail_writer.py`

| Test | Checks |
|---|---|
| `test_active_dispatch` | Active listing_state routes to active path |
| `test_unlisted_dispatch` | Unlisted routes to DELETE path |
| `test_unlisted_produces_delete_not_upsert` | DELETE payload generated for unlisted; no upsert payload |
| `test_vin_relisting_replaces_old_row` | When VIN found at new listing_id, old row is replaced |
| `test_carousel_filtered_by_search_configs` | Only matching make/model hints upserted to price_observations |
| `test_carousel_unmatched_goes_to_silver_only` | Unmatched hints written to silver regardless |
| `test_carousel_vin_from_lookup` | Carousel hint with known listing_id gets vin populated |
| `test_carousel_vin_null_when_unknown` | Carousel hint with unknown listing_id writes vin=NULL |
| `test_carousel_sanity_filter_drops_null_price` | Hints with price IS NULL dropped before any write |
| `test_carousel_sanity_filter_drops_null_body` | Hints with body IS NULL dropped before any write |

---

## Integration Tests (Layer 3)

`tests/integration/processing/`

### SRP artifact
```
Given: artifacts_queue row (artifact_type='results_page', minio_path → test HTML, 3 listings, 2 with VINs)

When:  POST /process/batch?artifact_type=results_page

Then:  price_observations has 3 rows (2 with vin populated, 1 with vin=NULL)
       vin_to_listing has 2 entries
       artifacts_queue.status = 'complete'
       response: srp_count=1, detail_count=0
```

### SRP does not downgrade vin_to_listing
```
Given: vin_to_listing has (VIN001 → AAA, mapped_at=T+10)
       SRP artifact from T+5 also sees listing AAA with VIN001

When:  SRP artifact processed

Then:  vin_to_listing.mapped_at for VIN001 is still T+10
```

### Detail artifact — active
```
Given: artifacts_queue row (detail_page, active HTML)
       detail_scrape_claims row for listing_id (status='claimed')

When:  POST /process/batch?artifact_type=detail_page

Then:  price_observations row upserted with vin, make, model, mileage
       vin_to_listing entry exists for this listing_id
       detail_scrape_claims.status = 'processed'
       artifacts_queue.status = 'complete'
       response: srp_count=0, detail_count=1
```

### Detail artifact — unlisted
```
Given: artifacts_queue row (detail_page, unlisted HTML)
       price_observations has a row for this listing_id

When:  POST /process/batch?artifact_type=detail_page

Then:  price_observations row DELETED for this listing_id
       artifacts_queue.status = 'complete'
```

### VIN relisting
```
Given: price_observations has (listing_id=AAA, vin=VIN001)
       vin_to_listing has (VIN001 → AAA)
       detail artifact for listing BBB discovers VIN001

When:  detail artifact processed

Then:  price_observations row for VIN001 now has listing_id=BBB
       old AAA row deleted (it was the VIN001 row with listing_id=AAA)
       vin_to_listing has VIN001 → BBB
```

### Parse failure → retry
```
Given: artifacts_queue row pointing to a corrupt or missing MinIO object

When:  POST /process/batch

Then:  artifacts_queue.status = 'retry'
       no price_observations writes
       response: retry_count=1
```

### /ready drain signal
```
Given: processing service is idle

When:  GET /ready

Then:  {"ready": true}
```

---

## Implementation Steps

1. Flyway migration: create `price_observations`, `vin_to_listing` tables with indexes
2. Copy parsers: `scraper/processors/parse_detail_page.py` → `processing/processors/`; same for `results_page_cards.py`
3. Add `processing/db.py` (asyncpg pool)
4. Add `processing/queries.py` (SQL loader pattern from Plan 89)
5. Write all SQL files in `processing/sql/`
6. Add `processing/events.py` — log-only stubs
7. Call `shared.minio.write_artifact_event()` in the processing service on every status transition (`processing`, `complete`, `retry`, `skip`); `pending` event is already written by the scraper
8. Implement `processing/writers/silver_writer.py` — `write_srp_silver()`, `write_detail_silver()`; same s3fs/pyarrow pattern as `archiver/processors/archive_artifacts.py`; skips silently if `MINIO_ENDPOINT` not set
8. Implement `processing/writers/srp_writer.py`
9. Implement `processing/writers/detail_writer.py`
10. Implement `processing/routers/batch.py` — wraps handler in `active_job()`
11. Implement `processing/routers/artifact.py` — `POST /process/artifact/{artifact_id}` for single-artifact reprocessing and Kafka readiness
12. Add `GET /ready` to `processing/app.py` using `shared/job_counter.is_idle()`
13. Wire routers into `processing/app.py`
14. Add `airflow/dags/results_processing.py`
15. Write Layer 3 integration tests (silver writes not asserted — no MinIO in test env; validated by Plan 96)
16. `docker compose build` + smoke test locally; confirm silver Parquet files appear in MinIO after first run

---

## What Changes in Other Services

**Scraper:** Nothing — Plan 97 already updated the scraper to write to MinIO and insert `artifacts_queue`. Parsers remain in scraper until Plan 71 Phase 7.

**Ops service:** Nothing new. `detail_scrape_claims` release SQL runs inside the processing service directly.

**dbt:** Nothing yet. Shadow period runs both n8n and Airflow paths. dbt decommission is Plan 90.

**n8n:** Results Processing workflow stays active and unmodified through the shadow period.

---

## Dropped from Previous Plan 93 Design

- Local disk reads — replaced by MinIO reads via `artifacts_queue.minio_path`
- `artifact_processing` table — replaced by `artifacts_queue`
- `srp_observations` / `detail_observations` Postgres writes — replaced by MinIO silver
- `detail_carousel_hints` Postgres table — carousel goes to MinIO silver + `price_observations`
- Plan 89 table set (`p89_*.sql` files, `listing_to_vin`, `vin_state`) — superseded by `vin_to_listing` + `price_observations`
