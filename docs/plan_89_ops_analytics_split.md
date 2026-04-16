# Plan 89: Operational/Analytics dbt Split

**Status:** Planned
**Priority:** High — prerequisite for clean Kafka/event-driven architecture; scope partially overlaps Plan 71

---

## Overview

The current dbt DAG does two fundamentally different jobs:

1. **Operational state** — "what VIN does this listing map to?", "what's the current price?", "is this vehicle stale?" These need to be current within seconds of a scrape completing. Right now they're materialized tables that are only as fresh as the last dbt run.

2. **Analytics** — deal scores, price history, market benchmarks, days-on-market. These can tolerate batch lag. A dashboard that's 2 hours behind is fine.

Because both jobs share the dbt DAG, operational freshness requirements force dbt to run after every batch, and a slow or locked dbt run blocks the scrape queue. The fix is to pull operational state out of dbt entirely and give it a direct write path owned by the services that produce it.

After this split:
- dbt failure is an analytics problem (dashboard data goes stale), not an operational problem (pipeline keeps running)
- The scrape queue is always current — it reads application-owned tables, not dbt materialized tables
- Kafka/event-driven architecture becomes viable without per-event dbt rebuilds

---

## What Moves Out of dbt

Three application-owned tables replace the operational dbt intermediate models:

### `listing_to_vin`
**Replaces:** `int_listing_to_vin`

The authoritative mapping from `listing_id` → `vin`. Written by the processing service on every SRP or detail observation that carries a valid VIN. The upsert strategy is recency-only — most recent observation wins, regardless of source.

```sql
INSERT INTO listing_to_vin (listing_id, vin, vin_observed_at, vin_artifact_id)
VALUES (%s, %s, %s, %s)
ON CONFLICT (listing_id) DO UPDATE
  SET vin              = EXCLUDED.vin,
      vin_observed_at  = EXCLUDED.vin_observed_at,
      vin_artifact_id  = EXCLUDED.vin_artifact_id
WHERE EXCLUDED.vin_observed_at > listing_to_vin.vin_observed_at;
```

Source priority (detail > SRP) is not enforced here — it is enforced in `vin_state` via the COALESCE upsert strategy described below. `listing_to_vin` is purely a location index: "the most recent observation that tied this listing_id to a VIN."

**VIN discovery triggers a backfill.** When a new `listing_to_vin` entry is written (i.e., a listing_id is resolved for the first time), the processing service also runs:

```sql
UPDATE price_observations
SET vin = %s
WHERE listing_id = %s AND vin IS NULL
```

This ensures carousel price observations recorded before VIN discovery are retroactively attributed to the correct VIN. The backfill runs in the same transaction as the `listing_to_vin` insert.

---

### `price_observations`
**Replaces:** `int_price_events`, `int_carousel_price_events_mapped`, `int_carousel_price_events_unmapped`, `int_latest_price_by_vin`

An append-only log of every price signal seen, from any source. This is the "live log table" concept — not a materialized aggregate, just a record of every time a price was observed.

```sql
CREATE TABLE price_observations (
    id              bigserial PRIMARY KEY,
    listing_id      text        NOT NULL,
    vin             text,           -- nullable: carousel hints may not be mapped yet
    price           integer     NOT NULL,
    observed_at     timestamptz NOT NULL,
    artifact_id     bigint      NOT NULL,
    source          text        NOT NULL  -- 'srp' | 'detail' | 'carousel'
);
CREATE INDEX ON price_observations (listing_id, observed_at DESC);
CREATE INDEX ON price_observations (vin, observed_at DESC) WHERE vin IS NOT NULL;
```

**Written by the processing service:**
- SRP artifact processed → one row per vehicle on the page (`source='srp'`)
- Detail artifact processed → one row (`source='detail'`)
- Carousel hints → one row per hint that passes the write-time sanity filter (`source='carousel'`), `vin` populated via batch lookup if already known

**Carousel VIN resolution at write time.** Before writing carousel hints, the processing service batch-queries `listing_to_vin` for all listing_ids in the artifact:

```sql
SELECT listing_id, vin FROM listing_to_vin WHERE listing_id = ANY(%s)
```

Hints whose listing_id is already mapped get `vin` populated. Hints with no mapping write `vin = NULL`. These NULL rows are retroactively filled when VIN discovery happens (see `listing_to_vin` backfill above).

**Write-time filter for carousel hints (sanity only):** The processing service drops hints before writing if `price IS NULL`, `price <= 0`, `body IS NULL`, or `listing_id IS NULL`. Make/model validation is **not** done at write time — it stays at read time in the Pool 3 ops query. This preserves the ability to retroactively discover hints for make/model targets added after the fact, and avoids hard-coding parsing logic as a gate on what gets stored.

**"Latest price" is now a query, not a table:**
```sql
SELECT DISTINCT ON (vin) vin, price, observed_at, source
FROM price_observations
WHERE vin = %s
ORDER BY vin, observed_at DESC, artifact_id DESC;
```

dbt's analytics models that need price history (`int_price_history_by_vin`, `mart_deal_scores`) read from `price_observations` directly — it's a raw table, dbt can ref it as a source.

---

### `vin_state`
**Replaces:** `int_latest_tier1_observation_by_vin`, and the operational parts of `mart_vehicle_snapshot`

The authoritative current state per VIN — current listing location, listing state, mileage, dealer IDs. Both SRP and detail observations write here, but they carry different fields.

```sql
CREATE TABLE vin_state (
    vin                    text PRIMARY KEY,
    listing_id             text,
    listing_state          text,       -- only populated by detail observations
    mileage                integer,    -- only populated by detail observations
    canonical_detail_url   text,
    seller_customer_id     text,       -- UUID format, from SRP
    customer_id            text,       -- numeric string, from detail page
    state_observed_at      timestamptz NOT NULL,
    state_artifact_id      bigint NOT NULL
);
```

**Two write paths, same upsert:**

The upsert uses `COALESCE` so that detail-only fields are never overwritten by NULL values from an SRP write. Location fields (`listing_id`, `canonical_detail_url`) are always updated if the incoming observation is newer — this ensures that when a vehicle moves from listing AAA to BBB (detected via SRP before a detail scrape), the ops queue routes the next scrape to BBB, not the stale AAA URL.

```sql
INSERT INTO vin_state (
    vin, listing_id, canonical_detail_url, seller_customer_id,
    listing_state, customer_id, mileage,
    state_observed_at, state_artifact_id
)
VALUES (%s, %s, %s, %s,  %s, %s, %s,  %s, %s)
ON CONFLICT (vin) DO UPDATE SET
    -- Location fields: always update if newer, regardless of source
    listing_id           = CASE WHEN EXCLUDED.state_observed_at > vin_state.state_observed_at
                                THEN EXCLUDED.listing_id           ELSE vin_state.listing_id END,
    canonical_detail_url = CASE WHEN EXCLUDED.state_observed_at > vin_state.state_observed_at
                                THEN EXCLUDED.canonical_detail_url ELSE vin_state.canonical_detail_url END,
    seller_customer_id   = CASE WHEN EXCLUDED.state_observed_at > vin_state.state_observed_at
                                THEN EXCLUDED.seller_customer_id   ELSE vin_state.seller_customer_id END,
    state_observed_at    = GREATEST(EXCLUDED.state_observed_at, vin_state.state_observed_at),
    state_artifact_id    = CASE WHEN EXCLUDED.state_observed_at > vin_state.state_observed_at
                                THEN EXCLUDED.state_artifact_id    ELSE vin_state.state_artifact_id END,
    -- Detail-only fields: COALESCE so SRP never overwrites real data with NULL
    listing_state        = COALESCE(EXCLUDED.listing_state, vin_state.listing_state),
    customer_id          = COALESCE(EXCLUDED.customer_id,   vin_state.customer_id),
    mileage              = COALESCE(EXCLUDED.mileage,        vin_state.mileage)
```

**SRP writes** pass `NULL` for `listing_state`, `customer_id`, and `mileage`. They update the location fields if newer.

**Detail writes** pass real values for all fields. They update everything if newer, and `listing_state` / `customer_id` / `mileage` propagate forward via COALESCE even when an older SRP observation comes in later.

---

## What the Ops Views Look Like After

`ops_vehicle_staleness` and `ops_detail_scrape_queue` become plain Postgres views — no dbt, always current.

`ops_vehicle_staleness` staleness logic is identical to today; it just reads `vin_state` and `price_observations` instead of dbt materialized tables.

`ops_detail_scrape_queue` manages two fundamentally different populations that are handled separately:

**Pool 1/2 — stale known VINs:** Reads from `vin_state` (for listing location and dealer state) and `price_observations` (for price freshness). These are VIN-keyed. The scrape URL comes from `vin_state.canonical_detail_url`, which is always the most recent location seen for that VIN across both SRP and detail observations — so if a vehicle moves from listing AAA to BBB before a detail scrape, the queue routes to BBB.

**Pool 3 — undiscovered carousel listings:** Queries `detail_carousel_hints` directly. These are listing_ids we have seen on a carousel but have never detail-scraped. They have no VIN, no entry in `vin_state`, and may have `NULL` vin rows in `price_observations`. The Pool 3 query excludes already-resolved listings via a LEFT JOIN against `listing_to_vin`, and filters to valid make/model targets by joining against `search_configs` — the make/model validation that currently lives in `int_carousel_hints_filtered`:

```sql
SELECT DISTINCT ON (h.listing_id)
    h.listing_id,
    h.price,
    h.fetched_at
FROM detail_carousel_hints h
JOIN scrape_targets st
    ON lower(parsed_make(h.body)) = lower(st.make)
    AND lower(parsed_model(h.body)) = lower(st.model)
LEFT JOIN listing_to_vin ltv ON ltv.listing_id = h.listing_id
WHERE ltv.listing_id IS NULL   -- not yet resolved to a VIN
  AND h.price IS NOT NULL
ORDER BY h.listing_id, h.fetched_at DESC
```

Once a listing_id is resolved (detail scrape completes, `listing_to_vin` entry written), it drops out of Pool 3 automatically on the next queue read. No cleanup needed.

**`detail_carousel_hints` is a record, not a queue.** Resolution does not delete rows. The table is append-only with a unique constraint on `(artifact_id, listing_id)`. This preserves the historical price signal needed for `price_observations` backfill and supports re-processing.

These views are created in a Flyway migration and live in the `ops` schema exactly as they do today. The `ops/` dbt models are deleted.

---

## What Stays in dbt

dbt becomes purely an analytics layer. It reads from raw observation tables and the three application-owned tables above.

| Model | Stays / Changes |
|---|---|
| All `stg_*` | Unchanged — pass-throughs are still dbt's entry point |
| `int_vehicle_attributes` | Unchanged — static VIN identity (make/model/trim/year), batch lag fine |
| `int_listing_days_on_market` | Unchanged — historical aggregate |
| `int_model_price_benchmarks` | Unchanged — market stats |
| `int_price_history_by_vin` | **Updated:** reads from `price_observations` source instead of `int_price_events` |
| `int_price_percentiles_by_vin` | Unchanged |
| `int_dealer_inventory` | Unchanged |
| `mart_vehicle_snapshot` | **Updated:** joins `vin_state` and `price_observations` directly instead of dbt intermediates |
| `mart_deal_scores` | Minor updates to reflect new source joins |
| `int_listing_to_vin` | **Deleted** — replaced by application table |
| `int_price_events` | **Deleted** — replaced by `price_observations` |
| `int_latest_price_by_vin` | **Deleted** — replaced by query against `price_observations` |
| `int_latest_tier1_observation_by_vin` | **Deleted** — replaced by `vin_state` |
| `int_carousel_hints_filtered` | **Deleted** — make/model validation moves to Pool 3 ops view query; sanity filtering moves to write time in processing service |
| `int_carousel_price_events_mapped` | **Deleted** |
| `int_carousel_price_events_unmapped` | **Deleted** |
| `ops_vehicle_staleness` | **Deleted** — becomes live Postgres view |
| `ops_detail_scrape_queue` | **Deleted** — becomes live Postgres view |

dbt's schedule can now be driven purely by analytics needs — every few hours is fine.

---

## Plan 71 Integration

This plan's implementation is tightly coupled to Plan 71 (Airflow + processing service). The processing service being built in Plan 71 is the natural owner of the write path for all three application tables.

**What gets added to Plan 71 scope:**
- Flyway migration: create `listing_to_vin`, `price_observations`, `vin_state`
- Processing service writes to all three tables as part of artifact processing
- Flyway migration: rewrite `ops_vehicle_staleness` and `ops_detail_scrape_queue` as live Postgres views
- Shadow validation period: both dbt-derived and app-owned tables populated simultaneously; assert they agree

**What is deferred to a follow-on cleanup plan (Plan 90):**
- Delete the now-redundant dbt intermediate models
- Update `mart_vehicle_snapshot` and `mart_deal_scores` to read from app tables
- Remove `dbt_intents` entries that referenced deleted models
- Run a full dbt test suite to verify the analytics layer is intact after removals

The shadow period is the safety gate. Don't delete any dbt models until `listing_to_vin` and `price_observations` have been live for at least one full scrape cycle and their contents have been validated against the dbt-derived equivalents.

---

## Testability Strategy

Moving logic from dbt into Python services is only an improvement if the Python is tested. The testing layers map to the new architecture:

### Layer 1 — SQL smoke tests (existing, extend)
Add smoke tests for the three new tables:
- `listing_to_vin` is not empty
- `price_observations` has rows with `observed_at` within the last N hours
- `vin_state` covers all VINs that have had detail scrapes

These are simple `SELECT COUNT(*)` or `MAX(observed_at)` checks — same pattern as existing Layer 1 tests.

### Layer 2 — Processing service unit tests
The processing logic that writes to the three tables should be unit-testable in isolation. Key invariants to test:

- `listing_to_vin` upsert does not downgrade — an older observation does not overwrite a newer one
- `vin_state` location fields update when SRP sees a newer observation
- `vin_state` detail-only fields (`listing_state`, `customer_id`, `mileage`) are not overwritten by a subsequent SRP write
- Carousel batch VIN lookup: hints with known listing_ids get `vin` populated; unknown ones get `NULL`
- Carousel write-time sanity filter: null/zero price and null body are dropped before writing
- Make/model validity is not checked at write time (no filter on `search_configs` during carousel processing)
- VIN backfill runs when a new `listing_to_vin` entry is created for a previously unmapped listing

These tests don't need a database — they test the Python logic that constructs the upsert values. The SQL itself is fixed and tested at the integration layer.

### Layer 3 — Integration tests (extend existing pattern)
Extend the existing Layer 3 ops API tests to cover the write path:

```
Given: a known SRP artifact in the test DB
When:  POST /artifacts/process/{artifact_id}
Then:  listing_to_vin has an entry for each listing on that SRP page (where VIN present)
       price_observations has one row per vehicle per artifact
       vin_state updated for each VIN — location fields current, detail-only fields unchanged
```

```
Given: a known detail artifact in the test DB
When:  POST /artifacts/process/{artifact_id}
Then:  listing_to_vin entry is created or updated
       price_observations has one row (source='detail')
       vin_state has correct listing_state, customer_id, mileage
       any prior NULL-vin price_observations rows for that listing_id are backfilled
```

```
Given: a carousel artifact with hints for listing_id AAA (already in listing_to_vin → VIN001)
       and listing_id BBB (not yet in listing_to_vin)
When:  carousel hints are processed
Then:  price_observations has a row for AAA with vin='VIN001' (source='carousel')
       price_observations has a row for BBB with vin=NULL (source='carousel')
       detail_carousel_hints has both rows (sanity-filtered only, not make/model filtered)
```

These tests run against a real DB (same pattern as existing Layer 3 tests) and validate the full write path — SQL included.

### Ops views are testable without dbt
Because the ops views now query application tables, they can be tested directly:

```
Given: seed vin_state and price_observations with known stale rows
When:  SELECT * FROM ops.ops_vehicle_staleness
Then:  expected VINs are flagged as stale with correct stale_reason
```

This is a significant improvement over the current state, where testing the ops views required a full dbt build.

---

## SQL in Python — File Strategy

### The problem
Inline SQL in Python is hard to review, impossible to run directly against the DB for debugging, gets no syntax highlighting, and conflates query logic with Python control flow. As the processing service gains responsibility for the write paths described above, this will only get worse.

### The pattern: load at module import time

Each service maintains a `sql/` subdirectory. Queries are `.sql` files. A small loader reads them at import time — they become module-level string constants with no runtime file I/O.

```
processing/
  sql/
    upsert_listing_to_vin.sql
    insert_price_observation.sql
    upsert_vin_state.sql
    get_valid_scrape_targets.sql    # replaces int_scrape_targets join
ops/
  sql/
    claim_batch.sql
    release_claims.sql
    advance_rotation.sql
shared/
  sql/
    check_vin_state.sql
```

Loader (one per service, no shared utility needed):

```python
# processing/queries.py
from pathlib import Path

_SQL = Path(__file__).parent / "sql"

def _q(name: str) -> str:
    return (_SQL / f"{name}.sql").read_text()

UPSERT_LISTING_TO_VIN    = _q("upsert_listing_to_vin")
INSERT_PRICE_OBSERVATION = _q("insert_price_observation")
UPSERT_VIN_STATE         = _q("upsert_vin_state")
GET_VALID_SCRAPE_TARGETS = _q("get_valid_scrape_targets")
```

Usage:

```python
from processing.queries import UPSERT_LISTING_TO_VIN

async def write_listing_to_vin(conn, listing_id, vin, observed_at, artifact_id):
    await conn.execute(UPSERT_LISTING_TO_VIN, (listing_id, vin, observed_at, artifact_id))
```

### Rules for what goes in a .sql file vs inline

**Put in a .sql file:**
- Any query longer than ~3 lines
- Any query that will be reused across more than one function
- Any INSERT/UPDATE/UPSERT that modifies the application tables described above
- The ops view definitions (managed in Flyway migrations, not Python)

**Keep inline:**
- Single-line lookups: `SELECT id FROM table WHERE key = %s`
- Dynamic queries where the SQL structure itself changes based on Python conditions (these should be rare and clearly commented)

### What this does for testability

Queries in `.sql` files can be:
- Run directly in psql against the dev or test DB without touching Python
- Reviewed in PRs with proper SQL syntax highlighting
- Diffed cleanly when logic changes
- Tested at the Layer 3 integration level by running the exact file used in production

The combination of `.sql` files + module-level loading means there's no magic, no ORM, no query builder — just SQL that happens to live in a file instead of a string literal.

### Existing inline SQL

Don't retroactively migrate all existing inline SQL. Apply the pattern to:
1. All new queries written for the processing service (Plan 71)
2. The existing ops service endpoints being added in Plan 71 (`claim_batch`, `release_claims`, `advance_rotation`)

Existing scraper inline SQL can be migrated opportunistically if those functions are touched anyway. Don't create a separate refactor plan for it — the pattern will propagate naturally as files are modified.

---

## Vehicle Lifecycle Through the New Tables

Two distinct populations flow through the system differently.

### Known VIN (seen on SRP or detail)

```
SRP observation (VIN001 at listing BBB, 4.20):
  → listing_to_vin:    UPSERT (BBB, VIN001, 4.20)
  → price_observations: INSERT (BBB, VIN001, 10000, 4.20, 'srp')
  → vin_state:         UPSERT — location fields updated if 4.20 > current state_observed_at;
                        listing_state/customer_id/mileage unchanged (COALESCE)

VIN001 goes stale → ops queue reads vin_state → scrapes vin_state.canonical_detail_url (BBB)
```

### Undiscovered carousel listing (no VIN yet)

```
Carousel hint seen (listing AAA, price 10000, 4.16):
  → detail_carousel_hints: INSERT (sanity filtered: price > 0, body not null)
  → listing_to_vin batch lookup: AAA not found
  → price_observations: INSERT (AAA, vin=NULL, 10000, 4.16, 'carousel')

Pool 3 ops query picks up AAA (LEFT JOIN listing_to_vin returns null, make/model valid):
  → detail scrape queued for AAA

Detail scrape completes (VIN001 discovered, 4.17):
  → listing_to_vin:    INSERT (AAA, VIN001, 4.17)  ← first time this listing_id is resolved
  → price_observations: UPDATE SET vin='VIN001' WHERE listing_id='AAA' AND vin IS NULL  ← backfill
  → price_observations: INSERT (AAA, VIN001, 10000, 4.17, 'detail')
  → vin_state:         INSERT (VIN001, AAA, active, ...)

AAA drops out of Pool 3 automatically (listing_to_vin now has it)
VIN001 enters Pool 1/2 staleness tracking via vin_state
```

---

## Rollout Order

1. **Flyway migration** — create `listing_to_vin`, `price_observations`, `vin_state` tables with appropriate indexes
2. **Processing service: write path** — implement the three table writes in Plan 71's processing service, with `.sql` file queries from day one
3. **Shadow period** — both dbt and app tables populated; run daily comparison: `listing_to_vin` vs `int_listing_to_vin`, `price_observations` vs `int_price_events`
4. **Layer 3 integration tests** — test the write path against the real test DB before cutover
5. **Flyway migration** — rewrite `ops_vehicle_staleness` and `ops_detail_scrape_queue` as live Postgres views reading app tables
6. **Validate ops queue** — run a full scrape cycle; confirm queue contents match the shadow period
7. **Plan 90 (cleanup)** — delete redundant dbt models, update marts, remove shadow comparison tests
