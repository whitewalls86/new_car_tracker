# Plan 99: Per-Source Price Observations

**Status:** COMPLETE (2026-04-27) — implemented with a simplified design; see note below
**Depends on:** Plan 93 (processing service, COMPLETE)
**Blocks:** V029 (plain Postgres ops views — COMPLETE)

> **Design note:** The per-source composite PK `(listing_id, source)` described below was not implemented. Instead, V028 added a `customer_id` column to `ops.price_observations`. `customer_id IS NULL` means the listing has never been detail-scraped (equivalent to "no detail source row"). V029 then rewrote the staleness view using this flag, eliminating the need to pivot by source. The original plan's goal — enabling the staleness view to distinguish detail vs SRP observations — was achieved with this simpler approach. The steps below are retained for reference.

---

## Overview

`ops.price_observations` currently stores one row per listing. Any source (SRP, detail page,
carousel) overwrites the same row, so `last_seen_at` reflects the most recent observation
regardless of how much data it carried. The table cannot answer "when did we last get *full*
detail information for this vehicle?"

This plan changes the primary key from `(listing_id)` to `(listing_id, source)`, giving one row
per `(listing, source)` pair. The V019 staleness view then pivots on source to compute separate
ages for detail and SRP observations — making `is_full_details_stale` correct and enabling the
scrape queue to prioritize the right listings.

The events side (`staging.price_observation_events`) already has a `source` column (V021) and
needs no schema change.

---

## Design Decisions

### Per-source rows, not a last-seen timestamp per source column

Adding separate `detail_last_seen_at` / `srp_last_seen_at` columns to the existing single-row
design would be fragile: each new source would require a schema migration. A composite PK on
`(listing_id, source)` follows the same HOT pattern used elsewhere and is queryable with a
standard GROUP BY pivot.

### Unique index becomes `(vin, source) WHERE vin IS NOT NULL`

Previously: `UNIQUE (vin) WHERE vin IS NOT NULL` — one row per VIN across all sources.
After: `UNIQUE (vin, source) WHERE vin IS NOT NULL` — one row per (VIN, source) pair.

This preserves the relisting collision check (a VIN appearing at a new listing_id) while
allowing the same VIN to have separate SRP and detail rows.

### VIN collision deletes all source rows for the old listing

When a VIN is confirmed at a new listing_id (relisting), the existing code deletes
`WHERE listing_id = old_listing_id` with no `source` filter. This is correct: the old listing
is gone regardless of which source had seen it. No change needed to the delete SQL.

### HOT table is rebuildable — no backfill required

The HOT table is current-state only. An empty table after migration is valid; it fills in as
scraping runs. `staging.silver_observations` has a `source` column on every row — a backfill
from the staging buffer is possible if operational continuity matters, but is not required for
correctness.

### Staleness thresholds (informational — owned by V019)

The per-source rows enable the following staleness signals in the V019 view:

| Signal | Condition |
|---|---|
| `is_price_stale` | No SRP or detail row, or `MAX(last_seen_at) FILTER (WHERE source IN ('srp', 'detail')) < now() - 24h` |
| `is_full_details_stale` | No detail row, or `last_seen_at FILTER (WHERE source = 'detail') < now() - 168h` |

Carousel rows do not count toward either staleness signal — carousel data is incomplete.

---

## Steps

### Step 1 — Migration V028: recreate `ops.price_observations`

Drop and recreate the HOT table with `(listing_id, source)` as composite PK.

```sql
-- V028: Per-source price observations
--
-- Changes ops.price_observations PK from (listing_id) to (listing_id, source).
-- One row per (listing, source) triple enables source-aware staleness in V019.
--
-- The HOT table is current-state only and rebuildable from silver — no data
-- migration needed. Drop and recreate is safe.

DROP TABLE ops.price_observations;

CREATE TABLE ops.price_observations (
    listing_id        uuid         NOT NULL,
    source            text         NOT NULL CHECK (source IN ('srp', 'detail', 'carousel')),
    vin               text,
    price             integer,
    make              text,
    model             text,
    last_seen_at      timestamptz  NOT NULL,
    last_artifact_id  bigint       NOT NULL REFERENCES ops.artifacts_queue(artifact_id),
    PRIMARY KEY (listing_id, source)
);

-- One (vin, source) pair maps to at most one listing at a time.
-- Relisting detection uses LOOKUP_VIN_COLLISION which filters source='detail' only.
CREATE UNIQUE INDEX ON ops.price_observations (vin, source) WHERE vin IS NOT NULL;

-- Efficient lookup by listing_id (for deletes and scrape queue joins)
CREATE INDEX ON ops.price_observations (listing_id);

-- Re-grant viewer access (table was dropped and recreated)
GRANT SELECT ON ops.price_observations TO viewer;
```

**Note:** `ops_detail_scrape_queue` (view) reads `ops.price_observations` indirectly via
`ops_vehicle_staleness`. Both are dbt models today. The migration drops the HOT table; the views
will error until V019 replaces them with plain Postgres views. Deploy V028 and V019 together or
accept a brief queue-view outage between them.

---

### Step 2 — Update `upsert_price_observation.sql`

Add `source` to the INSERT and change the conflict target.

```sql
-- Upsert a single price observation into the HOT table.
-- PK is (listing_id, source); vin has a partial unique index per source.
INSERT INTO ops.price_observations
    (listing_id, source, vin, price, make, model, last_seen_at, last_artifact_id)
VALUES
    (%(listing_id)s, %(source)s, %(vin)s, %(price)s, %(make)s, %(model)s,
     %(last_seen_at)s, %(last_artifact_id)s)
ON CONFLICT (listing_id, source) DO UPDATE SET
    vin              = COALESCE(EXCLUDED.vin, ops.price_observations.vin),
    price            = EXCLUDED.price,
    make             = EXCLUDED.make,
    model            = EXCLUDED.model,
    last_seen_at     = EXCLUDED.last_seen_at,
    last_artifact_id = EXCLUDED.last_artifact_id
```

---

### Step 3 — Update `lookup_vin_collision.sql`

The VIN collision check only applies to the `detail` source — SRP and carousel rows should not
trigger relisting detection. Add a `source` filter and `LIMIT 1` (multiple sources could match
otherwise, though with the new unique index they won't).

```sql
-- Check if a VIN already exists at a different listing_id for source='detail'.
-- Only detail observations drive relisting detection.
SELECT listing_id
FROM ops.price_observations
WHERE vin = %(vin)s
  AND source = 'detail'
  AND listing_id != %(listing_id)s
LIMIT 1
```

---

### Step 4 — Update `srp_writer.py`

Pass `source='srp'` to `UPSERT_PRICE_OBSERVATION`. Currently `source` is only passed to
`INSERT_PRICE_OBSERVATION_EVENT` (the events table); add it to the HOT table upsert.

```python
cur.execute(UPSERT_PRICE_OBSERVATION, {
    "listing_id": listing_id,
    "source": "srp",          # ← add
    "vin": vin,
    "price": listing.get("price"),
    "make": listing.get("make"),
    "model": listing.get("model"),
    "last_seen_at": fetched_at,
    "last_artifact_id": artifact_id,
})
```

---

### Step 5 — Update `detail_writer.py`

Two call sites in `write_detail_active`:

**Primary observation (Step 3 in the write path):**
```python
cur.execute(UPSERT_PRICE_OBSERVATION, {
    "listing_id": listing_id,
    "source": "detail",       # ← add
    "vin": vin,
    ...
})
```

**Carousel hints (Step 5 in the write path):**
```python
cur.execute(UPSERT_PRICE_OBSERVATION, {
    "listing_id": hint_listing_id,
    "source": "carousel",     # ← add
    "vin": hint_vin,
    ...
})
```

No changes needed to `write_detail_unlisted` or `write_detail_blocked` — those paths call
`DELETE_PRICE_OBSERVATION` (deletes all source rows by `listing_id`) and
`DELETE_PRICE_OBSERVATION_BY_VIN` (same), both of which are already correct.

---

### Step 6 — Layer 1 smoke test updates

`tests/integration/sql/test_processing_queries.py` seeds `ops.price_observations` rows. Update
any `INSERT` fixtures to include `source`, and update any queries that join or filter on the
table to reflect the new schema.

---

### Step 7 — Unit test updates

`tests/processing/test_srp_writer.py` and any detail writer unit tests that assert on
`UPSERT_PRICE_OBSERVATION` call arguments need `source` added to the expected parameter dict.

---

## Out of Scope

- `staging.price_observation_events` — already has `source`; no change.
- `ops.vin_to_listing` — not affected.
- `delete_price_observation.sql` — already `WHERE listing_id = X`; deletes all source rows correctly.
- `delete_price_observation_by_vin.sql` — same.
- V019 view content — V019 owns the staleness logic; this plan only ensures the HOT table
  shape is correct for V019 to read from.

---

## Deployment Order

1. Deploy V028 migration (drops + recreates `ops.price_observations`)
2. Deploy processing service with `source` wired to `UPSERT_PRICE_OBSERVATION`
3. Deploy V019 migration (rewrites `ops_vehicle_staleness` + `ops_detail_scrape_queue` as
   plain Postgres views reading the new per-source HOT table)

Steps 1 and 2 should be deployed together or in immediate succession — the processing service
will error on `UPSERT_PRICE_OBSERVATION` against the old schema (missing `source` column) or
vice versa if the migration runs without the code update.
