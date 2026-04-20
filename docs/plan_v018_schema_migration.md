# V018: Schema Migration

**Status:** Ready to implement — no blockers
**Migration file:** `db/migrations/V018__hot_tables_and_cleanup.sql`

---

## Overview

V018 is a pure schema migration. It:
- Drops the dead Plan 89 tables (created in V014, never written to)
- Creates the HOT + staging event tables for the three operational queues
- Fixes two UUID column type mismatches that survive into the new architecture
- Moves `blocked_cooldown` and `detail_scrape_claims` into the `ops` schema

No application code changes are required for V018 to ship safely. Nothing in production reads the tables being dropped, and the new tables are empty until Plan 93 writes to them.

---

## Tables Dropped

These were created by V014 (Plan 89). The processing service they were designed for was never built. They are superseded by the HOT table design below.

```sql
DROP TABLE IF EXISTS public.listing_to_vin;
DROP TABLE IF EXISTS public.vin_state;
DROP TABLE IF EXISTS public.price_observations;  -- the append-only V014 version
```

---

## Tables Created

### `ops.price_observations` — HOT table, current inventory

One row per active listing. Presence = active; DELETE = unlisted. Written by the processing service.

```sql
CREATE TABLE ops.price_observations (
    listing_id        uuid         PRIMARY KEY,
    vin               uuid,
    price             integer,
    make              text,
    model             text,
    last_seen_at      timestamptz  NOT NULL,
    last_artifact_id  bigint       NOT NULL REFERENCES ops.artifacts_queue(artifact_id)
);

CREATE UNIQUE INDEX ON ops.price_observations (vin) WHERE vin IS NOT NULL;
```

### `ops.vin_to_listing` — HOT table, authoritative VIN→listing mapping

Updated whenever a VIN is confirmed for a listing. Written by the processing service.

```sql
CREATE TABLE ops.vin_to_listing (
    vin          uuid         PRIMARY KEY,
    listing_id   uuid         NOT NULL,
    mapped_at    timestamptz  NOT NULL,
    artifact_id  bigint       NOT NULL REFERENCES ops.artifacts_queue(artifact_id)
);

CREATE INDEX ON ops.vin_to_listing (listing_id);
```

### `staging.detail_scrape_claim_events` — event log for claim lifecycle

One row per status transition on a detail scrape claim. Flushed to MinIO Parquet by the export DAG, then TRUNCATED.

```sql
CREATE TABLE staging.detail_scrape_claim_events (
    event_id     bigserial    PRIMARY KEY,
    listing_id   uuid         NOT NULL,
    run_id       uuid,
    status       text         NOT NULL,  -- 'claimed' | 'processed' | 'released' | 'expired'
    stale_reason text,
    vin          uuid,
    event_at     timestamptz  NOT NULL DEFAULT now()
);
```

### `ops.blocked_cooldown` — HOT table, 403 cooldown state (migrated from public)

Replaces `public.blocked_cooldown`. Same schema, ops schema, corrected UUID type.

```sql
CREATE TABLE ops.blocked_cooldown (
    listing_id           uuid        PRIMARY KEY,
    first_attempted_at   timestamptz NOT NULL DEFAULT now(),
    last_attempted_at    timestamptz NOT NULL DEFAULT now(),
    num_of_attempts      integer     NOT NULL DEFAULT 1
);
```

### `staging.blocked_cooldown_events` — event log for 403 transitions

One row per block/increment/clear event. Flushed to MinIO Parquet, then TRUNCATED.

```sql
CREATE TABLE staging.blocked_cooldown_events (
    event_id          bigserial    PRIMARY KEY,
    listing_id        uuid         NOT NULL,
    event_type        text         NOT NULL,  -- 'blocked' | 'incremented' | 'cleared'
    num_of_attempts   integer      NOT NULL,
    event_at          timestamptz  NOT NULL DEFAULT now()
);
```

---

## Tables Migrated

`detail_scrape_claims` and `blocked_cooldown` move from `public` to `ops` schema, and their `listing_id` columns are retyped from `text` to `uuid`.

```sql
-- Migrate detail_scrape_claims
ALTER TABLE public.detail_scrape_claims
    ALTER COLUMN listing_id TYPE uuid USING listing_id::uuid;
ALTER TABLE public.detail_scrape_claims SET SCHEMA ops;

-- Migrate blocked_cooldown (data preserved)
INSERT INTO ops.blocked_cooldown
    SELECT listing_id::uuid, first_attempted_at, last_attempted_at, num_of_attempts
    FROM public.blocked_cooldown;
DROP TABLE public.blocked_cooldown;
```

---

## search_path

The `cartracker` role already has `search_path = ops, staging, public` set in V017. No change needed — new tables in `ops` and `staging` are immediately visible.

---

## Deployment Safety

| Risk | Assessment |
|---|---|
| Drop `listing_to_vin`, `vin_state`, `price_observations` (V014) | Zero risk — confirmed never written to |
| Create new HOT/staging tables | Additive — no existing code touches them |
| `ALTER TABLE detail_scrape_claims SET SCHEMA ops` | ops service and n8n both reference this table; any hardcoded `public.detail_scrape_claims` references must be updated before deploying |
| `blocked_cooldown` migration | n8n Job Poller V2 writes to `public.blocked_cooldown`; update to `ops.blocked_cooldown` before deploying |

### Pre-deploy checklist

- [ ] Search all services and n8n workflows for `public.detail_scrape_claims` → update to `ops.detail_scrape_claims`
- [ ] Search n8n Job Poller V2 for `blocked_cooldown` table reference → update to `ops.blocked_cooldown`
- [ ] Confirm `stg_blocked_cooldown` dbt source reference — update source schema from `public` to `ops`
- [ ] Run migration in staging, confirm CI passes
- [ ] Deploy during low-traffic window (schema moves take brief ACCESS EXCLUSIVE locks)
