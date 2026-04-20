-- V018: HOT tables + schema cleanup
--
-- 1. Drop dead Plan 89 tables (V014) — never written to in production
-- 2. Create ops.price_observations — HOT table, current inventory
-- 3. Create ops.vin_to_listing — HOT table, authoritative VIN→listing mapping
-- 4. Create staging.detail_scrape_claim_events — event log for claim lifecycle
-- 5. Create ops.blocked_cooldown — HOT table migrated from public
--      - listing_id: text → uuid
--      - first_attempt_at renamed to first_attempted_at for consistency
--      - existing rows copied; public table dropped
-- 6. Create staging.blocked_cooldown_events — event log for 403 transitions
-- 7. Migrate detail_scrape_claims: listing_id text→uuid, move to ops schema
--
-- Pre-deploy checklist:
--   - Update n8n Job Poller V2: blocked_cooldown ref → ops.blocked_cooldown
--   - Confirm stg_blocked_cooldown dbt source updated from public → ops
--   - Deploy during low-traffic window (ALTER TABLE takes ACCESS EXCLUSIVE lock)

-- ---------------------------------------------------------------------------
-- 1. Drop dead Plan 89 tables (V014)
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS public.listing_to_vin;
DROP TABLE IF EXISTS public.vin_state;
DROP TABLE IF EXISTS public.price_observations;  -- append-only V014 version; superseded by HOT table below

-- ---------------------------------------------------------------------------
-- 2. ops.price_observations — HOT table, current live inventory
--    One row per active listing. Presence = active; DELETE = unlisted.
-- ---------------------------------------------------------------------------

CREATE TABLE ops.price_observations (
    listing_id        uuid         PRIMARY KEY,
    vin               uuid,
    price             integer,
    make              text,
    model             text,
    last_seen_at      timestamptz  NOT NULL,
    last_artifact_id  bigint       NOT NULL REFERENCES ops.artifacts_queue(artifact_id)
);

-- Unique constraint on VIN (where known): one row per VIN when VIN is confirmed
CREATE UNIQUE INDEX ON ops.price_observations (vin) WHERE vin IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. ops.vin_to_listing — HOT table, authoritative VIN → listing mapping
-- ---------------------------------------------------------------------------

CREATE TABLE ops.vin_to_listing (
    vin          uuid         PRIMARY KEY,
    listing_id   uuid         NOT NULL,
    mapped_at    timestamptz  NOT NULL,
    artifact_id  bigint       NOT NULL REFERENCES ops.artifacts_queue(artifact_id)
);

CREATE INDEX ON ops.vin_to_listing (listing_id);

-- ---------------------------------------------------------------------------
-- 4. staging.detail_scrape_claim_events — event log for claim lifecycle
-- ---------------------------------------------------------------------------

CREATE TABLE staging.detail_scrape_claim_events (
    event_id     bigserial    PRIMARY KEY,
    listing_id   uuid         NOT NULL,
    run_id       uuid,
    status       text         NOT NULL  CHECK (status IN ('claimed', 'processed', 'released', 'expired')),
    stale_reason text,
    vin          uuid,
    event_at     timestamptz  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 5. ops.blocked_cooldown — migrated from public.blocked_cooldown
--    Column renames: first_attempt_at → first_attempted_at
--    Column retype:  listing_id text → uuid
-- ---------------------------------------------------------------------------

CREATE TABLE ops.blocked_cooldown (
    listing_id           uuid        PRIMARY KEY,
    first_attempted_at   timestamptz NOT NULL DEFAULT now(),
    last_attempted_at    timestamptz NOT NULL DEFAULT now(),
    num_of_attempts      integer     NOT NULL DEFAULT 1
);

-- Copy existing rows (will be empty on first deploy to a fresh env; populated in production)
INSERT INTO ops.blocked_cooldown (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
    SELECT listing_id::uuid, first_attempt_at, last_attempted_at, num_of_attempts
    FROM public.blocked_cooldown;

DROP TABLE public.blocked_cooldown;

-- ---------------------------------------------------------------------------
-- 6. staging.blocked_cooldown_events — event log for 403 transitions
-- ---------------------------------------------------------------------------

CREATE TABLE staging.blocked_cooldown_events (
    event_id          bigserial    PRIMARY KEY,
    listing_id        uuid         NOT NULL,
    event_type        text         NOT NULL  CHECK (event_type IN ('blocked', 'incremented', 'cleared')),
    num_of_attempts   integer      NOT NULL,
    event_at          timestamptz  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 7. Migrate detail_scrape_claims: listing_id text → uuid, move to ops schema
--    The claimed_by column stays as text (run_id stored as text string).
--    search_path (ops, staging, public) means unqualified SQL refs still resolve.
-- ---------------------------------------------------------------------------

ALTER TABLE public.detail_scrape_claims
    ALTER COLUMN listing_id TYPE uuid USING listing_id::uuid;

ALTER TABLE public.detail_scrape_claims SET SCHEMA ops;
