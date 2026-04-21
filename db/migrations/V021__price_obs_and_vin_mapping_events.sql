-- V021: Add staging event tables for price_observations and vin_to_listing.
--
-- Follows the universal HOT/staging pattern: every ops.* table gets a
-- corresponding staging.*_events table for append-only audit trail.
--
-- These tables are the source-of-truth for:
--   - Price change history (enables silver replay without re-parsing)
--   - VIN mapping history (tracks relisting, collision resolution)
--
-- A separate DAG bulk-exports from these staging tables to MinIO Parquet.

-- ---------------------------------------------------------------------------
-- 1. staging.price_observation_events
--    One row per mutation (upsert or delete) to ops.price_observations.
-- ---------------------------------------------------------------------------

CREATE TABLE staging.price_observation_events (
    event_id          bigserial    PRIMARY KEY,
    listing_id        uuid         NOT NULL,
    vin               text,
    price             integer,
    make              text,
    model             text,
    artifact_id       bigint       NOT NULL,
    event_type        text         NOT NULL  CHECK (event_type IN ('upserted', 'deleted')),
    source            text         NOT NULL  CHECK (source IN ('srp', 'detail', 'carousel')),
    event_at          timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX price_observation_events_listing_id_idx
    ON staging.price_observation_events (listing_id);

CREATE INDEX price_observation_events_event_at_idx
    ON staging.price_observation_events (event_at);

-- ---------------------------------------------------------------------------
-- 2. staging.vin_to_listing_events
--    One row per mutation to ops.vin_to_listing.
-- ---------------------------------------------------------------------------

CREATE TABLE staging.vin_to_listing_events (
    event_id          bigserial    PRIMARY KEY,
    vin               text         NOT NULL,
    listing_id        uuid         NOT NULL,
    artifact_id       bigint       NOT NULL,
    event_type        text         NOT NULL  CHECK (event_type IN ('mapped', 'remapped')),
    previous_listing_id uuid,
    event_at          timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX vin_to_listing_events_vin_idx
    ON staging.vin_to_listing_events (vin);

CREATE INDEX vin_to_listing_events_event_at_idx
    ON staging.vin_to_listing_events (event_at);

-- ---------------------------------------------------------------------------
-- 3. Grant viewer SELECT on the new tables
-- ---------------------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA staging TO viewer;
