-- V019: Fix vin column type (uuid → text) and re-grant viewer on ops/staging.
--
-- Two bugs introduced in V018:
--   1. ops.price_observations.vin and ops.vin_to_listing.vin were typed uuid.
--      VINs are 17-char alphanumeric strings and cannot be stored in a uuid column.
--   2. V018 dropped and recreated ops.ops_detail_scrape_queue (view) and moved
--      detail_scrape_claims from public to ops. The point-in-time GRANTs from
--      V004/V008 do not cover objects created or moved after those migrations ran,
--      so the viewer role lost SELECT on those objects.
--
-- Both tables were created in V018 and have never been written to, so no data
-- migration is needed — the ALTER COLUMN is purely a type change on empty tables.

-- ---------------------------------------------------------------------------
-- 1. ops.price_observations.vin: uuid → text
--    V018 created this index with: CREATE UNIQUE INDEX ON ops.price_observations (vin)
--    Postgres auto-names it price_observations_vin_idx.
-- ---------------------------------------------------------------------------

DROP INDEX ops.price_observations_vin_idx;
ALTER TABLE ops.price_observations ALTER COLUMN vin TYPE text;
CREATE UNIQUE INDEX ON ops.price_observations (vin) WHERE vin IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 2. ops.vin_to_listing.vin: uuid PRIMARY KEY → text PRIMARY KEY
-- ---------------------------------------------------------------------------

ALTER TABLE ops.vin_to_listing DROP CONSTRAINT vin_to_listing_pkey;
ALTER TABLE ops.vin_to_listing ALTER COLUMN vin TYPE text;
ALTER TABLE ops.vin_to_listing ADD PRIMARY KEY (vin);

-- ---------------------------------------------------------------------------
-- 3. Re-grant viewer SELECT on all ops and staging objects.
--    Covers: detail_scrape_claims (moved from public), ops_detail_scrape_queue
--    (recreated), price_observations, vin_to_listing, blocked_cooldown,
--    artifacts_queue (V017), artifacts_queue_events (V017).
-- ---------------------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA ops TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO viewer;
