-- V020: Fix column types in staging.detail_scrape_claim_events + viewer search_path.
--
-- Two issues introduced in V018:
--   1. staging.detail_scrape_claim_events.run_id typed uuid, but
--      artifacts_queue.run_id is text — cast fails at INSERT time.
--   2. staging.detail_scrape_claim_events.vin typed uuid, but VINs are
--      17-char alphanumeric strings (same bug as ops.price_observations.vin,
--      fixed in V019).
--   Table was created in V018 and has never been written to.
--
--   3. viewer role has no search_path set, so unqualified references to ops
--      tables (e.g. detail_scrape_claims) resolve against the default public
--      schema only and raise "relation does not exist". cartracker role got
--      its search_path in V017; viewer was missed.

-- ---------------------------------------------------------------------------
-- 1 & 2. Fix staging.detail_scrape_claim_events column types
-- ---------------------------------------------------------------------------

ALTER TABLE staging.detail_scrape_claim_events
    ALTER COLUMN run_id TYPE text,
    ALTER COLUMN vin    TYPE text;

-- ---------------------------------------------------------------------------
-- 3. Set search_path for viewer to match cartracker role (set in V017)
-- ---------------------------------------------------------------------------

ALTER ROLE viewer SET search_path TO ops, staging, public;
