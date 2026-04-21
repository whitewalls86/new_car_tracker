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
--
--    analytics.stg_blocked_cooldown and ops.ops_detail_scrape_queue are views
--    that depend on public.blocked_cooldown. Drop them first, copy data, drop
--    the source table, then recreate both views pointing at ops.blocked_cooldown.
-- ---------------------------------------------------------------------------

-- Drop dependent views in dependency order (reverse of creation order)
DROP VIEW ops.ops_detail_scrape_queue;
DROP VIEW analytics.stg_blocked_cooldown;

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

-- Recreate analytics.stg_blocked_cooldown reading from ops.blocked_cooldown.
-- listing_id cast to text for downstream join compatibility (other tables store
-- listing_id as text). first_attempted_at aliased as first_attempt_at so
-- existing dashboard and n8n queries that reference the old column name still work.
CREATE VIEW analytics.stg_blocked_cooldown AS
    SELECT listing_id::text,
           first_attempted_at AS first_attempt_at,
           last_attempted_at,
           num_of_attempts,
           CASE WHEN num_of_attempts >= 5 THEN NULL::timestamptz
                ELSE last_attempted_at + (interval '1 hour' * (12 * power(2, num_of_attempts::float - 1)))
           END AS next_eligible_at,
           num_of_attempts >= 5 AS fully_blocked
    FROM ops.blocked_cooldown;

ALTER VIEW analytics.stg_blocked_cooldown OWNER TO cartracker;

-- Recreate ops.ops_detail_scrape_queue (unchanged — depends on stg_blocked_cooldown)
CREATE VIEW ops.ops_detail_scrape_queue AS
 WITH stale AS (
         SELECT ovs.vin,
            ovs.current_listing_url,
            ovs.listing_id,
            COALESCE(ovs.tier1_seller_customer_id, ovs.customer_id) AS seller_customer_id,
            ovs.is_price_stale,
            ovs.is_full_details_stale,
            ovs.stale_reason,
            ovs.tier1_age_hours,
            ovs.price_age_hours,
            row_number() OVER (PARTITION BY COALESCE(ovs.tier1_seller_customer_id, ovs.vin) ORDER BY
                CASE
                    WHEN ovs.is_full_details_stale THEN 0
                    ELSE 1
                END, COALESCE(ovs.price_observed_at, '1970-01-01 00:00:00+00'::timestamptz), COALESCE(ovs.tier1_observed_at, '1970-01-01 00:00:00+00'::timestamptz)) AS dealer_row_num
           FROM ops.ops_vehicle_staleness ovs
          WHERE ((ovs.is_price_stale OR ovs.is_full_details_stale) AND (COALESCE(ovs.listing_state, 'active'::text) = 'active'::text) AND (ovs.current_listing_url IS NOT NULL))
        ), dealer_picks AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            stale.stale_reason,
            1 AS priority
           FROM stale
          WHERE (stale.dealer_row_num = 1)
        ), force_stale AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            'force_stale_36h'::text AS stale_reason,
            2 AS priority
           FROM stale
          WHERE ((stale.price_age_hours > (36)::numeric) AND (stale.dealer_row_num > 1))
        ), carousel AS (
         SELECT sub.listing_id AS vin,
            (('https://www.cars.com/vehicledetail/'::text || sub.listing_id) || '/'::text) AS current_listing_url,
            sub.listing_id,
            NULL::text AS seller_customer_id,
            'unmapped_carousel'::text AS stale_reason,
            3 AS priority
           FROM ( SELECT int_carousel_price_events_unmapped.listing_id,
                    row_number() OVER (PARTITION BY int_carousel_price_events_unmapped.listing_id ORDER BY int_carousel_price_events_unmapped.observed_at DESC) AS rn
                   FROM analytics.int_carousel_price_events_unmapped) sub
          WHERE (sub.rn = 1)
        ), capacity_fill AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            concat(stale.stale_reason, '-extra') AS stale_reason,
            4 AS priority
           FROM stale
          WHERE (stale.dealer_row_num > 1)
        ), combined AS (
         SELECT dealer_picks.vin, dealer_picks.current_listing_url, dealer_picks.listing_id,
                dealer_picks.seller_customer_id, dealer_picks.stale_reason, dealer_picks.priority
           FROM dealer_picks
        UNION ALL
         SELECT force_stale.vin, force_stale.current_listing_url, force_stale.listing_id,
                force_stale.seller_customer_id, force_stale.stale_reason, force_stale.priority
           FROM force_stale
        UNION ALL
         SELECT carousel.vin, carousel.current_listing_url, carousel.listing_id,
                carousel.seller_customer_id, carousel.stale_reason, carousel.priority
           FROM carousel
        UNION ALL
         SELECT capacity_fill.vin, capacity_fill.current_listing_url, capacity_fill.listing_id,
                capacity_fill.seller_customer_id, capacity_fill.stale_reason, capacity_fill.priority
           FROM capacity_fill
        )
 SELECT DISTINCT ON (c.listing_id) c.vin,
    c.current_listing_url,
    c.listing_id,
    c.seller_customer_id,
    c.stale_reason,
    c.priority
   FROM (combined c
     LEFT JOIN analytics.stg_blocked_cooldown bc ON ((bc.listing_id = c.listing_id)))
  WHERE ((bc.listing_id IS NULL) OR ((bc.fully_blocked = false) AND (bc.next_eligible_at < now())))
  ORDER BY c.listing_id, c.priority;

ALTER VIEW ops.ops_detail_scrape_queue OWNER TO cartracker;

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
