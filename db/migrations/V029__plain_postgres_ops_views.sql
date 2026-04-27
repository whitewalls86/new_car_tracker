-- V029: Rewrite ops_vehicle_staleness and ops_detail_scrape_queue as plain
--       Postgres views reading directly from HOT tables.
--
-- Previously both views were dbt models that read through mart_vehicle_snapshot
-- and the full dbt DAG. This migration cuts that dependency:
--
--   ops_vehicle_staleness  → reads ops.price_observations + ops.blocked_cooldown
--   ops_detail_scrape_queue → reads ops_vehicle_staleness + ops.blocked_cooldown
--                             (blocked_cooldown formula inlined; no analytics.* refs)
--
-- Staleness model simplification (from Plan 99 design decision):
--
--   Old model: separate tier1_age (detail) and price_age (SRP) thresholds,
--              driven by mart_vehicle_snapshot which aggregated source-level obs.
--
--   New model: single last_seen_at (any source confirms listing is active + price
--              is current). Price doesn't need source discrimination because:
--              - Make/model/msrp/dealer are static for the life of a listing
--              - Only price changes, and any source (SRP, carousel, detail) carries it
--              - last_seen_at > 24h = needs re-scrape regardless of which source last saw it
--
--   Retained: dealer_unenriched = customer_id IS NULL (never been detail-scraped;
--             static dealer data missing). Triggers a detail scrape regardless of age.
--
--   Removed:  full_details stale_reason (7-day detail re-scrape). Static data doesn't
--             change; the 24h price_only threshold handles all re-scrape needs once
--             a listing is enriched.
--
-- Queue simplification:
--
--   Old carousel pool (priority 3): read from analytics.int_carousel_price_events_unmapped
--   New: carousel hints that passed the search_config filter are already in
--        ops.price_observations with customer_id IS NULL, so they surface as
--        dealer_unenriched in pool 1 naturally. No separate carousel pool needed.
--
--   Dealer partition: COALESCE(customer_id, vin::text)
--   Blocked cooldown: inlined exponential backoff formula (no analytics.* reference)

-- ---------------------------------------------------------------------------
-- 1. ops.ops_vehicle_staleness
-- ---------------------------------------------------------------------------

-- Drop in dependency order: queue depends on staleness
DROP VIEW IF EXISTS ops.ops_detail_scrape_queue;
DROP VIEW IF EXISTS ops.ops_vehicle_staleness;

CREATE VIEW ops.ops_vehicle_staleness AS
SELECT
    po.listing_id,
    po.vin,
    po.price,
    po.make,
    po.model,
    po.customer_id,
    po.last_seen_at,
    po.last_artifact_id,

    -- Constructed listing URL (Cars.com canonical form)
    'https://www.cars.com/vehicledetail/' || po.listing_id || '/' AS current_listing_url,

    -- Age in hours from last confirmation by any source
    extract(epoch from (now() - po.last_seen_at)) / 3600.0 AS age_hours,

    -- Staleness flags
    po.customer_id IS NULL AS is_full_details_stale,
    po.last_seen_at < now() - interval '24 hours' AS is_price_stale,

    -- Unified stale_reason (dealer_unenriched takes priority)
    CASE
        WHEN po.customer_id IS NULL            THEN 'dealer_unenriched'
        WHEN po.last_seen_at < now() - interval '24 hours' THEN 'price_only'
        ELSE 'not_stale'
    END AS stale_reason

FROM ops.price_observations po;

ALTER VIEW ops.ops_vehicle_staleness OWNER TO dbt_user;
GRANT SELECT ON ops.ops_vehicle_staleness TO viewer;

-- ---------------------------------------------------------------------------
-- 2. ops.ops_detail_scrape_queue
--
-- Pool 1 (priority 1): one stale vehicle per dealer/VIN, stalest first
-- Pool 2 (priority 2): force-grab vehicles unseen > 36h beyond pool 1
-- Pool 3 (priority 3): capacity fill — remaining stale vehicles
--
-- Blocked cooldown formula (inlined from stg_blocked_cooldown):
--   next_eligible_at = last_attempted_at + 12h * 2^(num_of_attempts - 1)
--   fully_blocked    = num_of_attempts >= 5
-- ---------------------------------------------------------------------------

CREATE VIEW ops.ops_detail_scrape_queue AS
WITH stale AS (
    SELECT
        ovs.listing_id,
        ovs.vin,
        ovs.current_listing_url,
        ovs.customer_id,
        ovs.is_price_stale,
        ovs.is_full_details_stale,
        ovs.stale_reason,
        ovs.age_hours,

        row_number() OVER (
            PARTITION BY COALESCE(ovs.customer_id, ovs.vin::text)
            ORDER BY
                CASE WHEN ovs.is_full_details_stale THEN 0 ELSE 1 END,
                ovs.last_seen_at ASC
        ) AS dealer_row_num

    FROM ops.ops_vehicle_staleness ovs
    WHERE (ovs.is_price_stale OR ovs.is_full_details_stale)
      AND ovs.current_listing_url IS NOT NULL
),

-- Pool 1: one per dealer/VIN, highest priority stale vehicle
dealer_picks AS (
    SELECT
        listing_id,
        vin,
        current_listing_url,
        customer_id,
        stale_reason,
        1 AS priority
    FROM stale
    WHERE dealer_row_num = 1
),

-- Pool 2: force-grab unseen > 36h that dealer_picks missed
force_stale AS (
    SELECT
        listing_id,
        vin,
        current_listing_url,
        customer_id,
        'force_stale_36h' AS stale_reason,
        2 AS priority
    FROM stale
    WHERE age_hours > 36
      AND dealer_row_num > 1
),

-- Pool 3: remaining stale vehicles (capacity fill)
capacity_fill AS (
    SELECT
        listing_id,
        vin,
        current_listing_url,
        customer_id,
        stale_reason || '-extra' AS stale_reason,
        3 AS priority
    FROM stale
    WHERE dealer_row_num > 1
),

combined AS (
    SELECT * FROM dealer_picks
    UNION ALL
    SELECT * FROM force_stale
    UNION ALL
    SELECT * FROM capacity_fill
)

SELECT DISTINCT ON (c.listing_id)
    c.listing_id,
    c.vin,
    c.current_listing_url,
    c.customer_id,
    c.stale_reason,
    c.priority
FROM combined c
LEFT JOIN ops.blocked_cooldown bc ON bc.listing_id = c.listing_id
WHERE
    bc.listing_id IS NULL
    OR (
        bc.num_of_attempts < 5
        AND bc.last_attempted_at + (interval '1 hour' * (12 * power(2, bc.num_of_attempts::float - 1))) < now()
    )
ORDER BY c.listing_id, c.priority;

ALTER VIEW ops.ops_detail_scrape_queue OWNER TO dbt_user;
GRANT SELECT ON ops.ops_detail_scrape_queue TO viewer;
