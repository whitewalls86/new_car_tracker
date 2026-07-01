-- V040: Detail unenriched circuit breaker (Plan 115).
--
-- Problem: listings with customer_id IS NULL are re-queued for detail scraping
-- every 15 minutes even after a successful detail scrape, because the old
-- is_full_details_stale check was purely customer_id IS NULL.
--
-- Fix: add last_detail_scraped_at to price_observations. Detail writes set it;
-- SRP and carousel writes leave it NULL (via COALESCE in the upsert). The
-- staleness view now gates dealer_unenriched on this timestamp so a recently
-- detail-scraped listing is suppressed for 7 days.

ALTER TABLE ops.price_observations
    ADD COLUMN IF NOT EXISTS last_detail_scraped_at timestamptz;

-- Recreate views in dependency order (queue depends on staleness).
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
    po.last_detail_scraped_at,

    -- Constructed listing URL (Cars.com canonical form)
    'https://www.cars.com/vehicledetail/' || po.listing_id || '/' AS current_listing_url,

    -- Age in hours from last confirmation by any source
    extract(epoch from (now() - po.last_seen_at)) / 3600.0 AS age_hours,

    -- Staleness flags
    --
    -- dealer_unenriched: customer_id IS NULL and either never detail-scraped,
    -- or last detail scrape was more than 7 days ago (periodic re-check).
    -- A successful detail scrape with customer_id still NULL is suppressed for
    -- 7 days via last_detail_scraped_at.
    (
        po.customer_id IS NULL
        AND (
            po.last_detail_scraped_at IS NULL
            OR po.last_detail_scraped_at < now() - interval '7 days'
        )
    ) AS is_full_details_stale,

    po.last_seen_at < now() - interval '24 hours' AS is_price_stale,

    -- Unified stale_reason (dealer_unenriched takes priority over price_only)
    CASE
        WHEN po.customer_id IS NULL
             AND (
                 po.last_detail_scraped_at IS NULL
                 OR po.last_detail_scraped_at < now() - interval '7 days'
             )
            THEN 'dealer_unenriched'
        WHEN po.last_seen_at < now() - interval '24 hours'
            THEN 'price_only'
        ELSE 'not_stale'
    END AS stale_reason

FROM ops.price_observations po;

ALTER VIEW ops.ops_vehicle_staleness OWNER TO dbt_user;
GRANT SELECT ON ops.ops_vehicle_staleness TO viewer;

-- ---------------------------------------------------------------------------
-- ops.ops_detail_scrape_queue (unchanged logic; rebuilt to pick up new view)
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
