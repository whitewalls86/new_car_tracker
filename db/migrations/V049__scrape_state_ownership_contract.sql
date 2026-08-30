-- V049: Scrape state ownership — the contract half (Plan 147, Stage 4).
-- V048 expanded; this contracts. Deliberately a separate deploy from V048 and
-- from the Stage 2 writers: dropping the column is the only irreversible step
-- in Plan 147, and it should not share a window with the change that proves
-- the replacement works.
--
-- By this point the split is verified in production: last_detail_fetched_at is
-- written by the scraper's POST /scrape/claims/release (the loop guard, proven
-- over 2,000 fetches with results_processing paused 81 minutes on 2026-08-30),
-- and last_detail_enriched_at is written by the processor. The legacy
-- last_detail_scraped_at has nothing left to say.
--
-- This migration:
--   1. repeats V048's backfill — see below, it is load-bearing;
--   2. rebuilds both views reading last_detail_enriched_at alone;
--   3. drops last_detail_scraped_at.
--
-- Order matters and the whole file is one transaction (Flyway wraps it), so a
-- failure at any step leaves the dual-write world intact.

-- ---------------------------------------------------------------------------
-- 1. Repeat the backfill, BEFORE either view is rebuilt.
-- ---------------------------------------------------------------------------
-- Between the Stage 1 and Stage 2 deploys the Stage-1-era processing image
-- wrote last_detail_scraped_at without last_detail_enriched_at. Those rows are
-- covered today only by the COALESCE that step 2 collapses. Without this
-- statement they read as never enriched, become is_full_details_stale, and are
-- re-queued — reopening the exact loop Plan 147 exists to close, silently, as
-- what looks like ordinary staleness rather than as an error.
--
-- Same statement V048 ran, and idempotent. The population was 45 at the Stage 2
-- deploy, 43 at 16:38 UTC on 2026-08-30 and 41 at 19:33 UTC the same day; it
-- decays as those listings are re-enriched inside seven days and cannot grow,
-- because the Stage 2 upsert binds both columns to one parameter. A zero
-- reading is not proof this is unnecessary — the measurement and the drop are
-- not atomic with each other, and the statement costs nothing when the
-- population is empty.
UPDATE ops.price_observations
SET last_detail_enriched_at = last_detail_scraped_at
WHERE last_detail_scraped_at IS NOT NULL
  AND last_detail_enriched_at IS NULL;

-- ---------------------------------------------------------------------------
-- 2. Rebuild views in dependency order (queue depends on staleness).
-- ---------------------------------------------------------------------------
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
    po.last_detail_fetched_at,
    po.last_detail_enriched_at,

    -- Constructed listing URL (Cars.com canonical form)
    'https://www.cars.com/vehicledetail/' || po.listing_id || '/' AS current_listing_url,

    -- Age in hours from last confirmation by any source
    extract(epoch from (now() - po.last_seen_at)) / 3600.0 AS age_hours,

    -- Staleness flags
    --
    -- dealer_unenriched: customer_id IS NULL and either never enriched, or the
    -- last enrichment was more than 7 days ago (periodic re-check). A
    -- successful detail scrape with customer_id still NULL is suppressed for
    -- 7 days — the Plan 115 circuit breaker, preserved through both halves of
    -- Plan 147.
    --
    -- V048 read COALESCE(last_detail_enriched_at, last_detail_scraped_at) here
    -- because the writers that set the new column deployed after the view did.
    -- Both are deployed now and the backfill above has folded the stragglers
    -- in, so the enrichment fact lives in exactly one column again.
    (
        po.customer_id IS NULL
        AND (
            po.last_detail_enriched_at IS NULL
            OR po.last_detail_enriched_at < now() - interval '7 days'
        )
    ) AS is_full_details_stale,

    po.last_seen_at < now() - interval '24 hours' AS is_price_stale,

    -- Unified stale_reason (dealer_unenriched takes priority over price_only).
    -- Duplicates the is_full_details_stale predicate above, as in V040 and
    -- V048; the two must stay in agreement.
    CASE
        WHEN po.customer_id IS NULL
             AND (
                 po.last_detail_enriched_at IS NULL
                 OR po.last_detail_enriched_at < now() - interval '7 days'
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
-- ops.ops_detail_scrape_queue — unchanged from V048 except that it no longer
-- reads through a column that has stopped existing. The fetch backoff, the
-- three pools, the dealer/VIN partitioning and the blocked_cooldown formula
-- are all carried over verbatim.
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
      -- Fetch backoff: a listing whose detail request was spent in the last
      -- six hours is not re-claimed, whatever became of the artifact. Six
      -- hours is far longer than healthy fetch-to-enrichment latency
      -- (processing runs */5, so seconds to minutes) and far shorter than the
      -- seven-day enrichment window, so in a healthy pipeline this never
      -- fires — enrichment sets last_detail_enriched_at first and the 7-day
      -- rule takes over. It binds only when the chain is broken.
      AND (
          ovs.last_detail_fetched_at IS NULL
          OR ovs.last_detail_fetched_at < now() - interval '6 hours'
      )
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

-- ---------------------------------------------------------------------------
-- 3. Drop the legacy column, last — nothing above still reads it, and the
--    Stage 2 writers stopped setting it independently of anything here.
-- ---------------------------------------------------------------------------
ALTER TABLE ops.price_observations
    DROP COLUMN last_detail_scraped_at;
