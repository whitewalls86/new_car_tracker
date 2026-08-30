-- V048: Scrape state ownership — separating fetch from enrichment (Plan 147,
-- Stage 1, the expand half of an expand/contract pair; V049 contracts).
--
-- Problem: ops.price_observations.last_detail_scraped_at is the guard that
-- stops a listing being re-fetched, but it is written by the *processing*
-- service, two hops downstream of the fetch and behind an async queue. When
-- that chain breaks — processing paused, crashed, backed up, or a parser gap —
-- the claim rows are deleted, nothing sets the timestamp, and the same ~100
-- listings are re-claimed every 15 minutes. Plan 115 (V040) closed one door
-- into that loop; the loop has since been reachable through at least three
-- more.
--
-- Fix: split the one column into the two separate facts it was being asked to
-- carry, each owned by the component that knows it:
--
--   last_detail_fetched_at   "we spent a detail request"  — written by the
--                            scraper's release_claims, drives the loop guard;
--   last_detail_enriched_at  "we got full detail data"    — written by the
--                            processor, drives is_full_details_stale (7d).
--
-- Expand, not rename. Flyway here is forward-only with no staging environment
-- to rehearse against, so an ALTER ... RENAME COLUMN would change the schema
-- the instant the stack comes up and break any still-running processing
-- container until it is recreated. last_detail_scraped_at therefore stays in
-- place and still written for one release; V049 drops it.
--
-- This migration is inert for existing rows on its own: last_detail_fetched_at
-- is null everywhere, so the new backoff predicate does not bind, and the
-- enrichment check reads through BOTH columns (see below) so the view keeps
-- behaving exactly as V040 did until the Stage 2 writers are deployed.

ALTER TABLE ops.price_observations
    ADD COLUMN IF NOT EXISTS last_detail_fetched_at timestamptz;

ALTER TABLE ops.price_observations
    ADD COLUMN IF NOT EXISTS last_detail_enriched_at timestamptz;

-- Existing last_detail_scraped_at values *are* enrichment timestamps: V040 set
-- the column on a detail parse outcome, not on a fetch. Backfill preserves the
-- 7-day suppression for every already-scraped listing.
UPDATE ops.price_observations
SET last_detail_enriched_at = last_detail_scraped_at
WHERE last_detail_scraped_at IS NOT NULL
  AND last_detail_enriched_at IS NULL;

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
    -- 7 days — the Plan 115 circuit breaker, preserved.
    --
    -- The COALESCE is load-bearing for the length of the dual-write release.
    -- Stage 2 is what teaches any writer to set last_detail_enriched_at, and
    -- it is a separate deploy from this migration. Reading the new column
    -- alone here would leave every freshly enriched listing with a null
    -- last_detail_enriched_at, is_full_details_stale true, and re-queued every
    -- fifteen minutes — reopening the exact Plan 115 loop this plan exists to
    -- close. V049 collapses it to the bare column when it drops the legacy one.
    (
        po.customer_id IS NULL
        AND (
            COALESCE(po.last_detail_enriched_at, po.last_detail_scraped_at) IS NULL
            OR COALESCE(po.last_detail_enriched_at, po.last_detail_scraped_at)
               < now() - interval '7 days'
        )
    ) AS is_full_details_stale,

    po.last_seen_at < now() - interval '24 hours' AS is_price_stale,

    -- Unified stale_reason (dealer_unenriched takes priority over price_only).
    -- Duplicates the is_full_details_stale predicate above, as in V040; the two
    -- must stay in agreement, COALESCE included.
    CASE
        WHEN po.customer_id IS NULL
             AND (
                 COALESCE(po.last_detail_enriched_at, po.last_detail_scraped_at) IS NULL
                 OR COALESCE(po.last_detail_enriched_at, po.last_detail_scraped_at)
                    < now() - interval '7 days'
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
-- ops.ops_detail_scrape_queue (V040 logic plus the fetch backoff)
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
      --
      -- Null means never fetched, which is every row on the day of this
      -- migration, so the predicate is a no-op until Stage 2 deploys the
      -- release_claims write.
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
