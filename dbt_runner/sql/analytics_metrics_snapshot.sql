-- Plan 136 Stage 2 (defect D1, second half).
--
-- This was MAX(hour), with no completeness filter, and the gauge it feeds is
-- documented as "the most recent COMPLETE scrape hour". It was not.
-- mart_scrape_volume buckets on date_trunc('hour', fetched_at) and
-- hourly_analytics_refresh builds at `0 * * * *`, so MAX(hour) is the bucket
-- for the hour currently in progress, holding whatever few minutes of data
-- results_processing had flushed by then. Published as an hourly total, that
-- is a manufactured drop, every hour -- and ct-scrape-volume-drop's threshold
-- is 100 observations. It is the most likely mechanism behind that alert
-- firing at 05:51 on 2026-08-15, forty minutes into a healthy recovery.
--
-- `now() AT TIME ZONE 'UTC'` rather than a bare `now()`: `hour` is a naive
-- TIMESTAMP holding UTC, and comparing it against a TIMESTAMPTZ would make the
-- boundary depend on the container's local timezone.
--
-- data_through moves with it deliberately. It is the freshness field on the
-- public /info page, and it should name the hour the counts actually describe;
-- letting the two disagree would trade a wrong number for a confusing pair.
-- Snapshot freshness is not this field -- that is
-- cartracker_metrics_last_success_timestamp_seconds (Plan 143), which is
-- unaffected.
WITH latest_scrape_hour AS (
    SELECT MAX(hour) AS data_through
    FROM main.mart_scrape_volume
    WHERE hour < CAST(date_trunc('hour', now() AT TIME ZONE 'UTC') AS TIMESTAMP)
),
latest_scrape_volume AS (
    SELECT
        COALESCE(SUM(observation_count), 0) AS observation_count,
        COALESCE(SUM(artifact_count), 0) AS artifact_count
    FROM main.mart_scrape_volume
    WHERE hour = (SELECT data_through FROM latest_scrape_hour)
),
latest_block_rate AS (
    SELECT COALESCE(total_block_events, 0) AS total_block_events
    FROM main.mart_block_rate
    ORDER BY hour DESC
    LIMIT 1
),
latest_detail_outcome AS (
    SELECT COALESCE(extraction_yield, 0) AS extraction_yield
    FROM main.mart_detail_batch_outcomes
    ORDER BY obs_date DESC
    LIMIT 1
),
price_freshness AS (
    SELECT COALESCE(
        ROUND(
            100.0 * SUM(stale_gt_14d) / NULLIF(
                SUM(
                    stale_gt_14d + fresh_lt_1d + fresh_1_3d
                    + fresh_4_7d + fresh_8_14d
                ),
                0
            ),
            2
        ),
        0
    ) AS stale_listings_pct
    FROM main.mart_price_freshness_trend
),
cooldown_counts AS (
    SELECT
        COALESCE(
            SUM(listing_count) FILTER (WHERE attempt_bucket IN ('1', '2', '3-4')),
            0
        ) AS cooldown_backlog,
        COALESCE(
            SUM(listing_count) FILTER (WHERE attempt_bucket IN ('5-10', '11+')),
            0
        ) AS cooldown_permanent
    FROM main.mart_cooldown_cohorts
)
SELECT
    scrape.observation_count AS cartracker_observation_count_last_hour,
    scrape.artifact_count AS cartracker_artifact_count_last_hour,
    COALESCE(blocks.total_block_events, 0) AS cartracker_block_events_last_hour,
    COALESCE(detail.extraction_yield, 0) AS cartracker_extraction_yield_last_day,
    freshness.stale_listings_pct AS cartracker_stale_listings_pct,
    cooldown.cooldown_backlog AS cartracker_cooldown_backlog,
    cooldown.cooldown_permanent AS cartracker_cooldown_permanent,
    latest.data_through
FROM latest_scrape_hour AS latest
CROSS JOIN latest_scrape_volume AS scrape
LEFT JOIN latest_block_rate AS blocks ON TRUE
LEFT JOIN latest_detail_outcome AS detail ON TRUE
CROSS JOIN price_freshness AS freshness
CROSS JOIN cooldown_counts AS cooldown
