WITH latest_scrape_hour AS (
    SELECT MAX(hour) AS data_through
    FROM main.mart_scrape_volume
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
