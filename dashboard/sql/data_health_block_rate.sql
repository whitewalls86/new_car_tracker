-- Hourly 403 block rate for the last 14 days.
-- Anchored to mart_scrape_volume so every active scraping hour appears,
-- including hours with zero blocks (block counts default to 0, not a gap).
SELECT
    sv.hour,
    COALESCE(b.new_blocks, 0)                   AS new_blocks,
    COALESCE(b.block_increments, 0)             AS block_increments,
    COALESCE(b.total_block_events, 0)           AS total_block_events,
    COALESCE(b.unique_listings_blocked, 0)      AS unique_listings_blocked,
    b.max_attempts_seen,
    sv.observation_count                        AS total_observations,
    ROUND(
        COALESCE(b.new_blocks, 0) * 100.0
        / NULLIF(sv.observation_count, 0), 3
    )                                           AS block_rate_pct
FROM (
    SELECT hour, SUM(observation_count) AS observation_count
    FROM mart_scrape_volume
    GROUP BY hour
) sv
LEFT JOIN mart_block_rate b ON b.hour = sv.hour
WHERE sv.hour >= now() - INTERVAL '14 days'
ORDER BY sv.hour ASC
