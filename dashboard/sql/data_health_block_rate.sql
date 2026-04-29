-- Hourly 403 block events for the last 14 days, joined to total observation
-- volume to compute a block rate percentage.
SELECT
    b.hour,
    b.new_blocks,
    b.block_increments,
    b.total_block_events,
    b.unique_listings_blocked,
    b.max_attempts_seen,
    COALESCE(sv.observation_count, 0)           AS total_observations,
    ROUND(
        b.new_blocks * 100.0
        / NULLIF(sv.observation_count, 0), 3
    )                                           AS block_rate_pct
FROM mart_block_rate b
LEFT JOIN (
    SELECT hour, SUM(observation_count) AS observation_count
    FROM mart_scrape_volume
    GROUP BY hour
) sv ON sv.hour = b.hour
WHERE b.hour >= now() - INTERVAL '14 days'
ORDER BY b.hour ASC
