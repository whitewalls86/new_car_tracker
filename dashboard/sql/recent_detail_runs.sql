WITH my_runs AS (
    SELECT *
    FROM runs
    WHERE trigger = 'detail scrape'
    ORDER BY started_at DESC
    LIMIT 20
),
price_min AS (
    SELECT vin, MIN(observed_at) AS min_observed_at
    FROM analytics.int_price_events
    GROUP BY vin
),
filtered_artifacts AS (
    SELECT ra.*
    FROM raw_artifacts ra
    JOIN my_runs r USING (run_id)
)
SELECT
    r.started_at AT TIME ZONE 'America/Chicago' AS started,
    CASE
        WHEN r.finished_at IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) / 60)::text || 'm'
        ELSE ROUND(EXTRACT(EPOCH FROM (now() - r.started_at)) / 60)::text || 'm (running)'
    END AS duration,
    r.status,
    r.total_count AS batch_size,
    r.error_count AS num_errors,
    COUNT(DISTINCT d.vin) FILTER (WHERE d.price IS NOT NULL) AS prices_refreshed,
    COUNT(DISTINCT ra.artifact_id) FILTER (
        WHERE d.listing_state = 'unlisted'
    ) AS newly_unlisted,
    COUNT(DISTINCT ra.artifact_id) FILTER (
        WHERE ap.message = 'unlisted' AND d.artifact_id IS NULL
    ) AS unlisted_carousel_hit,
    COUNT(DISTINCT d.vin17) FILTER (WHERE pe.vin IS NULL) AS newly_mapped_vins
FROM my_runs r
LEFT JOIN filtered_artifacts ra ON r.run_id = ra.run_id
LEFT JOIN artifact_processing ap ON ra.artifact_id = ap.artifact_id
LEFT JOIN analytics.stg_detail_observations d ON ra.artifact_id = d.artifact_id
LEFT JOIN price_min pe ON d.vin = pe.vin AND pe.min_observed_at <= r.started_at
GROUP BY r.run_id, r.started_at, r.finished_at, r.status,
         r.total_count, r.error_count, r.last_error
ORDER BY started DESC
