WITH vehicle_stats AS (
    SELECT
        COUNT(*) FILTER (WHERE listing_state = 'active') AS active_listings,
        COALESCE(SUM(total_price_observations), 0) AS price_observations,
        COUNT(DISTINCT make || '|' || model) FILTER (
            WHERE make IS NOT NULL AND model IS NOT NULL
        ) AS make_model_pairs
    FROM main.mart_vehicle_snapshot
),
throughput AS (
    SELECT
        COALESCE(SUM(artifact_count) / 24.0, 0) AS artifacts_per_hour,
        COALESCE(SUM(observation_count) / 24.0, 0) AS observations_per_hour
    FROM main.mart_scrape_volume
    WHERE hour >= NOW() - INTERVAL '24 hours'
)
SELECT
    vehicles.active_listings,
    vehicles.price_observations,
    vehicles.make_model_pairs,
    throughput.artifacts_per_hour,
    throughput.observations_per_hour
FROM vehicle_stats AS vehicles
CROSS JOIN throughput
