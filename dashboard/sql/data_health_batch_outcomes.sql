SELECT
    obs_date,
    detail_observations,
    detail_artifacts,
    valid_vin_count,
    unique_vins_enriched,
    extraction_yield
FROM mart_detail_batch_outcomes
ORDER BY obs_date DESC
LIMIT 30
