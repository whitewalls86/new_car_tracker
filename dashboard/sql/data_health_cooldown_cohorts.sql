SELECT
    attempt_bucket,
    listing_count,
    min_attempts,
    max_attempts
FROM mart_cooldown_cohorts
ORDER BY bucket_order
