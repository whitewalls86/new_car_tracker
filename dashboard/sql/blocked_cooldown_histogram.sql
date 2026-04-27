WITH buckets AS (
    SELECT
        FLOOR(
            GREATEST(
                (EXTRACT(EPOCH FROM (
                    last_attempted_at
                    + (interval '1 hour' * (12 * power(2, num_of_attempts::float - 1)))
                    - now()
                )) / 3600),
                0
            ) / 2
        ) * 2 AS age_floor
    FROM ops.blocked_cooldown
    WHERE num_of_attempts < 5
)
SELECT
    age_floor::numeric AS hours_until_eligible,
    TO_CHAR(age_floor::numeric, 'FM90.0') || 'h' AS eligible_bucket,
    COUNT(*) AS total
FROM buckets
GROUP BY age_floor
ORDER BY age_floor DESC
