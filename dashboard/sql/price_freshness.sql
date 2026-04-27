WITH buckets AS (
    SELECT
        FLOOR(LEAST(vs.age_hours, 24) * 2) / 2 AS age_floor,
        vs.is_full_details_stale
    FROM ops.ops_vehicle_staleness vs
    LEFT JOIN ops.blocked_cooldown bc ON bc.listing_id = vs.listing_id
    WHERE vs.age_hours IS NOT NULL
      AND bc.listing_id IS NULL
)
SELECT
    (24 - age_floor)::numeric AS hours_until_stale,
    TO_CHAR((24 - age_floor)::numeric, 'FM90.0') || 'h' AS expiry_bucket,
    COUNT(*) FILTER (WHERE NOT is_full_details_stale) AS enriched,
    COUNT(*) FILTER (WHERE is_full_details_stale) AS full_details_stale,
    COUNT(*) AS total
FROM buckets
GROUP BY age_floor
ORDER BY age_floor DESC
