WITH obs AS (
    SELECT vin, listing_id, artifact_id, fetched_at, listing_state
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
active AS (
    SELECT listing_id, min(fetched_at) AS first_active_at
    FROM obs
    WHERE listing_state = 'active'
    GROUP BY listing_id
),
unlisted AS (
    SELECT listing_id, min(fetched_at) AS first_unlisted_at
    FROM obs
    WHERE listing_state = 'unlisted'
    GROUP BY listing_id
)
SELECT a.listing_id, a.first_active_at, u.first_unlisted_at
FROM active a
JOIN unlisted u USING (listing_id)
WHERE u.first_unlisted_at > a.first_active_at
ORDER BY a.listing_id
