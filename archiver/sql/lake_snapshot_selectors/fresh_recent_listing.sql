WITH obs AS (
    SELECT listing_id, fetched_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
agg AS (
    SELECT listing_id,
           min(fetched_at) AS first_seen_at,
           max(fetched_at) AS last_seen_at
    FROM obs
    GROUP BY listing_id
),
anchored AS (
    SELECT *, max(last_seen_at) OVER () AS window_anchor
    FROM agg
)
SELECT listing_id, first_seen_at, last_seen_at, window_anchor
FROM anchored
WHERE window_anchor - first_seen_at <= INTERVAL 14 DAY
  AND window_anchor - last_seen_at  <= INTERVAL 2 DAY
ORDER BY listing_id
