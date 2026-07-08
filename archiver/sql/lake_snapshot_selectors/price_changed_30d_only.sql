WITH events AS (
    SELECT event_id, listing_id, vin, artifact_id, price, event_type, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
diffed AS (
    SELECT *,
        LAG(price) OVER (PARTITION BY listing_id ORDER BY event_at, event_id) AS prev_price,
        COALESCE(CAST(? AS TIMESTAMP), MAX(event_at) OVER ()) AS window_anchor
    FROM events
)
SELECT event_id, listing_id, vin, artifact_id, price, prev_price, event_at
FROM diffed
WHERE prev_price IS NOT NULL AND price != prev_price
  AND event_at >= window_anchor - INTERVAL 30 DAY
  AND event_at <  window_anchor - INTERVAL 7 DAY
ORDER BY listing_id, event_at
