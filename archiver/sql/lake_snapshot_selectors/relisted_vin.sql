WITH events AS (
    SELECT vin, listing_id, artifact_id, event_type, previous_listing_id, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
relisted AS (
    SELECT vin FROM events GROUP BY vin HAVING count(DISTINCT listing_id) > 1
)
SELECT e.vin, e.listing_id, e.artifact_id, e.event_type, e.previous_listing_id, e.event_at
FROM events e
JOIN relisted r USING (vin)
ORDER BY e.vin, e.event_at
