WITH obs AS (
    SELECT DISTINCT vin
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
priced AS (
    SELECT DISTINCT vin
    FROM read_parquet('{price_observation_events_path}', union_by_name=true)
    WHERE price IS NOT NULL AND price > 0
)
SELECT o.vin
FROM obs o
LEFT JOIN priced p USING (vin)
WHERE p.vin IS NULL
ORDER BY o.vin
