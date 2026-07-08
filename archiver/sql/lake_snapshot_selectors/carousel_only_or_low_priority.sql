WITH obs AS (
    SELECT vin, source
    FROM read_parquet('{path}', union_by_name=true)
    {where}
)
SELECT DISTINCT vin
FROM obs
WHERE source = 'carousel'
ORDER BY vin
