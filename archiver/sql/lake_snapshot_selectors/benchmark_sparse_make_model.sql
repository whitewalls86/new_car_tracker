WITH obs AS (
    SELECT make, model
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
grouped AS (
    SELECT make, model, count(*) AS row_count
    FROM obs
    WHERE make IS NOT NULL AND model IS NOT NULL
    GROUP BY make, model
)
SELECT make || ' ' || model AS make_model, make, model, row_count
FROM grouped
WHERE row_count > 0 AND row_count < 20
ORDER BY row_count
