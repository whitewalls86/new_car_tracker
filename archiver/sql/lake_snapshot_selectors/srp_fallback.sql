WITH obs AS (
    SELECT artifact_id, listing_id, vin, source, fetched_at, make
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
usable AS (
    SELECT * FROM obs WHERE make IS NOT NULL
),
vin_sources AS (
    SELECT vin, bool_or(source = 'detail') AS has_detail
    FROM usable
    GROUP BY vin
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY vin
            ORDER BY CASE source WHEN 'detail' THEN 1 WHEN 'srp' THEN 2 ELSE 3 END,
                     fetched_at DESC, artifact_id DESC
        ) AS rn
    FROM usable
)
SELECT r.vin, r.listing_id, r.artifact_id, r.source, r.fetched_at
FROM ranked r
JOIN vin_sources v USING (vin)
WHERE r.rn = 1 AND r.source = 'srp' AND NOT v.has_detail
ORDER BY r.vin
