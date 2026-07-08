SELECT artifact_id, listing_id, vin, fetched_at
FROM read_parquet('{path}', union_by_name=true)
{where}
ORDER BY fetched_at
