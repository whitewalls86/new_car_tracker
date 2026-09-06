SELECT vin, listing_id, artifact_id
FROM read_parquet('{path}', union_by_name=true)
WHERE vin IS NOT NULL
