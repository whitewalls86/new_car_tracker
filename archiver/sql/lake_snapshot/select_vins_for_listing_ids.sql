-- The mirror of select_listing_ids_for_vins.sql: the VINs a set of listing_ids
-- was ever observed under (Plan 120 Gate C). Closure alternates the two until
-- neither set grows, which is what makes the exported cohort self-contained.
SELECT DISTINCT vin
FROM read_parquet('{path}', union_by_name=true)
WHERE {where_sql}
