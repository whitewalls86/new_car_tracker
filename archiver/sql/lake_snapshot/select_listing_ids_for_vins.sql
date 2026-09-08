-- One step of entity closure: the listing_ids a set of VINs was ever observed
-- under (Plan 120 Gate C). Run once per table in _VIN_LISTING_TABLES.
SELECT DISTINCT listing_id
FROM read_parquet('{path}', union_by_name=true)
WHERE {where_sql}
