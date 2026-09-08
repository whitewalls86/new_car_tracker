-- The listing_ids a cohort's listings were remapped *from* (Plan 120 Gate C).
--
-- Without this step a VIN's history stops at its current listing_id, and the
-- exported cohort holds a remap event pointing at a listing that is not in it.
SELECT DISTINCT previous_listing_id
FROM read_parquet('{path}', union_by_name=true)
WHERE {where_sql}
