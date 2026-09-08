-- The exact (artifact_id, vin, listing_id) row identities behind a set of
-- already-selected entities (Plan 120 Gate C).
--
-- Assumes the selector's candidate SQL exposes all three columns, which holds
-- for every selector using this mechanism. `membership` is an in_clause() over
-- the selected keys -- its placeholders are bound, only their number varies.
SELECT DISTINCT artifact_id, vin, listing_id
FROM ({candidate_sql}) AS c
WHERE {membership}
