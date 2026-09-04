-- The artifact_ids a cohort's VINs and listings appear in (Plan 120 Gate C).
--
-- Collected for diagnostics and counts only. It is deliberately *not* used as
-- a blanket `artifact_id IN (...)` filter when materializing: one artifact_id
-- (an SRP or carousel page) legitimately spans many unrelated VINs, and
-- filtering on it that way silently reintroduces rows the closure excluded.
-- See lake_snapshot_export.py's module docstring.
SELECT DISTINCT artifact_id
FROM read_parquet('{path}', union_by_name=true)
WHERE {where_sql}
