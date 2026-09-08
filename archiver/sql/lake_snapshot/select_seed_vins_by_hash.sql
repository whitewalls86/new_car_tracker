-- Deterministically sample seed VINs for a cohort (Plan 120 Gate C).
--
-- ORDER BY md5(vin) is the sampling: stable across runs, independent of file
-- enumeration order, and uncorrelated with any column the cohort selects on.
-- The LIMIT overfetches by the size of the exclusion set so the caller can trim
-- in Python, rather than relying on DuckDB list-parameter binding semantics.
SELECT DISTINCT vin
FROM read_parquet('{path}', union_by_name=true)
WHERE {where_sql}
ORDER BY md5(vin)
LIMIT {fetch_limit}
