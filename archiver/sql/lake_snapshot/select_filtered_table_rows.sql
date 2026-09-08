-- Read one source table filtered to a closed cohort, in a deterministic row
-- order (Plan 120 Gate D).
--
-- ORDER BY the table's sort keys is load-bearing rather than cosmetic: the
-- archive is checksummed, so row order has to be stable across runs and
-- independent of source file enumeration order.
--
-- The predicate is built in Python because it is an OR of clause groups whose
-- shape depends on the table -- blocked_cooldown_events has no vin column, and
-- silver_observations alone admits exact artifact row keys outside the time
-- window. Every value inside it is bound; only the clause structure is
-- interpolated.
SELECT *
FROM read_parquet('{path}', union_by_name=true, hive_partitioning=true)
WHERE {where_sql}
ORDER BY {order_by}
