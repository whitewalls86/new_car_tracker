-- Row count, timestamp bounds and (where the table has a vin column) distinct
-- VINs for one production source table (Plan 120 Gate A).
--
-- The projection varies by table -- blocked_cooldown_events has no vin -- so
-- `select_parts` is filled from SOURCE_TABLE_SPECS rather than being one
-- statement per table. The audit is what tells an export whether the window it
-- is about to scan holds anything at all.
SELECT {select_parts}
FROM read_parquet('{path}', union_by_name=true)
{where_sql}
