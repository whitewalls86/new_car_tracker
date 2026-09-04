-- Replace one Postgres dimension table's contents from a lake snapshot's JSON
-- array (Plan 162 Stage 10). The consuming half of
-- select_postgres_snapshot_table.sql.
--
-- The DELETE is here rather than at the call site because "load the snapshot's
-- rows" is one operation: V001 seeds public.search_configs, so an INSERT alone
-- would hit its primary key, and a caller that forgot the DELETE would leave
-- the table holding a mix of snapshot and migration rows -- which is exactly
-- the "builds green over a world that is not the snapshot's" failure this job
-- exists to catch.
--
-- jsonb_populate_recordset against the table's own rowtype does every cast:
-- timestamptz from an ISO string, jsonb from a nested object, integer from a
-- number. A column the snapshot predates arrives NULL and fails loudly on a
-- NOT NULL, which is the correct signal to bump the pin.
DELETE FROM {schema}.{table};
INSERT INTO {schema}.{table}
SELECT * FROM jsonb_populate_recordset(NULL::{schema}.{table}, %s::jsonb)
