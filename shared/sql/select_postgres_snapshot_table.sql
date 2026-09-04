-- Read one whole Postgres dimension table as a single JSON array, for the
-- Plan 120 lake snapshot (Plan 162 Stage 10).
--
-- The relation is filled in from POSTGRES_SNAPSHOT_TABLES; a schema-qualified
-- relation name cannot be bound as a parameter, and the caller only ever
-- formats entries from that tuple.
--
-- to_jsonb(t) produces the exact shape jsonb_populate_recordset() reads back
-- in replace_postgres_snapshot_table.sql, so every type round-trips through
-- Postgres's own rowtype rather than a mapping written in Python.
--
-- ORDER BY to_jsonb(t)::text, not the primary key: this statement serves every
-- table in that tuple and jsonb's key order is canonical, so the text is a
-- total order that needs no per-table knowledge. The archive is checksummed,
-- so row order has to be stable across runs.
SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]'::jsonb)::text
FROM {schema}.{table} AS t
