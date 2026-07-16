{{
  config(
    materialized='ephemeral' if target.type == 'spark' else 'view'
  )
}}

-- Plan 125 Gate A: `ephemeral` on spark, `view` on duckdb. Both mean "no
-- stored data, recomputed on demand", so this is the closest Spark equivalent
-- of the DuckDB behavior -- not a materialization upgrade.
--
-- It cannot be a view on spark. A persisted view stores its body and
-- re-analyzes it against the view's own catalog on every read, which
-- qualifies this model's `parquet.`s3a://...`` datasource reference into
-- `cartracker.parquet.`s3a://...`` -- a table lookup in the Iceberg catalog,
-- which fails with TABLE_OR_VIEW_NOT_FOUND. Verified at Gate A: the same
-- reference resolves correctly inline (directly or in a CTE), which is
-- exactly what `ephemeral` compiles to.
--
-- 403 blocked cooldown lifecycle events from MinIO.
-- One row per event: either 'blocked' (first block) or 'incremented' (subsequent attempt).
-- num_of_attempts is the cumulative count at the time of the event.

select
    event_id,
    listing_id,
    event_type,
    num_of_attempts,
    event_at
from {{ parquet_source('ops_events', 'blocked_cooldown_events') }}
