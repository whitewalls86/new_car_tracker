-- Listings still counted as blocked: the latest lifecycle event for the listing
-- is 'blocked' or 'incremented' rather than 'cleared' (Plan 128).
--
-- Read straight from the ops_normalized Parquet through DuckDB rather than from
-- the persisted analytics.duckdb view over the same files, which would contend
-- with dbt's write lock. The Parquet glob is bound as a parameter, not
-- interpolated -- read_parquet() takes one.
SELECT listing_id, current_attempts FROM (
    SELECT listing_id,
           arg_max(num_of_attempts, event_at) AS current_attempts,
           arg_max(event_type, event_at)       AS latest_event
    FROM read_parquet(?, hive_partitioning=true)
    GROUP BY listing_id
) WHERE latest_event IN ('blocked', 'incremented')
