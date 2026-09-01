-- Drop the staging-event rows a flush has already written to MinIO.
-- Runs only after the Parquet write succeeds, and only up to the snapshot
-- boundary, so rows that arrived during the flush survive for the next one.
-- The relation and its primary key are filled in from _TABLE_CONFIGS.
DELETE FROM {table} WHERE {pk} <= %s
