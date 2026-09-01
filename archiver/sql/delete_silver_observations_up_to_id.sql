-- Drop the silver observations a flush has already written to MinIO.
-- Runs only after the Parquet write succeeds, and only up to the snapshot
-- boundary, so rows that arrived during the flush survive for the next one.
DELETE FROM staging.silver_observations WHERE id <= %s
