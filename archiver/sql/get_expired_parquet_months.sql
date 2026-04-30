-- raw_artifacts was dropped in V036. The old HTML→Parquet export pipeline
-- no longer exists; new raw HTML goes directly to MinIO via ops.artifacts_queue.
-- Return empty so the cleanup job succeeds as a no-op.
SELECT 0::int AS year, 0::int AS month WHERE FALSE
