-- The staging._events half of the results-page enqueue (hot/staging pattern).
-- Positional params: (artifact_id, minio_path, fetched_at, run_id).
-- No listing_id: a results page covers many listings, so the column is left null.
INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, minio_path, artifact_type, fetched_at, run_id)
VALUES (%s, 'pending', %s, 'results_page', %s, %s)
