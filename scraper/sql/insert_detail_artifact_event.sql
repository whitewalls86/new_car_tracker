-- The staging._events half of the detail-page enqueue (hot/staging pattern).
-- Positional params: (artifact_id, minio_path, fetched_at, listing_id, run_id).
-- Five placeholders for seven columns: status and artifact_type are literals,
-- interleaved between them. Both lists are in the same order.
INSERT INTO staging.artifacts_queue_events (
    artifact_id, status, minio_path, artifact_type,
    fetched_at, listing_id, run_id
)
VALUES (%s, 'pending', %s, 'detail_page', %s, %s, %s)
