INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, minio_path, artifact_type, fetched_at, listing_id, run_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
