INSERT INTO ops.artifacts_queue
    (minio_path, artifact_type, fetched_at, status)
VALUES (%s, %s, now(), %s)
RETURNING artifact_id, minio_path, artifact_type, listing_id, run_id, fetched_at
