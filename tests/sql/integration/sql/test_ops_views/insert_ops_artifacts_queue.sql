INSERT INTO ops.artifacts_queue
    (minio_path, artifact_type, fetched_at, status)
VALUES (%s, %s, now(), 'pending')
RETURNING artifact_id
