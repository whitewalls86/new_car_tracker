INSERT INTO ops.artifacts_queue
    (minio_path, artifact_type, listing_id, run_id, fetched_at, status, search_key)
VALUES (%s, %s, %s::uuid, %s, now(), %s, %s)
RETURNING artifact_id
