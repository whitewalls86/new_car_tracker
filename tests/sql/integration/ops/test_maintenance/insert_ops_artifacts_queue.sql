INSERT INTO ops.artifacts_queue (minio_path, artifact_type, fetched_at, status, created_at)
VALUES (%s, 'detail_page', now(), %s, now() - (%s || ' hours')::interval)
RETURNING artifact_id
