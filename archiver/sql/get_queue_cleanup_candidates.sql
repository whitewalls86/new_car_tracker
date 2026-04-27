-- Return artifacts_queue rows that are safe to delete.
-- Rows with status 'complete' or 'skip' are finished; 'retry' rows stay until resolved.
SELECT artifact_id, minio_path, artifact_type, status
FROM   ops.artifacts_queue
WHERE  status IN ('complete', 'skip')
ORDER  BY created_at
LIMIT  5000;
