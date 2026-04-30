-- Return artifacts_queue rows that are safe to delete.
-- 'complete' and 'skip' rows are always eligible.
-- 'retry' rows older than 1 hour are considered permanently stuck and included.
SELECT artifact_id, minio_path, artifact_type, status
FROM   ops.artifacts_queue
WHERE  status IN ('complete', 'skip')
   OR (status = 'retry' AND created_at < now() - interval '1 hour')
ORDER  BY created_at
LIMIT  5000;
