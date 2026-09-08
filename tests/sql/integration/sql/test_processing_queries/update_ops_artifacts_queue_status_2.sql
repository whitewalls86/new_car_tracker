UPDATE ops.artifacts_queue SET status = 'processing'
WHERE artifact_id IN (
    SELECT artifact_id FROM ops.artifacts_queue
    WHERE status IN ('pending', 'retry')
      AND artifact_type = %s
    ORDER BY artifact_id LIMIT 10
    FOR UPDATE SKIP LOCKED
)
RETURNING artifact_id
