-- Update artifact queue status (retry / skip during maintenance reaping).
UPDATE ops.artifacts_queue
SET status = %(status)s
WHERE artifact_id = %(artifact_id)s
