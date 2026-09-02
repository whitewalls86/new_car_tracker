-- Update artifact queue status (complete / retry / skip).
UPDATE ops.artifacts_queue
SET status = %(status)s
WHERE artifact_id = %(artifact_id)s
