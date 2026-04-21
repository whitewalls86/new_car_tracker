-- Claim up to %(limit)s pending/retry artifacts from ops.artifacts_queue.
-- Uses FOR UPDATE SKIP LOCKED so concurrent callers never double-claim.
-- Optional artifact_type filter injected by queries.py when present.
UPDATE ops.artifacts_queue
SET status = 'processing'
WHERE artifact_id IN (
    SELECT artifact_id FROM ops.artifacts_queue
    WHERE status IN ('pending', 'retry')
    {type_filter}
    ORDER BY artifact_id
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
)
RETURNING artifact_id, minio_path, artifact_type,
          listing_id, run_id, fetched_at, search_key
