-- Claim one artifact for processing, by id.
-- Distinct from claim_artifacts.sql, which claims a batch by type; this one
-- backs the single-artifact reprocess route.
UPDATE ops.artifacts_queue
   SET status = 'processing'
 WHERE artifact_id = %(artifact_id)s
   AND status IN ('pending', 'retry', 'skip')
RETURNING artifact_id, minio_path, artifact_type,
          listing_id, run_id, fetched_at
