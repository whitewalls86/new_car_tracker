-- Enqueue a fetched detail page for the processing service (Plan 97).
-- Written for every detail fetch, including non-200s: the artifact is in MinIO
-- either way and processing decides what the status code means.
-- Positional params: (minio_path, listing_id, run_id, fetched_at). artifact_type
-- and status are literals, both constrained by CHECKs on the table.
INSERT INTO ops.artifacts_queue
    (minio_path, artifact_type, listing_id, run_id, fetched_at, status)
VALUES (%s, 'detail_page', %s, %s, %s, 'pending')
RETURNING artifact_id
