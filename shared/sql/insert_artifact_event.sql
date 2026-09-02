-- Append a status-transition event row.
INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, minio_path, artifact_type, fetched_at, listing_id, run_id)
VALUES
    (%(artifact_id)s, %(status)s, %(minio_path)s, %(artifact_type)s,
     %(fetched_at)s, %(listing_id)s, %(run_id)s)
