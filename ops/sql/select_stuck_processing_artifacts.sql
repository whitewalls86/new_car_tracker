-- Artifacts stranded in 'processing' after a processing-service death mid-batch.
-- Gate on the most recent 'processing' event being >2h old (not created_at) so a
-- legitimately-old pending row that only just began processing is not reaped out
-- from under an active worker. Normal batches finish in minutes.
SELECT aq.artifact_id, aq.minio_path, aq.artifact_type, aq.fetched_at,
       aq.listing_id, aq.run_id
FROM ops.artifacts_queue aq
WHERE aq.status = 'processing'
  AND (
    SELECT max(e.event_at)
    FROM staging.artifacts_queue_events e
    WHERE e.artifact_id = aq.artifact_id
      AND e.status = 'processing'
  ) < now() - interval '2 hours'
