-- Artifacts stranded in 'processing' after a processing-service death mid-batch.
-- Gate on how long the row has been in 'processing': prefer the most recent
-- 'processing' event, but fall back to created_at when that event is gone.
--
-- staging.artifacts_queue_events is flushed to parquet and deleted hourly, so
-- a row stuck longer than that flush interval has NO 'processing' event left —
-- keying on the event alone silently skips exactly the rows we most need to
-- reap. created_at (insert time, ~= processing start for a normally-flowing
-- artifact) is always present. A just-claimed row from an old pending backlog
-- still has its fresh 'processing' event, so COALESCE picks that (recent) value
-- and the row is correctly left alone.
SELECT aq.artifact_id, aq.minio_path, aq.artifact_type, aq.fetched_at,
       aq.listing_id, aq.run_id
FROM ops.artifacts_queue aq
WHERE aq.status = 'processing'
  AND COALESCE(
        (SELECT max(e.event_at)
         FROM staging.artifacts_queue_events e
         WHERE e.artifact_id = aq.artifact_id
           AND e.status = 'processing'),
        aq.created_at
      ) < now() - interval '2 hours'
