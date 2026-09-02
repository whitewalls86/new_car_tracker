-- Delete artifacts the cleanup pass has cleared for removal.
-- Retry rows get an hour's grace so an in-flight retry is not deleted
-- underneath the processor that is still working it.
DELETE FROM ops.artifacts_queue
WHERE  artifact_id = ANY(%s)
  AND (
      status IN ('complete', 'skip')
      OR (status = 'retry' AND created_at < now() - interval '1 hour')
  )
RETURNING artifact_id
