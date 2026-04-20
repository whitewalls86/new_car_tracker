SELECT ra.artifact_id, ra.filepath, ra.archived_at
FROM raw_artifacts ra
WHERE ra.deleted_at IS NULL
  AND ra.filepath IS NOT NULL
  AND (
    -- ok: delete after 48 hours
    EXISTS (
      SELECT 1 FROM artifact_processing ap
      WHERE ap.artifact_id = ra.artifact_id
        AND ap.status = 'ok'
        AND ra.fetched_at < now() - interval '48 hours'
    )
    OR
    -- skip (no ok): delete immediately
    (
      EXISTS (
        SELECT 1 FROM artifact_processing ap
        WHERE ap.artifact_id = ra.artifact_id AND ap.status = 'skip'
      )
      AND NOT EXISTS (
        SELECT 1 FROM artifact_processing ap
        WHERE ap.artifact_id = ra.artifact_id AND ap.status = 'ok'
      )
    )
    OR
    -- retry (no ok): delete after 7 days
    (
      EXISTS (
        SELECT 1 FROM artifact_processing ap
        WHERE ap.artifact_id = ra.artifact_id AND ap.status = 'retry'
      )
      AND NOT EXISTS (
        SELECT 1 FROM artifact_processing ap
        WHERE ap.artifact_id = ra.artifact_id AND ap.status = 'ok'
      )
      AND ra.fetched_at < now() - interval '7 days'
    )
  )
LIMIT 10000
