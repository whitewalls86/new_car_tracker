UPDATE raw_artifacts
SET deleted_at = now()
WHERE archived_at IS NOT NULL
  AND deleted_at IS NULL
  AND archived_at < now() - interval '28 days'
