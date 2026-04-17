UPDATE raw_artifacts
SET deleted_at = now()
WHERE artifact_id = ANY(%s)
