SELECT is_nullable FROM information_schema.columns
WHERE table_schema = 'ops' AND table_name = 'artifacts_queue'
  AND column_name = 'minio_path'
