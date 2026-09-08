SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'ops' AND table_name = 'artifacts_queue'
ORDER BY ordinal_position
