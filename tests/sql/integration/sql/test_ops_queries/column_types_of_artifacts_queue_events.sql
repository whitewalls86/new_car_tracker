SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = 'artifacts_queue_events'
ORDER BY ordinal_position
