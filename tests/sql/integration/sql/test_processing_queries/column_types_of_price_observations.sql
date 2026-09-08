SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'ops' AND table_name = 'price_observations'
ORDER BY ordinal_position
