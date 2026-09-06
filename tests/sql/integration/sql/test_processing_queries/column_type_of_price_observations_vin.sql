SELECT data_type FROM information_schema.columns
WHERE table_schema = 'ops' AND table_name = 'price_observations'
  AND column_name = 'vin'
