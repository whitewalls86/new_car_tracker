SELECT data_type FROM information_schema.columns
WHERE table_schema = 'ops' AND table_name = 'vin_to_listing'
  AND column_name = 'vin'
