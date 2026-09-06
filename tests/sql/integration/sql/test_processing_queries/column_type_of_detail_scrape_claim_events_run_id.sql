SELECT data_type FROM information_schema.columns
WHERE table_schema = 'staging'
  AND table_name = 'detail_scrape_claim_events'
  AND column_name = 'run_id'
