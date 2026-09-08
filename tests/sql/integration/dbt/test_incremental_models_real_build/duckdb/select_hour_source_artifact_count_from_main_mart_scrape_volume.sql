select hour, source, artifact_count, observation_count, unique_listings, valid_vin_count, vin_extraction_pct from main.mart_scrape_volume where hour = ? and source = ?
