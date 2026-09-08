DELETE FROM staging.detail_scrape_claim_events WHERE listing_id = ANY(%s::uuid[])
