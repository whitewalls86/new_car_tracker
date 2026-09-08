SELECT status FROM staging.detail_scrape_claim_events WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1
