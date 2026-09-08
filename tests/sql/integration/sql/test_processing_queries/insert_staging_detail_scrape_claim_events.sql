INSERT INTO staging.detail_scrape_claim_events
    (listing_id, run_id, status)
VALUES (%s::uuid, %s, 'processed')
