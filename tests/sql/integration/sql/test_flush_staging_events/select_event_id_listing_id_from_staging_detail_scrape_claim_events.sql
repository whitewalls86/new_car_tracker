SELECT event_id, listing_id, run_id, status, stale_reason, vin, event_at
               FROM staging.detail_scrape_claim_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.detail_scrape_claim_events)
