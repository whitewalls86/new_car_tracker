INSERT INTO staging.detail_scrape_claim_events
               (listing_id, status, event_at)
           VALUES (%s, 'claimed', %s)
           RETURNING event_id
