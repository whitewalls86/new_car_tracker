-- Record a detail scrape claim lifecycle event.
INSERT INTO staging.detail_scrape_claim_events
    (listing_id, run_id, status)
VALUES
    (%(listing_id)s, %(run_id)s, %(status)s)
