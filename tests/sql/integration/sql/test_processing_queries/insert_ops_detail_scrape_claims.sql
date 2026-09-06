INSERT INTO ops.detail_scrape_claims
    (listing_id, claimed_by, claimed_at, status)
VALUES (%s::uuid, 'test-run', now(), 'running')
ON CONFLICT (listing_id) DO NOTHING
