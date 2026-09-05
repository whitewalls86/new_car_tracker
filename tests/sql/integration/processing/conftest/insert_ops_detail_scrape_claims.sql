INSERT INTO ops.detail_scrape_claims (listing_id, claimed_by, status)
VALUES (%s::uuid, %s, 'running')
