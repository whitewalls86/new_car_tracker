INSERT INTO detail_scrape_claims (listing_id, claimed_by, status, claimed_at)
VALUES (%s, %s, 'running', now() - (%s || ' hours')::interval)
