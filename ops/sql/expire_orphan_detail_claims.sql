DELETE FROM detail_scrape_claims
WHERE status = 'running'
  AND claimed_at < now() - interval '2 hours'
RETURNING listing_id
