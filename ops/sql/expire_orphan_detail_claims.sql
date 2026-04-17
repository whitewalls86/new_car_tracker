DELETE FROM detail_scrape_claims
WHERE status = 'running'
  AND claimed_by::uuid NOT IN (
      SELECT DISTINCT run_id FROM runs WHERE status = 'running'
  )
RETURNING listing_id
