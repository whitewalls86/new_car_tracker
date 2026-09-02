-- How many detail claims are still running, and the oldest one's timestamp.
-- coordination_drain reads this to decide whether a drain has finished; the
-- MIN is what distinguishes "busy" from "stuck".
SELECT COUNT(*), MIN(claimed_at)
  FROM ops.detail_scrape_claims
 WHERE status = 'running'
