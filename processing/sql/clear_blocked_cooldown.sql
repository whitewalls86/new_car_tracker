-- Clear blocked cooldown on successful detail scrape.
-- No-op if listing was not blocked. RETURNING lets the caller emit a 'cleared'
-- lifecycle event (and skip it when nothing was deleted).
DELETE FROM ops.blocked_cooldown
WHERE listing_id = %(listing_id)s
RETURNING num_of_attempts
