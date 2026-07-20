-- Clear blocked cooldown on successful detail scrape.
-- No-op if listing was not blocked.
DELETE FROM ops.blocked_cooldown
WHERE listing_id = %(listing_id)s
