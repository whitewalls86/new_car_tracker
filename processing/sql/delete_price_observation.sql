-- Delete a price observation (unlisted path).
-- Presence in the table = active; absence = unlisted.
DELETE FROM ops.price_observations
WHERE listing_id = %(listing_id)s
