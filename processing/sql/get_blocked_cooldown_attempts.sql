-- Get current attempt count for a listing (used after upsert to log the new value).
SELECT num_of_attempts
FROM ops.blocked_cooldown
WHERE listing_id = %(listing_id)s
