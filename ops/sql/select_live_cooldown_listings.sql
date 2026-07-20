-- Listing IDs currently present in the live blocked_cooldown table.
SELECT listing_id::text AS listing_id FROM ops.blocked_cooldown
