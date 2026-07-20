-- Evict blocked_cooldown rows for listings that no longer exist in
-- price_observations (delisted vehicles that can never clear via a successful
-- scrape). Returns the rows removed so the caller can emit 'cleared' events.
DELETE FROM ops.blocked_cooldown bc
WHERE NOT EXISTS (
    SELECT 1 FROM ops.price_observations po
    WHERE po.listing_id = bc.listing_id
)
RETURNING listing_id, num_of_attempts
