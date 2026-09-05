DELETE FROM staging.blocked_cooldown_events WHERE listing_id = ANY(%s::uuid[])
