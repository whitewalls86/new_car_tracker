DELETE FROM ops.blocked_cooldown WHERE listing_id = ANY(%s::uuid[])
