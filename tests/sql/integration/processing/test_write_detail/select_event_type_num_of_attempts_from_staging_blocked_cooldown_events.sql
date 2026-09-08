SELECT event_type, num_of_attempts FROM staging.blocked_cooldown_events WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1
