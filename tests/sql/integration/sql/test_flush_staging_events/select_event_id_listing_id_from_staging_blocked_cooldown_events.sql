SELECT event_id, listing_id, event_type, num_of_attempts, event_at
               FROM staging.blocked_cooldown_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.blocked_cooldown_events)
