INSERT INTO staging.blocked_cooldown_events
               (listing_id, event_type, num_of_attempts, event_at)
           VALUES (%s, 'blocked', 1, %s)
           RETURNING event_id
