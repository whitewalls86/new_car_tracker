-- Emit cleared-cooldown events for the reconcile pass, one round trip.
-- The VALUES %s placeholder is psycopg2.extras.execute_values' own, not a
-- parameter marker: it expands to the whole argument list.
INSERT INTO staging.blocked_cooldown_events
    (listing_id, event_type, num_of_attempts) VALUES %s
