-- Emit cleared-cooldown events for the reconcile pass, one round trip.
-- The VALUES placeholder below belongs to psycopg2.extras.execute_values, not
-- to the caller: it expands to the whole argument list rather than binding one
-- value. Do not write a placeholder into these comments -- execute_values
-- counts them across the entire string and refuses a statement with two.
INSERT INTO staging.blocked_cooldown_events
    (listing_id, event_type, num_of_attempts) VALUES %s
