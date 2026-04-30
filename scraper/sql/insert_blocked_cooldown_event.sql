-- Record a blocked cooldown lifecycle event.
INSERT INTO staging.blocked_cooldown_events
    (listing_id, event_type, num_of_attempts)
VALUES
    (%(listing_id)s, %(event_type)s, %(num_of_attempts)s)
