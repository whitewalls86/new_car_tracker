-- Record a 'cleared' blocked-cooldown lifecycle event so mart_cooldown_cohorts
-- (state = latest event per listing) drops the listing from the backlog.
INSERT INTO staging.blocked_cooldown_events
    (listing_id, event_type, num_of_attempts)
VALUES
    (%(listing_id)s, 'cleared', %(num_of_attempts)s)
