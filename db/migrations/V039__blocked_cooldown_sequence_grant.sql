-- V039: Grant scraper_user sequence access for staging.blocked_cooldown_events.
--
-- V038 granted INSERT on the table but omitted the sequence, causing
-- "permission denied for sequence blocked_cooldown_events_event_id_seq" errors.

GRANT USAGE, SELECT ON SEQUENCE staging.blocked_cooldown_events_event_id_seq TO scraper_user;
