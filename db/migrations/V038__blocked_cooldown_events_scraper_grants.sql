-- V038: Grant scraper_user INSERT + sequence access on staging.blocked_cooldown_events.
--
-- V027 granted SELECT + DELETE for the archiver flush path but omitted INSERT and
-- the sequence grant needed for the scraper's insert_blocked_cooldown_event writes.
-- This caused "permission denied for sequence blocked_cooldown_events_event_id_seq"
-- warnings logged as non-fatal in the scraper.

GRANT INSERT ON staging.blocked_cooldown_events TO scraper_user;
GRANT USAGE, SELECT ON SEQUENCE staging.blocked_cooldown_events_event_id_seq TO scraper_user;
