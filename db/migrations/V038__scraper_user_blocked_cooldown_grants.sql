-- V038: Grant scraper_user write access to blocked_cooldown tables.
--
-- scraper_user needs INSERT/UPDATE on ops.blocked_cooldown and INSERT on
-- staging.blocked_cooldown_events to record 403 blocks at scrape time.
-- Previously only SELECT was granted, causing silent permission errors.

GRANT INSERT, UPDATE ON ops.blocked_cooldown TO scraper_user;
GRANT INSERT ON staging.blocked_cooldown_events TO scraper_user;
