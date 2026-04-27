-- V027: Grant archiver (scraper_user role) access to staging tables it flushes.
--
-- The archiver service runs as the scraper_user DB user and needs USAGE on the
-- staging schema plus SELECT + DELETE on the tables it reads and flushes.

GRANT USAGE ON SCHEMA staging TO scraper_user;

-- silver observations
GRANT SELECT, DELETE ON staging.silver_observations TO scraper_user;

-- ops event tables
GRANT SELECT, DELETE ON staging.artifacts_queue_events    TO scraper_user;
GRANT SELECT, DELETE ON staging.detail_scrape_claim_events TO scraper_user;
GRANT SELECT, DELETE ON staging.blocked_cooldown_events   TO scraper_user;
GRANT SELECT, DELETE ON staging.price_observation_events  TO scraper_user;
GRANT SELECT, DELETE ON staging.vin_to_listing_events     TO scraper_user;
