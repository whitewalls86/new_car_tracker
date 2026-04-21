-- V026: Fix permissions for scraper_user
--
-- scraper_user connects from both the scraper service (queue inserts) and the
-- processing service (silver staging writes). V003 granted public-schema access
-- and read-only on ops; these grants cover the ops/staging tables added later.

-- Table grants (schema-qualified)
GRANT SELECT, INSERT, UPDATE, DELETE ON ops.artifacts_queue TO scraper_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON staging.artifacts_queue_events TO scraper_user;
GRANT SELECT, INSERT ON staging.silver_observations TO scraper_user;

-- Schema usage (USAGE only — scraper_user does not create objects)
GRANT USAGE ON SCHEMA ops TO scraper_user;
GRANT USAGE ON SCHEMA staging TO scraper_user;

-- Sequences (bigserial columns require USAGE + SELECT to generate next value)
GRANT USAGE, SELECT ON SEQUENCE ops.artifacts_queue_artifact_id_seq TO scraper_user;
GRANT USAGE, SELECT ON SEQUENCE staging.artifacts_queue_events_event_id_seq TO scraper_user;
GRANT USAGE, SELECT ON SEQUENCE staging.silver_observations_id_seq TO scraper_user;
