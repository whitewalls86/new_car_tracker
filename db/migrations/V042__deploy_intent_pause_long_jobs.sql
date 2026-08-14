-- Plan 131 Stage 5 D3b: a deploy stops a long job at its next boundary.
--
-- A month-scale pack or prune runs for hours. Today a deploy landing mid-run
-- restarts the container underneath it, and an hour of in-flight work is lost
-- to something that looks like a crash rather than a deploy. The jobs now read
-- this flag at a boundary they already have, return cleanly, and resume on an
-- Airflow retry once the intent clears.
--
-- Default true, because the safe behaviour should be the one you get by
-- forgetting. A deploy that touches nothing these jobs depend on can pass
-- pause_long_jobs=false to POST /deploy/start.

ALTER TABLE deploy_intent
    ADD COLUMN IF NOT EXISTS pause_long_jobs BOOLEAN NOT NULL DEFAULT true;

-- The archiver reads deploy_intent as scraper_user (docker-compose.yml). That
-- already works, inherited from V003's blanket grant on schema public — naming
-- it here makes the dependency legible rather than accidental, and keeps this
-- migration self-contained if the blanket grant is ever narrowed.
GRANT SELECT ON deploy_intent TO scraper_user;
