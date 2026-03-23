-- detail_scrape_claims: tracks which listing_ids are currently being scraped.
-- status = 'running' while the owning run is active; set to 'completed' when done.
-- New runs overwrite completed claims via ON CONFLICT DO UPDATE.

CREATE TABLE IF NOT EXISTS detail_scrape_claims (
    listing_id   text        PRIMARY KEY,
    claimed_by   text        NOT NULL,   -- run_id (uuid as text)
    claimed_at   timestamptz NOT NULL DEFAULT now(),
    status       text        NOT NULL DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS ix_detail_scrape_claims_claimed_by
    ON detail_scrape_claims (claimed_by);
