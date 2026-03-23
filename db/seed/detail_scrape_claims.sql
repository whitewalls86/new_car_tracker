-- detail_scrape_claims: tracks which listing_ids are currently being scraped.
-- A claim is "active" as long as its associated run is still in 'running' status.
-- Claims are never explicitly deleted — they become inactive when the run finishes.
-- A 6-hour safety fallback prevents truly crashed runs from blocking forever.

CREATE TABLE IF NOT EXISTS detail_scrape_claims (
    listing_id   text        PRIMARY KEY,
    claimed_by   text        NOT NULL,   -- run_id (uuid as text)
    claimed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_detail_scrape_claims_claimed_by
    ON detail_scrape_claims (claimed_by);
