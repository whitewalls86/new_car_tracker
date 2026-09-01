-- Claim the next batch of detail listings in one round trip.
--
-- The anti-join in `batch` skips anything already running; `claimed` inserts
-- the claims and ON CONFLICT DO UPDATE re-claims a stale row, guarded by
-- `WHERE detail_scrape_claims.status != 'running'` so a live claim is never
-- stolen. The final join is what makes the whole thing safe under concurrency:
-- only rows the INSERT actually returned are handed to the caller, so two
-- racing callers cannot both be given the same listing.
--
-- `SELECT q.*` is deliberate -- the caller builds its response from
-- cur.description, so a column added to the queue view reaches the scraper
-- without a change here.
WITH batch AS (
    SELECT q.*
    FROM ops.ops_detail_scrape_queue q
    LEFT JOIN detail_scrape_claims c
        ON c.listing_id = q.listing_id
       AND c.status = 'running'
    WHERE c.listing_id IS NULL
    ORDER BY q.priority, q.listing_id
    LIMIT %s
),
claimed AS (
    INSERT INTO detail_scrape_claims
        (listing_id, claimed_by, claimed_at, status)
    SELECT b.listing_id, %s, now(), 'running'
    FROM batch b
    ON CONFLICT (listing_id) DO UPDATE
        SET claimed_by = EXCLUDED.claimed_by,
            claimed_at = EXCLUDED.claimed_at,
            status     = 'running'
        WHERE detail_scrape_claims.status != 'running'
    RETURNING listing_id
)
SELECT b.* FROM batch b
JOIN claimed c ON c.listing_id = b.listing_id
