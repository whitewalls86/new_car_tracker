-- Upsert a single price observation into the HOT table.
-- listing_id is PRIMARY KEY; vin has a partial unique index (WHERE NOT NULL).
--
-- customer_id: populated by detail writes only; COALESCE ensures a later SRP or
-- carousel write never overwrites an already-enriched customer_id with NULL.
--
-- last_detail_enriched_at: "we got full detail data" — set by detail writes
-- only (pass NULL from SRP/carousel). COALESCE preserves a previous value; a
-- non-NULL incoming value always wins. This is what drives the 7-day
-- is_full_details_stale window and the Plan 115 circuit breaker.
--
-- The statement writes the same bound value to the legacy
-- last_detail_scraped_at column for the length of the Plan 147 dual-write
-- release, so old and new code paths are both valid during the deploy. Binding
-- both columns to one parameter is deliberate: it makes the two physically
-- unable to disagree, which is what makes V049's drop of the legacy column
-- safe rather than hopeful. V049 deletes the two legacy lines below and
-- nothing else here changes.
--
-- Note this statement never touches last_detail_fetched_at. That column is the
-- scraper's, written by POST /scrape/claims/release; a processor that could
-- advance it would reintroduce the coupling Plan 147 removes.
INSERT INTO ops.price_observations
    (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id,
     last_detail_enriched_at, last_detail_scraped_at)
VALUES
    (%(listing_id)s, %(vin)s, %(price)s, %(make)s, %(model)s,
     %(customer_id)s, %(last_seen_at)s, %(last_artifact_id)s,
     %(last_detail_enriched_at)s, %(last_detail_enriched_at)s)
ON CONFLICT (listing_id) DO UPDATE SET
    vin                     = COALESCE(EXCLUDED.vin, ops.price_observations.vin),
    price                   = EXCLUDED.price,
    make                    = EXCLUDED.make,
    model                   = EXCLUDED.model,
    customer_id             = COALESCE(EXCLUDED.customer_id, ops.price_observations.customer_id),
    last_seen_at            = EXCLUDED.last_seen_at,
    last_artifact_id        = EXCLUDED.last_artifact_id,
    last_detail_enriched_at = COALESCE(
        EXCLUDED.last_detail_enriched_at,
        ops.price_observations.last_detail_enriched_at
    ),
    last_detail_scraped_at  = COALESCE(
        EXCLUDED.last_detail_scraped_at,
        ops.price_observations.last_detail_scraped_at
    )
