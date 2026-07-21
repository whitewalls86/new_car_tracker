-- Listing IDs that already have a 'cleared' event staged but not yet flushed to
-- the analytics store. Used to make reconciliation idempotent across re-runs
-- that happen before the staging->parquet flush and next mart build.
SELECT DISTINCT listing_id::text AS listing_id
FROM staging.blocked_cooldown_events
WHERE event_type = 'cleared'
