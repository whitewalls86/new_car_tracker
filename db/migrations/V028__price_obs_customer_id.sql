-- V028: Add customer_id to ops.price_observations
--
-- customer_id is populated by detail-page processing and is the Cars.com dealer
-- identifier. Two purposes:
--
--   1. Dealer grouping in the scrape queue: PARTITION BY COALESCE(customer_id, vin)
--      limits the queue to one vehicle per dealer, maximising carousel hint reuse
--      (scraping vehicle A from dealer X yields carousel prices for B, C, D for free).
--
--   2. Enrichment flag: customer_id IS NULL means this listing has never been
--      detail-scraped, so full dealer info is missing. This replaces the old
--      'dealer_unenriched' signal that required joining to mart_vehicle_snapshot.
--
-- SRP and carousel writes leave customer_id NULL; only the detail write path
-- populates it. The upsert uses COALESCE so a later SRP or carousel write never
-- downgrades a row that already has a customer_id.

ALTER TABLE ops.price_observations ADD COLUMN customer_id text;
