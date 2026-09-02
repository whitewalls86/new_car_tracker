-- Plan 147's loop guard: record that a detail request was spent on each listing
-- the scraper attempted. It runs on the same cursor as
-- delete_detail_scrape_claims.sql, so a released claim and its recorded fetch
-- are never separated -- before this, the only thing stopping an immediate
-- re-fetch was a timestamp written by the processing service two hops away.
UPDATE ops.price_observations
   SET last_detail_fetched_at = now()
 WHERE listing_id = ANY(%s::uuid[])
