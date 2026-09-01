-- Release a batch's claims. Keyed by claimed_by as well as listing_id, so a run
-- can only release the claims it actually holds -- a late release from a run
-- whose claims were already re-claimed by someone else deletes nothing.
DELETE FROM detail_scrape_claims
 WHERE listing_id = ANY(%s::uuid[]) AND claimed_by = %s
