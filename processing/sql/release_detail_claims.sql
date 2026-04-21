-- Release a detail scrape claim by deleting it from the HOT table.
DELETE FROM ops.detail_scrape_claims
WHERE listing_id = %(listing_id)s
