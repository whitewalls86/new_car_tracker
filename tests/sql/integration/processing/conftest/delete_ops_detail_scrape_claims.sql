DELETE FROM ops.detail_scrape_claims WHERE listing_id = ANY(%s::uuid[])
