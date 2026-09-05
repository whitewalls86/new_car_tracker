SELECT listing_id, stale_reason FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid
