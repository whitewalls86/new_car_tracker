SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = ANY(%s::uuid[])
