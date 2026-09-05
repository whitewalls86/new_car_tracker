SELECT listing_id, priority
FROM ops.ops_detail_scrape_queue
WHERE listing_id IN (%s::uuid, %s::uuid)
ORDER BY priority
