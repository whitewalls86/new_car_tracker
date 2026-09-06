SELECT price, last_detail_fetched_at, last_detail_enriched_at FROM ops.price_observations WHERE listing_id = %s::uuid
