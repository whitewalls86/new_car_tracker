UPDATE ops.price_observations SET last_detail_fetched_at = now() WHERE listing_id = %s::uuid
