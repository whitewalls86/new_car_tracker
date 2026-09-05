UPDATE ops.price_observations SET last_detail_fetched_at = now() - interval '7 hours' WHERE listing_id = %s::uuid
