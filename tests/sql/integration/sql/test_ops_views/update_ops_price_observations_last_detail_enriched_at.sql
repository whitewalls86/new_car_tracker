UPDATE ops.price_observations SET last_detail_enriched_at = now() WHERE listing_id = %s::uuid
