SELECT price, customer_id, last_detail_enriched_at FROM ops.price_observations WHERE listing_id = %s::uuid
