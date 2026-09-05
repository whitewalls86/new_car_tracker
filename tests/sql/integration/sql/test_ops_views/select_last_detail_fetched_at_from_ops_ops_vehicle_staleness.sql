SELECT last_detail_fetched_at, last_detail_enriched_at FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid
