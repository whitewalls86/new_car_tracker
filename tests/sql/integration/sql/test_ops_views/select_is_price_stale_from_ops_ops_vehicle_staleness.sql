SELECT is_price_stale, is_full_details_stale, stale_reason FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid
