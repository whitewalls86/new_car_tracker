DELETE FROM ops.price_observations WHERE listing_id = ANY(%s::uuid[])
