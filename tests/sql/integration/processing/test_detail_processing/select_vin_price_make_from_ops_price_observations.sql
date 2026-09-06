SELECT vin, price, make, model FROM ops.price_observations WHERE listing_id = %s::uuid
