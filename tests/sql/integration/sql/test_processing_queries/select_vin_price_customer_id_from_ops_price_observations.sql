SELECT vin, price, customer_id 
FROM ops.price_observations WHERE listing_id = %s::uuid
