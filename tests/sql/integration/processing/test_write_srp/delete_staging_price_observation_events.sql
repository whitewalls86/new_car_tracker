DELETE FROM staging.price_observation_events WHERE listing_id = ANY(%s::uuid[])
