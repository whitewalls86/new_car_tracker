INSERT INTO ops.price_observations
    (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (listing_id) DO UPDATE SET
    price            = EXCLUDED.price,
    customer_id      = COALESCE(EXCLUDED.customer_id, 
                                ops.price_observations.customer_id),
    last_seen_at     = EXCLUDED.last_seen_at,
    last_artifact_id = EXCLUDED.last_artifact_id
