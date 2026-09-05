INSERT INTO ops.price_observations
    (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
