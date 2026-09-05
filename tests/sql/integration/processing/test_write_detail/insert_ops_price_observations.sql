INSERT INTO ops.price_observations (listing_id, vin, price, make, model, last_seen_at, last_artifact_id) VALUES (%s::uuid, %s, 25000, 'Honda', 'CR-V', now(), %s)
