INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)
VALUES (%s, %s::uuid, now(), %s)
