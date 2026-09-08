INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)
VALUES (%s, %s::uuid, now(), %s)
ON CONFLICT (vin) DO UPDATE SET
    listing_id  = EXCLUDED.listing_id,
    mapped_at   = EXCLUDED.mapped_at,
    artifact_id = EXCLUDED.artifact_id
