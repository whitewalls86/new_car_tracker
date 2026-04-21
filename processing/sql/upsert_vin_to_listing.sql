-- Upsert VIN → listing mapping. Recency guard: only update if new mapped_at is newer.
INSERT INTO ops.vin_to_listing
    (vin, listing_id, mapped_at, artifact_id)
VALUES
    (%(vin)s, %(listing_id)s, %(mapped_at)s, %(artifact_id)s)
ON CONFLICT (vin) DO UPDATE SET
    listing_id  = EXCLUDED.listing_id,
    mapped_at   = EXCLUDED.mapped_at,
    artifact_id = EXCLUDED.artifact_id
WHERE EXCLUDED.mapped_at > ops.vin_to_listing.mapped_at
