SELECT listing_id, source, fetched_at, artifact_id FROM staging.silver_observations WHERE artifact_id = %s ORDER BY source
