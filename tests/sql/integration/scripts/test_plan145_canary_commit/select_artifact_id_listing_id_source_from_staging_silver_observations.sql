SELECT artifact_id, listing_id, source, listing_state, vin, fetched_at FROM staging.silver_observations WHERE artifact_id = ANY(%s) ORDER BY artifact_id, source
