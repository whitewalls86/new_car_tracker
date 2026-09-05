SELECT artifact_id, listing_id, source, fetched_at FROM staging.silver_observations WHERE artifact_id = ANY(%s)
