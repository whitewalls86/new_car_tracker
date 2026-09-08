INSERT INTO staging.silver_observations (artifact_id, listing_id, source, listing_state, fetched_at) VALUES (%s, %s, 'carousel', 'active', %s) RETURNING id
