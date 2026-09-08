INSERT INTO staging.silver_observations
               (artifact_id, listing_id, source, listing_state, fetched_at)
           VALUES (999999, 'listing-smoke-test', %s, 'active', %s)
           RETURNING id
