DELETE FROM staging.price_observation_events WHERE artifact_id = ANY(%s)
