SELECT artifact_id, event_type, source, event_at FROM staging.price_observation_events WHERE artifact_id = ANY(%s)
