SELECT artifact_id, listing_id, event_type, event_at FROM staging.price_observation_events WHERE artifact_id = ANY(%s)
