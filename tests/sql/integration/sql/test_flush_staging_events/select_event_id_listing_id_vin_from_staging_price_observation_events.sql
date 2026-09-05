SELECT event_id, listing_id, vin, price, make, model,
                      artifact_id, event_type, source, event_at
               FROM staging.price_observation_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.price_observation_events)
