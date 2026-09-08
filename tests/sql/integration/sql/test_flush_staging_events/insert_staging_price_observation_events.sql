INSERT INTO staging.price_observation_events
               (listing_id, artifact_id, event_type, source, event_at)
           VALUES (%s, 999999, 'upserted', 'srp', %s)
           RETURNING event_id
