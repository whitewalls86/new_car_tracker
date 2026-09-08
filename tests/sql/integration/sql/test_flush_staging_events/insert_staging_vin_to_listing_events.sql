INSERT INTO staging.vin_to_listing_events
               (vin, listing_id, artifact_id, event_type, event_at)
           VALUES (%s, %s, 999999, 'mapped', %s)
           RETURNING event_id
