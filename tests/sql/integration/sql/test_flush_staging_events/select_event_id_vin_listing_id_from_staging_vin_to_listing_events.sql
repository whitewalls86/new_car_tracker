SELECT event_id, vin, listing_id, artifact_id,
                      event_type, previous_listing_id, event_at
               FROM staging.vin_to_listing_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.vin_to_listing_events)
