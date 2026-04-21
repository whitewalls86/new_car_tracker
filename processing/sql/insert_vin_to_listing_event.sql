-- Record a vin_to_listing mutation event (mapped or remapped).
INSERT INTO staging.vin_to_listing_events
    (vin, listing_id, artifact_id, event_type, previous_listing_id)
VALUES
    (%(vin)s, %(listing_id)s, %(artifact_id)s, %(event_type)s, %(previous_listing_id)s)
