-- Record a price_observation mutation event (upsert or delete).
INSERT INTO staging.price_observation_events
    (listing_id, vin, price, make, model, artifact_id, event_type, source)
VALUES
    (%(listing_id)s, %(vin)s, %(price)s, %(make)s, %(model)s,
     %(artifact_id)s, %(event_type)s, %(source)s)
