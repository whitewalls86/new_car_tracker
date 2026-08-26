-- Historical recovery must preserve the original capture time.  The normal
-- event insert deliberately relies on the database default of now().
INSERT INTO staging.price_observation_events
    (listing_id, vin, price, make, model, artifact_id, event_type, source, event_at)
VALUES
    (%(listing_id)s, %(vin)s, %(price)s, %(make)s, %(model)s,
     %(artifact_id)s, %(event_type)s, %(source)s, %(event_at)s)
