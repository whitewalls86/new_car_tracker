-- Upsert a single price observation into the HOT table.
-- listing_id is PRIMARY KEY; vin has a partial unique index (WHERE NOT NULL).
--
-- customer_id: populated by detail writes only; COALESCE ensures a later SRP or
-- carousel write never overwrites an already-enriched customer_id with NULL.
INSERT INTO ops.price_observations
    (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
VALUES
    (%(listing_id)s, %(vin)s, %(price)s, %(make)s, %(model)s,
     %(customer_id)s, %(last_seen_at)s, %(last_artifact_id)s)
ON CONFLICT (listing_id) DO UPDATE SET
    vin              = COALESCE(EXCLUDED.vin, ops.price_observations.vin),
    price            = EXCLUDED.price,
    make             = EXCLUDED.make,
    model            = EXCLUDED.model,
    customer_id      = COALESCE(EXCLUDED.customer_id, ops.price_observations.customer_id),
    last_seen_at     = EXCLUDED.last_seen_at,
    last_artifact_id = EXCLUDED.last_artifact_id
