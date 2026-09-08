INSERT INTO ops.tracked_models (search_key, make, model)
VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING
