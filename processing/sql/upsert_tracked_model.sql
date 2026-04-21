-- Upsert a (search_key, make, model) row into the tracked_models HOT table.
-- No-op on conflict — presence is all that matters.
INSERT INTO ops.tracked_models (search_key, make, model)
VALUES (%(search_key)s, %(make)s, %(model)s)
ON CONFLICT (search_key, make, model) DO NOTHING
