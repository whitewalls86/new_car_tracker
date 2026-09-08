INSERT INTO search_configs
    (search_key, enabled, params, rotation_order, created_at, updated_at)
VALUES (%s, true, '{"makes": ["honda"], "models": ["honda-cr_v"]}'::jsonb, 1, now(), now())
