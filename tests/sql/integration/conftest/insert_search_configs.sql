INSERT INTO search_configs
    (search_key, enabled, params, rotation_order, created_at, updated_at)
VALUES (%s, true, '{"makes": ["test"]}'::jsonb, 1, now(), now())
