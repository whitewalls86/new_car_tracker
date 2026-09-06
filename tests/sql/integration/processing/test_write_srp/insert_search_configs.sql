INSERT INTO search_configs
    (search_key, enabled, params, rotation_order, created_at, updated_at)
VALUES (%s, true, '{}'::jsonb, 99, now(), now())
