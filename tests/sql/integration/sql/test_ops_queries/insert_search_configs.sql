INSERT INTO search_configs
    (search_key, enabled, params, rotation_order, rotation_slot, created_at, updated_at)
VALUES (%s, %s, %s::jsonb, %s, %s, now(), now())
