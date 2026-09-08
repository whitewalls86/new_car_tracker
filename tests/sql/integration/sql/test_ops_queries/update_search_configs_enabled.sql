UPDATE search_configs
SET enabled = %s, params = %s::jsonb, rotation_order = %s,
    rotation_slot = %s, updated_at = now()
WHERE search_key = %s
