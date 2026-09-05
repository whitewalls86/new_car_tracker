INSERT INTO search_configs (
    search_key, params, enabled, rotation_slot, last_queued_at
)
VALUES (
    %s,
    '{"makes": ["honda"], "models": ["cr-v"], "scopes": ["national"]}'::jsonb,
    %s,
    %s,
    %s
)
