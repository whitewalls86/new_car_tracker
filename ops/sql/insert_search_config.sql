-- Create a search config from the admin UI.
-- params is cast to jsonb explicitly; the caller passes a JSON string.
INSERT INTO search_configs (
    search_key,
    enabled,
    params,
    rotation_order,
    rotation_slot,
    created_at,
    updated_at
)
VALUES (%s, %s, %s::jsonb, %s, %s, now(), now())
