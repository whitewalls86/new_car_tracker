-- One search config for the admin detail view.
-- Narrower than select_search_configs.sql: no created_at/updated_at, which the
-- single-config view does not render.
SELECT search_key, enabled, source, params, rotation_order, last_queued_at
FROM search_configs
WHERE search_key = %s
