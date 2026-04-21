-- Get active search configs for carousel make/model filtering.
-- Returns the makes and models arrays from params jsonb.
SELECT
    search_key,
    params -> 'makes' AS makes,
    params -> 'models' AS models
FROM public.search_configs
WHERE enabled = true
