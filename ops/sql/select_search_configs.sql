-- Every search config, in the order the admin UI lists them: enabled first,
-- then rotation order, then key. NULLS LAST keeps unslotted configs at the
-- bottom rather than the top, which is where Postgres would put them by
-- default for DESC.
SELECT
    search_key,
    enabled,
    source,
    params,
    rotation_order,
    last_queued_at,
    created_at,
    updated_at
FROM search_configs
ORDER BY enabled DESC, rotation_order NULLS LAST, search_key
