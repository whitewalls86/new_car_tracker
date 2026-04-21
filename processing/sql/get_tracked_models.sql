-- Get the set of (make, model) pairs currently tracked by enabled search configs.
-- Used for carousel hint filtering at detail processing time.
SELECT DISTINCT tm.make, tm.model
FROM ops.tracked_models tm
JOIN public.search_configs sc
    ON sc.search_key = tm.search_key
    AND sc.enabled = true
