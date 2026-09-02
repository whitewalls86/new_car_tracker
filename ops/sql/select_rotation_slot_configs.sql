-- The configs the caller just claimed, read back in the order the scraper
-- should run them. Enabled is repeated here because mark_rotation_slot_queued
-- only stamped the enabled ones.
SELECT search_key, params
FROM search_configs
WHERE enabled = true AND rotation_slot = %s
ORDER BY rotation_order NULLS LAST, search_key
