-- Fallback for configs that predate rotation slots: claim a single due config
-- that has no rotation_slot. FOR UPDATE SKIP LOCKED is the claim -- two callers
-- racing here take different rows rather than the same one, which the slot path
-- above gets from the slot-wide UPDATE instead.
SELECT search_key, params
FROM search_configs
WHERE enabled = true
  AND rotation_slot IS NULL
  AND (last_queued_at IS NULL
       OR last_queued_at < now() - make_interval(mins => %s))
ORDER BY rotation_order NULLS LAST, search_key
LIMIT 1
FOR UPDATE SKIP LOCKED
