-- Claim a whole rotation slot by stamping every enabled config in it. This runs
-- before the configs are read back, so a concurrent caller selecting the next
-- due slot can no longer see this one.
UPDATE search_configs
SET last_queued_at = now()
WHERE enabled = true AND rotation_slot = %s
