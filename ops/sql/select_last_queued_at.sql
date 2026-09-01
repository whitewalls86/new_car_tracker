-- The rotation's global spacing guard: the most recent queue time across every
-- enabled search config, whatever slot it belongs to. Without it several slots
-- with stale timestamps would all come due at once and fire back to back.
SELECT MAX(last_queued_at)
FROM search_configs
WHERE enabled = true
