-- The next rotation slot due for scraping, oldest first. A slot is due only
-- when every config in it has been idle for min_idle_minutes, which is why the
-- interval predicate sits in WHERE and the ordering is on MIN(): a slot is the
-- unit of work, and one fresh config in it holds the whole slot back.
-- COALESCE puts a never-queued config at the front of the ordering.
SELECT rotation_slot
FROM search_configs
WHERE enabled = true
  AND rotation_slot IS NOT NULL
  AND (last_queued_at IS NULL
       OR last_queued_at < now() - make_interval(mins => %s))
GROUP BY rotation_slot
ORDER BY MIN(COALESCE(last_queued_at, '1970-01-01'::timestamptz)), rotation_slot
LIMIT 1
