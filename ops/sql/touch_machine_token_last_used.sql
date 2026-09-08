-- Record that a credential was used, at most once per throttle window.
--
-- The age test is in the WHERE rather than in Python so the engine decides
-- whether to write: concurrent requests through one credential race to the same
-- row and only the first updates it. Reading last_used_at and then deciding
-- would have every one of them write, which is the per-request write this
-- column is throttled to avoid.
--
-- The window is bound as a parameter, not interpolated -- it comes from a
-- module constant in ops/routers/snapshots.py, and this file names no value of
-- its own so the two cannot drift.
UPDATE ops.machine_tokens
SET last_used_at = now()
WHERE token_sha256 = %s
  AND (last_used_at IS NULL OR last_used_at < now() - %s::interval)
