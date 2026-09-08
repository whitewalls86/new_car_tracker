-- Resolve a machine caller from the SHA-256 of the bearer token they presented.
--
-- One indexed probe against a unique key, not a scan. That is what hashing
-- bought: the environment-variable form had to compare every entry with no
-- early exit, because stopping at the first match leaked which caller called
-- through response time. There is no per-entry comparison here to leak it.
--
-- Returns the row whatever its lifecycle state, and reports the two refusals
-- rather than filtering them out, so the service can log "this credential is
-- revoked" and "this credential expired" as different events from "no such
-- token". The caller gets the same 403 for all three -- the distinction is for
-- whoever is reading the log six months from now.
--
-- Both verdicts are computed here so expiry is decided against one clock. The
-- ops container and Postgres are separate containers, and a Python-side
-- comparison would silently mix theirs.
SELECT
    name,
    scope,
    revoked_at IS NOT NULL AS is_revoked,
    (expires_at IS NOT NULL AND expires_at <= now()) AS is_expired
FROM ops.machine_tokens
WHERE token_sha256 = %s
