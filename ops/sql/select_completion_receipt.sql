-- Idempotent re-completion. A caller that already completed this generation
-- against this manifest gets its receipt back instead of a conflict, which is
-- what makes /coordination/complete safe to retry after a dropped response.
SELECT generation FROM coordination_completion_receipts
 WHERE generation = %s AND manifest_sha256 = %s
