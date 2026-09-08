UPDATE ops.machine_tokens
SET last_used_at = now() - interval '1 hour'
WHERE token_sha256 = %s
