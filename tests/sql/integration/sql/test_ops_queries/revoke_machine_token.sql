UPDATE ops.machine_tokens SET revoked_at = now() WHERE token_sha256 = %s
