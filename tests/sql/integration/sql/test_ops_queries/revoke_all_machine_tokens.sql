UPDATE ops.machine_tokens SET revoked_at = now() WHERE revoked_at IS NULL
