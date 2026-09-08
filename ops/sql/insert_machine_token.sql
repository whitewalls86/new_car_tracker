-- Issue a machine credential.
--
-- The plaintext never reaches this statement. scripts/issue_machine_token.py
-- generates the token, hashes it, passes the digest, and prints the token once
-- to stdout -- so the only copy that ever existed is the one on the operator's
-- screen.
INSERT INTO ops.machine_tokens (name, scope, token_sha256, created_by, expires_at)
VALUES (%s, %s, %s, %s, %s)
RETURNING id, name, scope, created_at, expires_at
