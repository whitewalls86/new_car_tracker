-- Resolve a caller's role from the hash of their email address.
SELECT role FROM authorized_users WHERE email_hash = %s
