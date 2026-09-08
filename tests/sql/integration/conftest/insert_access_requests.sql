INSERT INTO access_requests (email_hash, requested_role, status)
VALUES (%s, 'viewer', 'pending')
RETURNING id
