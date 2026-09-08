INSERT INTO access_requests (email_hash, requested_role, notification_email, requested_at)
VALUES
    ('test-stale-hash', 'viewer', 'stale@example.com', now() - interval '3 days'),
    ('test-recent-hash', 'viewer', 'recent@example.com', now())
RETURNING id, email_hash
