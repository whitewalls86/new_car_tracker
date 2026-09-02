-- Everything approval needs from one pending request: the identity to authorize
-- and the address to notify. The status predicate is the idempotency guard --
-- a second approval of the same request reads nothing and is a no-op.
SELECT email_hash, requested_role, display_name, notification_email
FROM access_requests WHERE id = %s AND status = 'pending'
