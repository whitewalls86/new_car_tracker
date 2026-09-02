-- Does this email hash already have an access request waiting? Read by the
-- public /request-access form so a repeat visitor sees "pending" rather than a
-- blank form. Only the status is needed, and only the newest one.
SELECT status FROM access_requests
 WHERE email_hash = %s AND status = 'pending'
 ORDER BY requested_at DESC LIMIT 1
