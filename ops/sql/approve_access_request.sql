-- Close an access request as approved. notification_email is cleared in the
-- same statement: the address was stored only to send this one message, and the
-- caller has already read it out before this runs.
UPDATE access_requests
SET status = 'approved', resolved_at = now(), resolved_by = %s,
    notification_email = NULL
WHERE id = %s
