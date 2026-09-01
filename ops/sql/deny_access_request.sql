-- Close an access request as denied, clearing the stored address in the same
-- statement. Unlike approve_access_request.sql this keeps the 'pending'
-- predicate, so a repeated denial changes no already-resolved row.
UPDATE access_requests
SET status = 'denied', resolved_at = now(), resolved_by = %s,
    notification_email = NULL
WHERE id = %s AND status = 'pending'
