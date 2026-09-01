-- The address to notify on denial, read before deny_access_request.sql clears
-- it. Narrower than select_pending_request_details.sql because a denial grants
-- nothing and so needs no identity or role.
SELECT notification_email FROM access_requests
 WHERE id = %s AND status = 'pending'
