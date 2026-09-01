-- Record one access request. notification_email is NULL unless the requester
-- opted in to being told the outcome; the approve and deny statements clear it
-- again once the mail has been sent.
INSERT INTO access_requests
    (email_hash, requested_role, display_name, notification_email)
VALUES (%s, %s, %s, %s)
