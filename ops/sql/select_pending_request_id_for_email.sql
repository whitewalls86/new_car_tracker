-- The duplicate guard on POST /request-access: any pending row for this email
-- hash means the submission is a repeat. Kept separate from
-- select_pending_request_for_email.sql, which reads status for display and
-- orders and limits for it; this one only needs existence.
SELECT id FROM access_requests WHERE email_hash = %s AND status = 'pending'
