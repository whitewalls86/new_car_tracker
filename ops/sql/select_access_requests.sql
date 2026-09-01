-- The admin access-request queue. Pending rows sort first whatever their age,
-- then everything resolved, newest first -- the CASE is the queue, not a
-- tiebreak on the timestamp.
SELECT id, email_hash, display_name, requested_role, requested_at, status,
       resolved_at, resolved_by
FROM access_requests
ORDER BY
  CASE status WHEN 'pending' THEN 0 ELSE 1 END,
  requested_at DESC
