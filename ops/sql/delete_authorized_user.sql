-- Revoke access by removing the row. There is no soft delete here: the
-- authorization check reads this table directly, so absence is the revocation.
DELETE FROM authorized_users WHERE id = %s
