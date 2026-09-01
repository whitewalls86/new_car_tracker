-- Change one user's role. The caller has already checked the role against
-- ops.routers.users.ROLE_LABELS, which is what keeps an arbitrary form value
-- out of the column.
UPDATE authorized_users SET role = %s WHERE id = %s
