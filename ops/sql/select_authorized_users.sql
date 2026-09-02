-- The admin user list. Grouped by role first so the table reads as a hierarchy
-- rather than as an arrival order.
SELECT id, email_hash, role, display_name, created_at
FROM authorized_users
ORDER BY role, created_at
