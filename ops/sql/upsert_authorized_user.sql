-- Authorize the requester. ON CONFLICT because an approval can re-grant an
-- email hash that already has a row; the role and the granting admin are
-- refreshed, and display_name is deliberately left as it was.
INSERT INTO authorized_users
    (email_hash, role, display_name, created_by)
VALUES (%s, %s, %s, %s)
ON CONFLICT (email_hash) DO UPDATE
    SET role = EXCLUDED.role,
        created_by = EXCLUDED.created_by
