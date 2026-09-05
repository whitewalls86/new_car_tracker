INSERT INTO authorized_users (email_hash, role, display_name)
VALUES (%s, 'admin', 'Test Admin')
RETURNING id
