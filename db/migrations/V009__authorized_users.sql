-- Plan 82: DB-backed authorization
-- Creates authorized_users and access_requests tables in the public schema.
-- The cartracker superuser owns these tables; scraper_user and dbt_user get no access.
-- Bootstrap: seeds the initial admin via Flyway placeholders.

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS authorized_users (
    id           SERIAL PRIMARY KEY,
    email_hash   TEXT NOT NULL UNIQUE,  -- SHA-256(AUTH_EMAIL_SALT || lowercase_email)
    role         TEXT NOT NULL CHECK (role IN ('admin', 'power_user', 'observer', 'viewer')),
    display_name TEXT,                  -- human-readable label for the UI
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by   TEXT                   -- email_hash of the admin who approved
);

CREATE TABLE IF NOT EXISTS access_requests (
    id             SERIAL PRIMARY KEY,
    email_hash     TEXT NOT NULL,
    requested_role TEXT NOT NULL DEFAULT 'viewer'
                       CHECK (requested_role IN ('power_user', 'observer', 'viewer')),
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status         TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'approved', 'denied')),
    resolved_at    TIMESTAMPTZ,
    resolved_by    TEXT                -- email_hash of admin who acted
);

CREATE INDEX IF NOT EXISTS access_requests_pending_idx
    ON access_requests (status) WHERE status = 'pending';

-- ── Permissions ───────────────────────────────────────────────────────────────
-- Only cartracker (ops service) accesses these tables.
-- scraper_user and dbt_user explicitly excluded (no GRANT).

-- ── Bootstrap admin ──────────────────────────────────────────────────────────
-- Initial admin cannot self-request (no admins exist yet). Seeded once via placeholders.

INSERT INTO authorized_users (email_hash, role, display_name)
VALUES (
    encode(digest('${authEmailSalt}' || lower('${adminEmail}'), 'sha256'), 'hex'),
    'admin',
    'Initial admin'
)
ON CONFLICT DO NOTHING;
