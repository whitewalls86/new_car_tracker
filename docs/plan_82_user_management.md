# Plan 82: User Management — DB-backed Auth with Access Requests

**Status:** Planned
**Priority:** Medium — current ADMIN_PATTERN works but doesn't scale past 1-2 people

## Overview

Replace the regex-based `ADMIN_PATTERN` / `POWER_USER_PATTERN` env vars with a
database-backed user table. Caddy delegates all auth decisions to a new
`GET /auth/check` endpoint in the ops service, which hashes the incoming email
and looks it up. Users who aren't in the table can submit an access request;
admins approve/deny through the existing ops UI.

---

## Roles

| Role | Access |
|---|---|
| `admin` | Everything — deploy, dbt, search config edits, pgAdmin, n8n, minio |
| `power_user` | `/admin/searches`, `/admin/runs` — can edit configs and trigger runs |
| `observer` | All `/admin/*` pages read-only — can view but not mutate anything |
| `viewer` | Dashboard only |

`observer` is the portfolio-safe role: shows off the ops UI without giving
control over the live pipeline.

---

## Security Design

### Why hash emails?

Emails in `authorized_users` are stored as SHA-256 hashes (lowercase, UTF-8).
This prevents casual DB read access (compromised viewer credential, SQL
injection) from revealing who has privileged access. The hash is deterministic
so the auth check can hash the incoming header and compare without reversing it.

### Why not bcrypt/argon2?

Those are designed for high-entropy secrets and are intentionally slow. The
threat here is DB read access / enumeration, not offline brute force of a
leaked hash. SHA-256 + a fixed app-level salt (stored in env, not in the DB)
is a reasonable trade-off.

**Salt strategy:** A single `AUTH_EMAIL_SALT` env var, not per-row.
- Prevents bulk lookups against a leaked DB
- The salt is in the container environment, not the DB
- Admin must know the salt to bootstrap or audit users

### Auth check endpoint security

`/auth/check` is only reachable internally (Caddy → ops on `cartracker-net`).
It is not exposed through any public Caddy route. Returns:
- `200` + `X-User-Role: admin|power_user|observer|viewer` if authorized
- `403` if the email is not in the table

---

## Data Model

### Migration V009: authorized_users + access_requests

```sql
CREATE TABLE authorized_users (
    id           SERIAL PRIMARY KEY,
    email_hash   TEXT NOT NULL UNIQUE,  -- SHA-256(salt + lowercase_email)
    role         TEXT NOT NULL CHECK (role IN ('admin', 'power_user', 'observer', 'viewer')),
    display_name TEXT,                  -- human-readable label for the UI
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by   TEXT                   -- email_hash of the admin who approved
);

CREATE TABLE access_requests (
    id             SERIAL PRIMARY KEY,
    email_hash     TEXT NOT NULL,
    requested_role TEXT NOT NULL DEFAULT 'viewer'
                       CHECK (requested_role IN ('power_user', 'observer', 'viewer')),
    -- 'admin' cannot be self-requested; must be manually inserted
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status         TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'approved', 'denied')),
    resolved_at    TIMESTAMPTZ,
    resolved_by    TEXT                -- email_hash of admin who acted
);

CREATE INDEX ON access_requests (status) WHERE status = 'pending';
```

### Bootstrap

Initial admin cannot be self-requested (no admins exist yet to approve).
Seeded via Flyway placeholders in V009:

```sql
INSERT INTO authorized_users (email_hash, role, display_name)
VALUES (
    encode(digest('${authEmailSalt}' || lower('${adminEmail}'), 'sha256'), 'hex'),
    'admin',
    'Initial admin'
)
ON CONFLICT DO NOTHING;
```

Add `AUTH_EMAIL_SALT` and `ADMIN_EMAIL` as Flyway placeholders in
`docker-compose.yml` alongside the existing password placeholders.

---

## Components

### 1. Flyway migration V009

- Creates `authorized_users` and `access_requests` in `public` schema
- Seeds the initial admin row via placeholder
- `scraper_user` and `dbt_user` get no access to these tables
- `cartracker` (ops service) owns and has full access

### 2. Ops: `/auth/check` endpoint

New router `ops/routers/auth.py`:

```
GET /auth/check
  Headers in:  X-Auth-Request-Email
  Logic:       hash = SHA-256(AUTH_EMAIL_SALT + email.lower())
               look up authorized_users WHERE email_hash = hash
  Returns:     200 + X-User-Role header  (if found)
               403                       (if not found)
```

Internal-only — not exposed through any public Caddy route.

### 3. Ops: read-only enforcement for observer role

Caddy forwards `X-User-Role` to ops on every request. Ops middleware checks
the header on all mutating routes (POST/DELETE/PATCH):
- `admin` / `power_user` — allowed (as today)
- `observer` — 403 on mutations; all GET/view routes pass through normally
- Action buttons in templates are hidden when `X-User-Role: observer`

This requires no separate UI — same pages, same URLs, buttons conditionally
rendered.

### 4. Ops: access request flow

**`GET /request-access`** — public page shown when a logged-in Google user gets
403 from Caddy. Shows a form: display name (optional), requested role (viewer,
observer, or power_user), brief reason. `X-Auth-Request-Email` header is
already present so the user doesn't type their email.

**`POST /request-access`** — inserts into `access_requests`. Sends a Telegram
notification to admin. Returns a "request received" confirmation page.

**`GET /admin/users`** — lists `authorized_users` with role badges. Admin can
change roles or revoke.

**`GET /admin/access-requests`** — lists pending requests. Approve inserts into
`authorized_users` and updates status. Deny just updates status.

### 5. Caddyfile changes

Replace `header_regexp` pattern checks with a secondary `forward_auth` to
`/auth/check`. The role header returned is used to gate each tier:

```caddy
# Shared snippet — call after oauth2 forward_auth passes
forward_auth ops:8060 {
    uri /auth/check
    copy_headers X-User-Role
    @unauthorized status 403
    handle_response @unauthorized {
        redir * /request-access
    }
}

# Admin-only block example:
@not_admin {
    not header X-User-Role admin
}
respond @not_admin 403

# Observer+ block example (admin, power_user, observer all pass):
@not_at_least_observer {
    not header_regexp X-User-Role ^(admin|power_user|observer)$
}
respond @not_at_least_observer 403
```

### 6. pgAdmin SSO (no second login screen)

While touching Caddy and ops, wire header-based auth for pgAdmin:

docker-compose.yml additions to pgadmin service:
```yaml
PGADMIN_CONFIG_AUTHENTICATION_SOURCES: "['webserver']"
PGADMIN_CONFIG_WEBSERVER_REMOTE_USER: "X-Auth-Request-Email"
PGADMIN_CONFIG_WEBSERVER_AUTO_CREATE_USER: "True"
```

Caddyfile pgadmin reverse_proxy block addition:
```caddy
reverse_proxy pgadmin:80 {
    header_up X-Auth-Request-Email {http.request.header.X-Auth-Request-Email}
}
```

pgAdmin role promotion (admin → pgAdmin Administrator) still done manually via
pgAdmin User Management UI — no API for this.

### 7. n8n

Disable n8n's own auth layer — Caddy is already the sole gatekeeper, and the
second login adds nothing. For read-only portfolio access to n8n workflows, see
Plan 83.

docker-compose.yml addition to n8n service:
```yaml
- N8N_USER_MANAGEMENT_DISABLED=true
```

### 8. MinIO OIDC SSO (no second login screen)

MinIO supports OpenID Connect natively. Point it directly at Google's OIDC
endpoint using the existing `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` — the
same Google account that passed Caddy will be used to log into the MinIO
console automatically.

docker-compose.yml additions to minio service:
```yaml
MINIO_IDENTITY_OPENID_CONFIG_URL: "https://accounts.google.com/.well-known/openid-configuration"
MINIO_IDENTITY_OPENID_CLIENT_ID: ${GOOGLE_CLIENT_ID}
MINIO_IDENTITY_OPENID_CLIENT_SECRET: ${GOOGLE_CLIENT_SECRET}
MINIO_IDENTITY_OPENID_REDIRECT_URI: "https://cartracker.info/minio/oauth_callback"
MINIO_IDENTITY_OPENID_CLAIM_NAME: "email"
MINIO_IDENTITY_OPENID_SCOPES: "openid,email,profile"
```

After enabling OIDC, MinIO users who log in via Google get a temporary STS
session. Permissions are controlled by MinIO policies attached to the OIDC
claim value (the email). The admin email should be mapped to a MinIO policy
granting full console access. This is done once via `mc` CLI after first login:

```bash
mc admin policy attach local readwrite --user <admin-email>
```

MinIO still has its own root credentials (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)
as a break-glass fallback — these are unchanged.

---

## New environment variables

| Variable | Where | Purpose |
|---|---|---|
| `AUTH_EMAIL_SALT` | `.env` + Flyway placeholder | Salt for email hashing |
| `ADMIN_EMAIL` | `.env` + Flyway placeholder | Bootstrap admin (used once in V009) |

`ADMIN_PATTERN` and `POWER_USER_PATTERN` removed from `.env` and docker-compose
once the new auth is live.

---

## Rollout Order

1. Add `AUTH_EMAIL_SALT` + `ADMIN_EMAIL` to `.env` and docker-compose Flyway placeholders
2. Write V009 migration
3. Add `/auth/check` to ops
4. Add observer middleware to ops (read-only enforcement)
5. Add `/request-access`, `/admin/users`, `/admin/access-requests` to ops
6. Update Caddyfile
7. Wire pgAdmin header auth + disable n8n basic auth
8. Configure MinIO OIDC env vars; attach admin policy via `mc` after first login
9. Remove `ADMIN_PATTERN` / `POWER_USER_PATTERN`
10. `docker compose build ops && docker compose up -d`

---

## What stays the same

- Google OAuth via oauth2-proxy is still the authentication layer (who you are)
- This plan only changes authorization (what you're allowed to do)
- `cartracker-net` isolation means `/auth/check` is never publicly reachable
