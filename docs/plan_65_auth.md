# Plan 65: Authentication & Authorization Stack

**Status:** Not started
**Priority:** Medium (low risk while local; required before any public deployment)

Currently all services are protected only by Docker network isolation. Port 8000 (admin), 8501 (dashboard), and 5050 (pgAdmin) are open to anyone who can reach the host. No authentication, no user tiers, no database-level permission separation.

## Target architecture

```
Internet
  → Caddy (HTTPS, Let's Encrypt, reverse proxy)
    → Authelia (authentication + coarse authorization)
      → Google OAuth (identity provider — no passwords to manage)
        → Services (admin, dashboard, pgAdmin)
          → Postgres (role-based permissions per service)
```

## Layer 1 — HTTPS (Caddy)
Caddy sits in front of everything as a reverse proxy and handles TLS automatically via Let's Encrypt. No certificate management required. All traffic encrypted in transit.

## Layer 2 — Authentication (Authelia + Google OAuth)
Authelia is a self-hosted SSO server. It intercepts all requests and redirects unauthenticated users to a login page. Authentication is delegated to Google OAuth — users log in with their Google account, no passwords stored anywhere in the project.

- Single login session works across all services (true SSO)
- No password management, no credential storage
- Google handles identity verification

## Layer 3 — Coarse authorization (Authelia rules)
Authelia enforces URL-level access tiers via group membership:

| Group | Access |
|-------|--------|
| `viewer` | `/dashboard` only |
| `power_user` | `/dashboard`, `/admin/searches/`, `/admin/runs` |
| `admin` | All routes including `/admin/dbt`, `/pgadmin` |

Group membership managed in Authelia config. Consumer Google accounts don't support group claims, so groups are managed locally in Authelia and mapped to Google email addresses.

## Layer 4 — Database permissions (Postgres roles)
Each service connects to Postgres (via PgBouncer — Plan 64) with a scoped role rather than a single superuser:

| Role | Permissions | Used by |
|------|------------|---------|
| `scraper_user` | Write to raw tables, read search_configs | scraper |
| `dbt_user` | Write to analytics schema | dbt, dbt_runner |
| `viewer` | SELECT on analytics schema only | dashboard |
| `cartracker_admin` | Full access | admin UI, migrations |

## New containers
- `caddy` — reverse proxy + TLS
- `authelia` — SSO server
- `authelia_redis` — session storage (Authelia requires Redis)

## Notes
- Google OAuth requires registering an app in Google Cloud Console (free) to get client ID + secret
- Consumer Google accounts don't support group claims — Authelia manages groups locally, mapped by email
- Postgres role migration needs to be coordinated with Plan 63 (schema migrations) and Plan 64 (PgBouncer)
- Plans 65 and 66 (SQL injection audit) should both be complete before exposing any port publicly
