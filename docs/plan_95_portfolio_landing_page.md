# Plan 95: Portfolio Landing Page

**Status:** Planned
**Priority:** Unprioritized

## Overview

Replace the current `/info` page (which renders `README.md` as-is) with a
purpose-built landing page that sells the project to a hiring manager in 30
seconds. The README is a technical reference; the landing page is a pitch.

---

## Current State

`ops/routers/info.py` reads `README.md` at request time and injects it into a
page via `marked.js`. The result is a long, dense reference document — not
something a recruiter or hiring manager will read. The page has the right
infrastructure (public, no auth, already linked from the sidebar) but the wrong
content.

---

## What to Build

### Structure

A single bespoke HTML page (rendered by the info router, no new routes) with
five sections:

**1. Hero**
One paragraph: what CarTracker is, why it's technically interesting. Not a
feature list — a positioning statement. Something like:

> CarTracker scrapes Cars.com every 15 minutes across dozens of make/model
> pairs, tracking real-time pricing across 10 production services. Built to
> demonstrate how a production data pipeline actually works — scraping,
> orchestration, transformation, and delivery — not just the happy path.

**2. Live stats** *(single DB query on page load)*

A row of 3–4 numbers pulled fresh from the database:

| Stat | Query |
|---|---|
| VINs tracked | `COUNT(*) FROM mart_vehicle_snapshot` |
| Price observations | `COUNT(*) FROM price_observations` (or `stg_srp_observations`) |
| Last pipeline run | `MAX(created_at) FROM runs WHERE status='complete'` |
| Listings scraped today | count from `raw_artifacts` where `created_at >= today` |

These signal that the system is real and running, not a mock. If any query
fails, silently omit that stat (no error banner on the landing page).

**3. Architecture at a glance**

A compact two-column grid of the services — name, one-line description, and
a small badge for the technology (FastAPI, Streamlit, dbt, etc.). Not the full
services table from the README — just enough to convey scale.

**4. Technical highlights**

4–5 callout cards, each covering one interesting decision:

- **Fingerprint impersonation** — curl_cffi with Chrome TLS fingerprinting +
  FlareSolverr for Cloudflare JS challenges; process-wide credential cache with
  25-min TTL and automatic re-bootstrap on 403
- **Atomic claim pattern** — parallel detail scrapes use `ON CONFLICT DO UPDATE`
  against `detail_scrape_claims` to prevent duplicate work without a queue
  service
- **Exponential backoff cooldown** — 403'd listings tracked in
  `blocked_cooldown`; all backoff logic lives in `stg_blocked_cooldown` dbt
  model so it's queryable and testable without touching application code
- **Safe redeploy** — a `deploy_intent` DB flag lets all 7 primary workflows
  drain gracefully before a redeploy; the ops UI controls the flag and the
  `redeploy.sh` script automates the sequence
- **Role-based auth without an auth service** — email hashes + roles in
  Postgres; Caddy calls `GET /auth/check` as a forward_auth step on every
  protected route; no session tokens stored

**5. Links**

A clean set of CTAs:
- Request Access → `/request-access`
- View Dashboard → `/dashboard` (requires auth)
- Ops Admin UI → `/admin` (requires auth)
- GitHub → external repo link

---

## Implementation Notes

### Template approach

Replace the current inline HTML string in `info.py` with a Jinja2 template at
`ops/templates/info.html`. The ops app already uses Jinja2 templates for other
routes — just add a `Jinja2Templates` instance if one isn't already present in
the info router.

The template receives a `stats` dict from the router. If the DB query fails,
`stats` is an empty dict and the stats section is omitted via a template
conditional.

### Styling

Keep PicoCSS (already used by the current info page). Add a small inline
`<style>` block for the grid layout and highlight cards — no build step, no
external CSS file.

### README stays as-is

The README remains the technical reference. The landing page replaces the
`/info` route's content, not the README itself. The two serve different
audiences.

---

## What Changes

| File | Change |
|---|---|
| `ops/routers/info.py` | Replace marked.js README dump with Jinja2 template render + stats DB query |
| `ops/templates/info.html` | New template: hero, stats, services grid, highlights, links |
| Nothing else | No new routes, no Caddyfile changes, no new env vars |

---

## Rollout Order

1. Write `ops/templates/info.html`
2. Update `ops/routers/info.py` to render the template with a stats query
3. `docker compose build ops && docker compose up -d ops`
4. View `/info` — verify stats populate and layout looks right on desktop and
   mobile
