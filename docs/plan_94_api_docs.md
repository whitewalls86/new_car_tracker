# Plan 94: API Documentation Hub

**Status:** Planned
**Priority:** Unprioritized

## Overview

Expose the auto-generated Swagger UI for all four FastAPI services at clean,
authenticated paths via Caddy. Add an index page at `/api-docs` that links to
each service's docs. Useful for navigating internal APIs without grepping source
files, and a concrete portfolio artifact showing API surface area.

---

## Current State

All four FastAPI services generate `/docs` (Swagger UI) and `/openapi.json`
automatically, but none are reachable from outside Docker. The services are
internal-only:

| Service | Internal address | Port |
|---------|-----------------|------|
| scraper | `scraper:8000` | 8000 |
| ops | `ops:8060` | 8060 |
| dbt_runner | `dbt_runner:8080` | 8080 |
| processing | `processing:8070` | 8070 |

All four `FastAPI()` instantiations have no `title`, `description`, or `version`
— Swagger UI renders "FastAPI" for all of them.

---

## What to Build

### 1. Add metadata to each FastAPI app

Add `title=`, `description=`, and `version=` to all four `FastAPI()`
instantiations. These appear as the heading in Swagger UI and in the OpenAPI
spec.

```python
# scraper/app.py
app = FastAPI(
    title="Scraper API",
    description="SRP and detail page fetching — async job queue, rotation, FlareSolverr credential management.",
    version="1.0.0",
)

# ops/app.py
app = FastAPI(
    title="Ops API",
    description="Admin UI and deploy coordination — search config CRUD, run history, dbt actions, deploy intent.",
    version="1.0.0",
)

# dbt_runner/app.py
app = FastAPI(
    title="dbt Runner API",
    description="HTTP wrapper for dbt builds — lock-aware intent-based partial builds.",
    version="1.0.0",
)

# processing/app.py
app = FastAPI(
    title="Processing API",
    description="Artifact processing — claim → parse → write loop for SRP and detail artifacts.",
    version="1.0.0",
)
```

### 2. Caddy routes

Use `handle_path` (same pattern as `/n8n*` and `/minio*`) to strip the prefix
before proxying. This lets Swagger UI's internal asset calls (`/openapi.json`,
`/docs/oauth2-redirect`) resolve correctly on the backend.

```caddy
handle_path /api-docs/scraper* {
    # observer+ auth (same block as /admin*)
    reverse_proxy scraper:8000
}

handle_path /api-docs/ops* {
    # observer+ auth
    reverse_proxy ops:8060
}

handle_path /api-docs/dbt-runner* {
    # observer+ auth
    reverse_proxy dbt_runner:8080
}

handle_path /api-docs/processing* {
    # observer+ auth
    reverse_proxy processing:8070
}
```

These routes must be placed **before** the catch-all `/admin*` block in the
Caddyfile.

### 3. Index page at `/api-docs`

New route in ops: `GET /admin/api-docs` (falls under the existing observer+
`/admin*` Caddy block — no additional Caddy config needed for the index).

A simple HTML page listing each service with a brief description and a "Open
Docs →" link to `/api-docs/{service}/docs`. No live fetching — static links
only.

---

## Access Control

All routes at `/api-docs/*` are observer+. This matches the `/admin*` policy:
portfolio visitors with observer access can browse the API docs, but they are
not publicly exposed.

The ops service's own `/docs` already reveals admin routes — this is acceptable
for observer-level access given that mutations require auth at the application
layer.

---

## Rollout Order

1. Add `title`, `description`, `version` to all four `FastAPI()` instantiations
2. Add the four `handle_path` blocks to Caddyfile (before `/admin*`)
3. Add `/admin/api-docs` index route and template to ops
4. `docker compose build ops && docker compose up -d` (Caddy picks up Caddyfile
   changes on reload; the other services only need a rebuild if their app.py
   changed)
5. Verify each service's Swagger UI loads correctly at its `/api-docs/*/docs`
   path

---

## What Does Not Change

- No route protection changes for internal service-to-service calls
- FastAPI's built-in `/redoc` also becomes accessible at the same paths as a
  side effect — no extra work needed
- No new environment variables
