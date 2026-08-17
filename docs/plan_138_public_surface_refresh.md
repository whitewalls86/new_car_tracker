# Plan 138: Public Surface Refresh

## Status

**DRAFT — audit complete; implementation has not started.** Written 2026-08-17
after comparing the live `https://cartracker.info/info` page and `README.md`
against `master` at `6f6a2ba`.

This plan covers the repository README and the unauthenticated portfolio surface.
It does not change dashboard behavior, authorization roles, or the production data
architecture.

## Decision in one paragraph

Make the portfolio landing page the public root at `https://cartracker.info/`, move
the authenticated application entry point to the already-supported `/dashboard`
path, and redirect `/info` permanently to `/`. Rewrite the README and landing page
from the same factual contract: clearly separate the DuckDB-backed production
system from the Iceberg/Lakekeeper/Spark/MLflow migration track, describe the
actual HOT-state plus staging-event write pattern, and avoid brittle counts in
evergreen prose. Move the landing page's CSS and JavaScript into local static
assets, make every interaction keyboard-accessible, replace the 41.7 MB eager
video with a bounded preview, and serve public SEO and security metadata. Finally,
take database work out of the request path by refreshing a last-known-good public
stats snapshot in the background. Add a same-origin, dynamically loaded "Recent
work" section whose build-generated JSON comes from the scored roadmap and recent
completion tables in `docs/PLANS.md`, so the public page follows source control
without making GitHub or Markdown parsing a production dependency.

---

## Why this work is needed

The current public surface tells a strong architectural story, but it now mixes
three different snapshots of the project.

| Public claim | Repository or live state on 2026-08-17 |
|---|---|
| 13+, 14, and 40+ make/model pairs | The same live page contains all three values; the live query reports 14 |
| 12 Airflow DAGs | 15 DAG definitions |
| 15 dbt models | 22 SQL models: 5 staging, 9 intermediate, and 8 marts |
| 36 Flyway migrations | 42 migrations |
| 971 tests | `pytest --collect-only -q` collects 2,553 tests in the audited environment |
| 11 Docker containers | 26 default Compose service definitions, including init and exporter services |
| Staging events are flushed into HOT tables | Services update HOT state and append staging events together; the Archiver flushes staging rows to Parquet |
| Exponential cooldown lives entirely in dbt | Operational eligibility is in the Postgres detail-queue view; dbt retains the analytical event history |
| Silver and dbt DAGs each run on their old schedules | `hourly_analytics_refresh` owns the scheduled flush/build sequence; component DAGs are manual |
| All failures surface as notifications | Plan 136 records an eight-hour functional solver outage that container-health alerts missed |

The project has also added meaningful work that neither public surface explains:

- production zstd dictionary compression for bronze HTML;
- indexed cold-storage packs with transparent read fallback and verified pruning;
- deploy-aware, single-flight bronze lifecycle jobs;
- production-derived CI lake snapshots;
- a proven Iceberg/Lakekeeper/Spark/MLflow experiment path and dbt parity work;
- incremental fingerprints, observation runs, and volatility features.

The route shape compounds the content drift. `/info` is public, but the root,
`robots.txt`, and `sitemap.xml` enter the Google OAuth flow. Someone following the
bare domain sees a login screen before seeing what the project is.

---

## Goals

1. Give an unauthenticated visitor a useful, accurate explanation within one page
   load and without requiring Google sign-in.
2. Make `/` the canonical public portfolio URL while preserving `/dashboard` as
   the authenticated application URL.
3. Make the README a useful technical entry point: architecture, evidence,
   current-versus-experimental status, local setup, and access boundaries.
4. Remove known factual contradictions and make future drift visible in CI.
5. Meet a basic accessibility contract for keyboard, screen-reader, reduced-motion,
   and mobile users.
6. Keep the narrative available when DuckDB or Postgres is locked or unavailable.
7. Reduce the public page's initial transfer and eliminate uncontrolled third-party
   runtime dependencies.
8. Show the next planned work and recently completed work from a source-controlled,
   CI-validated snapshot rather than another hand-maintained block of page copy.

## Non-goals

- Making the dashboard, admin UI, Airflow, Grafana, MinIO, or pgAdmin public.
- Changing the Google OAuth2 or DB-backed authorization model.
- Completing Plan 125's DuckDB-to-Iceberg production migration.
- Implementing Plan 136's liveness fixes; this plan may describe that lesson but
  must not claim the fixes have shipped.
- Redesigning the Streamlit dashboard.
- Introducing a JavaScript framework or frontend build system.
- Calling the GitHub API, cloning the repository, or parsing Markdown on a public
  HTTP request.
- Publishing secrets, production infrastructure identifiers, private metrics, or
  operational endpoints that are not already intentionally public.

---

## Public truth contract

Both surfaces must distinguish three kinds of facts.

### 1. Production today

- Airflow orchestrates scraping, processing, archival, maintenance, and analytics.
- Postgres owns current operational state and short-lived event buffers.
- MinIO holds replayable bronze HTML and permanent Parquet history.
- dbt and DuckDB currently build and serve the analytical marts used by `/info`,
  metrics, and Streamlit.
- Caddy, oauth2-proxy, and the ops authorization check protect application routes.

### 2. Proven but not production-serving

- Production-shaped CI lake snapshots.
- Iceberg tables registered through Lakekeeper and exercised through Spark.
- dbt-Spark parity work and MLflow experiment provenance.
- Adaptive-refresh feature and backtesting foundations.

These belong under a heading such as "Platform evolution," never in language that
implies the public dashboard already reads Iceberg.

### 3. Volatile operational numbers

Active listings, observations, throughput, tracked pairs, and analytical freshness
come from the public stats snapshot. They must not also be hard-coded in hero or
service-card prose. Repository inventory may use rounded statements such as
"more than a dozen DAGs," "20+ dbt models," and "2,500+ tests," with an explicit
"verified on" date where precision adds value.

Avoid the phrases "without manual intervention," "every failure alerts," and
"every service exposes `/ready`." The narrower, true claim is that long-running
worker services participate in deploy draining, while functional liveness remains
a separately measured concern.

### 4. Recent and planned work

The public feed is a projection of the roadmap, not a second roadmap:

- "Planned next" comes from the ordered rows in `docs/PLANS.md`'s **Default
  build order** table. Publish only the first four executable rows.
- "Recently completed" comes from the newest-first **Completed** table in the
  same file. Publish only the first four rows.
- Titles, short public summaries, priority, effort, state, and source links may
  be shown. Internal hostnames, incident payloads, approval records, production
  object keys, and other operational detail must not be copied into the feed.
- A plan is not presented as complete until its completion row exists. A merged
  implementation with an outstanding production gate remains "verification" or
  "closeout," not "completed."

The plan document remains the detailed source. The landing page is a small,
current window into it.

---

## Target route and access contract

| Route | Access | Target behavior |
|---|---|---|
| `/` | Public | Canonical portfolio landing page, HTTP 200 |
| `/info` | Public | Permanent redirect to `/` |
| `/static_ops/*` | Public | Versioned local assets with long-lived caching |
| `/robots.txt` | Public | Allows the public root and references the sitemap |
| `/sitemap.xml` | Public | Contains only canonical public URLs |
| `/request-access*` | Google-authenticated | Existing request workflow; CTA explains the sign-in step |
| `/dashboard*` | `viewer`+ | Existing Streamlit application |
| `/admin*` | `observer`+ with current mutation rules | Unchanged |
| infrastructure tools | Existing role requirements | Unchanged |

Caddy should match the exact public root before the final authenticated catch-all.
It may internally rewrite `/` to the ops `/info` handler during the transition,
but the browser-visible canonical URL must remain `/`. The `/info` redirect must
not loop through the internal rewrite.

---

## Stage 0 — Record the baseline and freeze claims

Before changing copy, add a short audit artifact or test fixture recording:

- the current DAG, dbt-model, migration, and Compose service counts;
- the current README and template claims that are intentionally being removed;
- the current live route/auth behavior;
- the current landing HTML transfer size and demo-video size;
- screenshots at desktop and 360 px mobile width.

This baseline is evidence, not permanent product copy. Exact counts may change
during later plans without forcing a prose rewrite.

**Gate 0:** every contradiction in the table above has an assigned replacement or
an explicit decision to remove the claim.

## Stage 1 — Rewrite the factual narrative

### 1a. README structure

Rebuild `README.md` around this order:

1. Existing cover image, one-sentence value proposition, public landing link, and
   CI status.
2. "What this demonstrates" — replayable ingestion, HOT/event separation,
   orchestration, schema evolution, testing, storage economics, and observability.
3. Accurate production data flow.
4. Production services and access boundaries.
5. Technical case studies: Postgres separation, n8n-to-Airflow, deploy drain,
   dictionary compression plus packed storage, and functional-liveness learning.
6. "Production today vs platform evolution."
7. Test strategy without brittle unit/integration totals.
8. Minimal local quick start and links to deeper runbooks.
9. Project structure and protected endpoint matrix.
10. Responsible-use, Cars.com non-affiliation, trademark, and repository-license
    status.

The README must explain that source availability does not grant a license if no
`LICENSE` file is added. Choosing a license is a separate owner decision; the
README should not imply permission that the repository does not grant.

### 1b. Landing-page structure

Keep the current visual restraint and "why" storytelling, but use this sequence:

1. Hero and two CTAs: "Explore the architecture" and "Sign in to request
   read-only access."
2. Live stats with a visible freshness timestamp.
3. Correct data journey:
   `Fetch -> Bronze HTML -> Parse/HOT state -> Silver Parquet -> dbt/DuckDB marts -> Dashboard`.
4. Production architecture cards.
5. Platform-evolution callout that clearly labels Iceberg work as a migration
   track.
6. "Recent work" with separate "Planned next" and "Recently completed" lists,
   loaded progressively from the source-controlled public roadmap snapshot.
7. Four or five decision stories, including packed cold storage and the
   liveness lesson.
8. Testing/evidence section and CTA.

Replace "Cloudflare bypass" and "anti-detection" marketing language with precise,
neutral language about browser-assisted session bootstrap, TLS-compatible HTTP
clients, bounded retries, and adaptive cooldown. Keep the deeper implementation
details in the technical README.

### 1c. Cross-surface consistency

Create a review checklist, and optionally a small source module for shared URLs
and durable labels. At minimum, tests must prevent the known conflicting numeric
phrases from returning. The README and HTML do not need identical copy, but they
must agree on ownership, production status, and route access.

**Gate 1:** a reviewer can answer "what runs in production?", "what is
experimental?", "where does history live?", and "what requires authentication?"
without reconciling contradictory statements.

### 1d. Public roadmap projection contract

Keep `docs/PLANS.md` as the human-edited source of truth. Add
`scripts/build_public_roadmap.py` to parse only two explicitly named Markdown
tables: **Default build order** and **Completed**. Do not implement a general
Markdown crawler and do not infer status from prose in every plan file.

The generator writes deterministic `ops/static_ops/project-updates.json` with a
small versioned schema:

```json
{
  "schema_version": 1,
  "as_of": "2026-08-17",
  "planned": [
    {
      "plan": "136",
      "title": "Solver recycle and real liveness",
      "order": 1,
      "priority": 98,
      "effort": "M",
      "state": "planned",
      "summary": "Add truthful solver outcomes and a drain-aware recycle.",
      "href": "https://github.com/whitewalls86/new_car_tracker/blob/master/docs/plan_136_solver_recycle_and_liveness.md"
    }
  ],
  "completed": []
}
```

The real output contains at most four planned and four completed items. Use the
roadmap's `as of` date instead of wall-clock generation time so unchanged input
produces byte-identical output. Map the ordered table's **Next executable slice**
column to the public `summary` field. Normalize plan identifiers to strings because
the archive contains identifiers such as `V029` and `14.11`.

The script supports `--check`: regenerate in memory, compare with the committed
JSON, and exit non-zero on drift. CI runs that mode and also validates unique
build order, score range 0-100, the `XS|S|M|L|XL` effort vocabulary,
newest-first completion dates, local plan-link existence,
and the public schema. This makes a roadmap edit fail visibly when its public
projection was not refreshed.

## Stage 2 — Make the landing page discoverable

1. Add the exact-root public Caddy handler and keep `/dashboard*` protected.
2. Redirect external `/info` requests to `/` and add a canonical link.
3. Serve public `robots.txt` and `sitemap.xml` without OAuth.
4. Add a descriptive title and meta description.
5. Add Open Graph and Twitter metadata using the existing cover artwork or a
   purpose-built static preview.
6. Add favicon links and JSON-LD for the software project and author.
7. Update README, dashboard sidebar, email links, and other first-party links to
   use the canonical root or explicit `/dashboard` route as appropriate.

Search metadata must describe only the public page. Protected application paths
should not be listed in the sitemap.

**Gate 2:** a fresh unauthenticated session gets HTTP 200 at `/`; `/dashboard`
enters OAuth; `/info` redirects once to `/`; robots and sitemap return their real
content rather than a Google sign-in page.

## Stage 3 — Accessibility and static-asset performance

### 3a. Semantic interactions

- Replace clickable service-card and highlight `<div>` elements with buttons plus
  associated panels, or native `<details>/<summary>` elements.
- Expose `aria-expanded`, `aria-controls`, focus state, and Escape/collapse
  behavior where custom controls remain.
- Restore a valid heading hierarchy (`h1` -> `h2` -> `h3`).
- Do not depend on color alone for pipeline layers or active state.
- Respect `prefers-reduced-motion` and avoid autoplay for those users.

### 3b. Demo media

The checked-in `demo.mp4` is 41,699,885 bytes. Replace it with:

- a poster image that communicates the dashboard before playback;
- a WebM primary plus compressed MP4 fallback;
- `preload="metadata"` or `preload="none"`;
- controls, an accessible label, and a short text transcript/caption;
- an 8 MiB maximum per video asset and no eager full-video transfer.

### 3c. Local assets and response policy

- Extract inline CSS and JavaScript into versioned `static_ops` files.
- Self-host PicoCSS and required icons, preserving license notices, so the public
  page can operate under a same-origin CSP.
- Cache fingerprinted static assets for one year with `immutable`; keep HTML
  uncached or on a short revalidation policy.
- Enable gzip or Brotli for HTML, CSS, JavaScript, SVG, and JSON/XML responses.
- Apply public-route headers: CSP, `X-Content-Type-Options`, `Referrer-Policy`,
  `Permissions-Policy`, and `frame-ancestors 'none'`.

Do not apply a landing-page CSP blindly to Grafana, Airflow, Streamlit, MinIO, or
OAuth routes; scope the header block to the public handlers.

**Gate 3:** all page functions are usable with keyboard only, reduced-motion
users do not receive autoplay, no third-party request is required to render the
page, and the initial page view does not download the full demo.

## Stage 4 — Remove databases from the request path

The current handler opens DuckDB independently for four stats and may repeat the
retry delay for each connection. It also derives "last pipeline run" from
completed queue rows that the hourly cleanup removes.

Replace that path with a small `ops` public-stats component:

1. Refresh a thread-safe snapshot in the background every 60 seconds from the
   existing ops lifespan.
2. Use one read-only DuckDB connection per refresh and consolidate related
   aggregates into as few queries as practical.
3. Preserve last-known-good values per field when one source fails, along with
   `refreshed_at` and `data_through` timestamps.
4. Derive `data_through` from the latest mart/scrape-volume hour, not from the
   transient artifact queue, and label it "Analytics data through."
5. Render the narrative immediately when the snapshot is empty; never sleep or
   retry inside the HTTP request.
6. Log refresh failure once per refresh and expose snapshot age through the
   existing metrics surface.

The public page may show a subtle "temporarily unavailable" state for a missing
metric, but missing analytics must never make the page fail.

### Project updates snapshot and dynamic loading

The work feed is dynamic in the browser but static and source-controlled at the
service boundary:

1. Run `python scripts/build_public_roadmap.py` whenever the roadmap changes.
   Commit the deterministic `ops/static_ops/project-updates.json`; the existing
   ops image build already copies it into the image.
2. Add semantic "Planned next" and "Recently completed" containers to
   `info.html`, with a plain link to the GitHub roadmap as the no-JavaScript and
   fetch-failure fallback.
3. `info.js` fetches `/static_ops/project-updates.json` after the narrative is
   usable, validates `schema_version`, and renders with DOM APIs and
   `textContent` only. Do not inject feed values through `innerHTML`.
4. Sort planned items by `order` and completed items by date even though the
   generator already emits them correctly. Cap both lists again client-side so
   a malformed snapshot cannot create an unbounded page.
5. On a timeout, non-2xx response, invalid schema, or empty feed, keep the
   fallback visible and log at most one concise console warning. The rest of the
   page remains unchanged.
6. Serve this non-fingerprinted JSON with short caching plus revalidation (for
   example `public, max-age=300, must-revalidate` and ETag), not the one-year
   immutable policy used by fingerprinted CSS, JavaScript, and media.

There is no background application refresher for project updates and no GitHub
token. New roadmap content becomes public with the next normal image deploy.
Visitors requesting `cartracker.info/info` receive the planned permanent redirect
to `/` and see the same dynamically loaded section there.

**Gate 4:** `/` performs no database connection, remains responsive during a dbt
write lock and Postgres outage, clearly distinguishes stale cached stats from
fresh ones, and loads recent work without a database or upstream network call.

## Stage 5 — Regression coverage

Add tests for:

- public stats aggregation, formatting, last-known-good behavior, concurrency,
  and failure isolation;
- landing-template rendering with full, partial, stale, and empty stats;
- public-roadmap generation, deterministic `--check`, schema validation, score
  and effort constraints, ordering, item caps, and broken local plan links;
- project-updates progressive enhancement with valid, unavailable, malformed,
  empty, and unsupported-schema JSON;
- semantic controls, required metadata, canonical URL, media fallback, and the
  absence of the known stale phrases;
- Caddy route ordering and access requirements;
- public security and cache headers;
- robots and sitemap content;
- README links and the production-versus-experimental wording contract.

CI verification should include:

```text
GET /                 -> 200, no OAuth redirect
GET /info             -> 308 -> /
GET /static_ops/project-updates.json -> 200 application/json
GET /robots.txt       -> 200 text/plain
GET /sitemap.xml      -> 200 application/xml
GET /dashboard        -> OAuth redirect when unauthenticated
GET /admin            -> existing OAuth + role behavior
```

Run a manual Lighthouse pass against the deployed page at mobile width. Targets:

- accessibility >= 95;
- SEO >= 95;
- best practices >= 95;
- performance >= 90 on a warm VPS response;
- no horizontal page overflow at 360 px;
- no console errors and no failed asset requests.

These scores are release evidence, not a new CI dependency in the first pass.

**Gate 5:** automated route/template tests pass and the manual accessibility,
mobile, and performance checklist is attached to the implementation PR.

## Stage 6 — Deployment and verification

Deploy `ops` and Caddy together because the root route depends on both.

1. Build the new ops image and validate its health internally.
2. Confirm the public-stats background refresh has either a fresh snapshot or a
   valid empty state.
3. Confirm the project-updates JSON matches the roadmap, has the expected cache
   policy, and renders both lists without blocking the page.
4. Apply the Caddy route change.
5. Run the unauthenticated route matrix from an external client.
6. Sign in as `viewer`, `observer`, and `admin` and verify existing boundaries.
7. Confirm dashboards, request-access email links, static media, and social cards.
8. Watch Caddy and ops errors, response latency, OAuth redirects, and public-stats
   age for at least one analytics refresh cycle.

Rollback is the previous Caddyfile plus previous ops image. No database migration
is required, and `/dashboard` remains the stable explicit application path during
both rollout and rollback.

---

## Expected file map

| File | Change |
|---|---|
| `README.md` | Rewrite technical public entry point |
| `Caddyfile` | Public root, redirect, robots/sitemap, scoped headers, static caching |
| `ops/routers/info.py` | Render cached snapshot; canonical public responses |
| `ops/public_stats.py` | New background snapshot collector and cache |
| `ops/app.py` | Start/stop stats refresh with app lifespan |
| `ops/templates/info.html` | Correct copy and semantic markup |
| `ops/static_ops/info.css` | Extracted page styles |
| `ops/static_ops/info.js` | Accessible progressive enhancement |
| `ops/static_ops/project-updates.json` | Deterministic public projection of planned and completed work |
| `ops/static_ops/*` | Local vendor assets, poster, optimized video, favicon/social image |
| `scripts/build_public_roadmap.py` | Parse the two roadmap tables, validate them, and generate/check the JSON snapshot |
| `dashboard/app.py` | Canonical portfolio and dashboard links |
| `ops/email.py` | Canonical destinations where needed |
| `tests/ops/routers/test_info.py` | Stats and template behavior |
| `tests/test_observability_config.py` or focused Caddy test | Public/protected route contract and headers |
| `docs/PLANS.md` | Ordered/scored plan source plus newest-first public completion summaries |
| `.github/workflows/ci.yml` | Reject stale or invalid project-updates snapshots |

## Recommended PR sequence

1. **PR A — Truth and roadmap pass:** README and landing copy, accurate
   architecture, current versus experimental, scored roadmap, deterministic
   public projection, and CI drift check; no routing change.
2. **PR B — Public root:** Caddy route contract, canonical metadata, robots,
   sitemap, link updates, and route tests.
3. **PR C — Frontend quality:** semantic interactions, dynamically loaded work
   feed, extracted/local assets, optimized media, CSP, caching, and accessibility
   evidence.
4. **PR D — Stats reliability:** background snapshot, freshness semantics,
   metrics, and failure tests.

PR A can ship independently. PRs B and C should be reviewed together for CSP and
asset-path compatibility. PR D may ship before or after them but must preserve the
current soft-failure behavior throughout.

## Completion criteria

Plan 138 is complete only when:

- `/` is the canonical public landing page and `/dashboard` remains protected;
- `/info`, robots, sitemap, and every first-party link follow the route contract;
- README and landing copy contain none of the audited factual contradictions;
- production and experimental architecture are visibly separated;
- public requests do not connect to DuckDB or Postgres;
- the page remains useful with no stats available;
- planned and recently completed work load from a deterministic, CI-validated
  source-control snapshot, with a useful no-JavaScript/failure fallback;
- the demo is bounded, lazy, accessible, and cached;
- interactive content works with keyboard and screen reader semantics;
- scoped public security headers and local assets are in production;
- automated tests and the external route matrix pass;
- a mobile Lighthouse report and screenshots are recorded in the closing PR;
- `docs/PLANS.md` and `docs/completed_plans.md` record the final deployment date
  and measured before/after results.
