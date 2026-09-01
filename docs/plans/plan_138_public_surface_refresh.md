# Plan 138: Public Surface Refresh

## Status

**STAGE 0 COMPLETE — STAGE 1 IN PROGRESS. Stages 2 through 6 not started.**
Written 2026-08-17 after comparing the live `https://cartracker.info/info` page
and `README.md` against `master` at `6f6a2ba`.

Both Stage 0 gates closed on 2026-08-31: **Gate 0b** reconciled the internal
overviews (CAR-37, PR #313) and **Gate 0** recorded the baseline and gave every
drift-table row a disposition (CAR-44, PR #315), so Stage 1 draws copy from the
reconciled overviews and the assigned replacement claims.

| Stage 1 slice | State |
|---|---|
| **1a** README rewrite (CAR-38, PR #320) | Merged to `master` at `a458877`, **soaking** — merged is not closed, per the truth contract's §4 |
| **1b** Landing-page structure (CAR-39, PR #322) | Merged to `master` at `63e5b6e` on 2026-08-31, **soaking and undeployed** — the template changed, the live page has not |
| **1c** Cross-surface consistency (CAR-56) | **In progress**; 1b was built to meet it by construction rather than leave it to a later check, so this slice records and enforces that agreement |
| **1d** Public roadmap projection | Not started. The landing page carries the section and its list ids; the generator and `project-updates.json` do not exist yet |
| **1e** Weekly recap projection | Not started |
| **1f** Reconcile against the published writings | **Audit done 2026-08-31**, copy pass not started. Three articles supplied; one carries ten disposed-of claims and contradicts another on bronze retention. The surface scope question is open |

**The two public surfaces are now in different states, and the distinction
matters.** The repository is public, so 1a's README changed a public surface the
moment it merged. The landing page has not: 1b merged to `master` on
2026-08-31, but the ops service has not been redeployed from it, so
`https://cartracker.info/info` still serves the pre-plan copy the Stage 0
baseline screenshotted. A fetch on 2026-08-31 returned 54,343 bytes still
carrying "without manual intervention" and the hardcoded make/model counts —
two of the phrases Stage 0 disposed of. **Merging fixed the template, not the
surface.** Until that deploy lands, **the README and the live page disagree** —
which is the drift this plan exists to remove, temporarily widened by fixing one
surface before the other. Stage 6 is what closes it.

The analytics acquisition and database-removal portion of Stage 4 moved to
[Plan 143](plan_143_analytics_serving_snapshot.md) on 2026-08-18 before either
plan deployed it. This plan owns presentation of that snapshot, not its
production or storage connection. **Plan 143 completed on 2026-08-20** (PRs #217
and #218; see [completed_plans.md](../planning/completed_plans.md)), so Stage 4
and PR D are unblocked — the snapshot contract they consume already exists in
production.

Two changes on 2026-08-31, from a scoping session:

- **Weekly recaps become a public surface.** `docs/recaps/` holds an unbroken
  weekly file from 2026-02-01 onward, and Stage 1d already establishes the
  pattern a recap index needs. Stage 1e generates it; Stage 3d presents it.
- **Per-service subdomain routing left this plan** and became
  [Plan 165](plan_165_service_subdomain_routing.md), triggered by Plan 69. That
  document records why it is not a stage here.

This plan covers the repository README and the unauthenticated portfolio surface.
It does not change dashboard behavior, authorization roles, or the production data
architecture. **That inventory of public surfaces may be incomplete** — the
author's published writing is public and covers some of the same claims, which
[Stage 1f](#1f-reconcile-against-the-published-writings) raises as an explicit
scope decision rather than leaving unstated here.

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
take database work out of the request path by rendering the last-known-good
serving snapshot produced by Plan 143. Add a same-origin, dynamically loaded "Recent
work" section whose build-generated JSON comes from the scored roadmap and recent
completion tables in `docs/PLANS.md`, so the public page follows source control
without making GitHub or Markdown parsing a production dependency. Publish the
weekly recaps through the same build-time projection, as static pages the "Recent
work" section links into, so the page that says what shipped has somewhere to
send a reader who wants the account of it.

---

## Why this work is needed

The current public surface tells a strong architectural story, but it now mixes
three different snapshots of the project.

The right-hand column is a dated audit and is **not** maintained. Six of its
values went stale within two weeks; Gate 0 re-measured them on 2026-08-31 and
recorded the current figures in
[the Stage 0 baseline](../evidence/plan_138_stage_0_baseline_2026-08-31.md).

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
9. Publish the weekly recaps as readable public pages, generated from
   `docs/recaps/` at build time, so the long-form account of the work is reachable
   from the landing page and stays current without hand-maintained copy.

## Non-goals

- Making the dashboard, admin UI, Airflow, Grafana, MinIO, or pgAdmin public.
- Changing the Google OAuth2 or DB-backed authorization model.
- Completing Plan 125's DuckDB-to-Iceberg production migration.
- Implementing Plan 136's solver-liveness fixes or Plan 143's analytics serving
  boundary; this plan may describe those lessons but must not claim they shipped.
- Redesigning the Streamlit dashboard.
- Introducing a JavaScript framework or frontend build system.
- Calling the GitHub API, cloning the repository, or parsing Markdown on a public
  HTTP request. Markdown is rendered at image-build time, never in a request.
- Moving any service from a path prefix to its own subdomain. That is
  [Plan 165](plan_165_service_subdomain_routing.md), and no part of it — including
  a single-host proof — belongs in this plan's Caddy change.
- Publishing plan documents themselves, or `ARCHITECTURAL_OVERVIEW.md` and
  `OPERATIONAL_ENGINEERING_OVERVIEW.md`, as first-party pages. Recap links to plan
  documents resolve to GitHub, as Stage 1d's roadmap links already do.
- Adding comments, feeds beyond the recap index, search, or any other
  blog-platform feature. The recap surface is a generated index plus generated
  pages.
- Publishing secrets, production infrastructure identifiers, private metrics, or
  operational endpoints that are not already intentionally public.
- Lifting the Stage 0 overviews' prose onto the README or landing page. This is
  a **register** rule, not a disclosure one, and the distinction was corrected
  on 2026-08-31: the repository is public, so those documents are already a
  public surface, and the earlier wording — that they "may not reach a public
  surface" — described a boundary that does not exist. The anti-detection path
  is documented in `scraper/processors/cf_session.py`, which is public;
  `ARCHITECTURAL_OVERVIEW` §1 narrates the same mechanism in prose. Neither is
  secret and neither can be made secret by a label. What the front door does
  not get is the deep dive's register: the README and landing page describe the
  scrape path in neutral terms, and production object prefixes stay out of
  authored entry-point copy because they serve no reader there.

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
"more than a dozen DAGs," "20+ dbt models," and "3,000+ tests," with an explicit
"verified on" date where precision adds value. (The example read "2,500+ tests"
until Gate 0 re-measured the suite at 3,661 on 2026-08-31.)

Avoid the phrases "without manual intervention," "every failure alerts," and
"every service exposes `/ready`." The narrower, true claim is that long-running
worker services participate in deploy draining, while functional liveness remains
a separately measured concern.

### 4. Recent and planned work

The public feed is a projection of the roadmap, not a second roadmap:

- "Planned next" comes from the ordered rows in `docs/PLANS.md`'s **Default
  build order** table. Publish only the first four executable rows.
- "Recently completed" comes from the newest-first table in
  [`docs/planning/completed_plans.md`](../planning/completed_plans.md). Publish
  only the first four rows. **This is a different file from the build order.**
  Plan 146 removed `docs/PLANS.md`'s duplicate Completed table; what remains
  under that heading is a pointer, and a generator that parses it finds no rows.
- Titles, short public summaries, priority, effort, state, and source links may
  be shown. Internal hostnames, incident payloads, approval records, production
  object keys, and other operational detail must not be copied into the feed.
- A plan is not presented as complete until its completion row exists. A merged
  implementation with an outstanding production gate remains "verification" or
  "closeout," not "completed."

The plan document remains the detailed source. The landing page is a small,
current window into it.

### 5. The weekly recaps

The recaps are a fourth kind of fact, and the one with the least editorial
control: they are written weekly by the `plan-week` skill against git history,
not authored as public copy.

- They are already public. Stage 1d's roadmap links resolve to
  `github.com/whitewalls86/new_car_tracker`, so this plan already assumes the
  repository is publicly readable, and `docs/recaps/` is in it. Publishing them
  on the site changes their **prominence and framing**, not their disclosure
  status. The review below is an editorial gate, not a leak gate.
- They are a record of a week, not evergreen prose. A recap is correct as of its
  date and is never revised to match a later truth. Every published recap must
  carry its week and a statement that it is a point-in-time record, so §1's
  production/experimental split is not contradicted by a six-month-old page.
- They may name what the truth contract's §3 narrows. The word `trawl` appears
  eleven times across four recap files — `2026-07-12.md` (2), `2026-07-19.md`
  (5), `2026-08-23.md` (1) and `2026-08-30.md` (3), which is the complete set —
  and the scrape path is exactly what the non-goals bar from *authored* public
  copy. Stage 1e decides the policy; it does not get to be decided silently by a
  generator.

---

## Target route and access contract

| Route | Access | Target behavior |
|---|---|---|
| `/` | Public | Canonical portfolio landing page, HTTP 200 |
| `/info` | Public | Permanent redirect to `/` |
| `/recaps` | Public | Generated recap index, newest first, HTTP 200 |
| `/recaps/YYYY-MM-DD` | Public | One generated recap page, HTTP 200 |
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

#### Gate 0 evidence — 2026-08-31 (CAR-44)

**Gate 0 closed on 2026-08-31.** The baseline it asks for is
[`docs/evidence/plan_138_stage_0_baseline_2026-08-31.md`](../evidence/plan_138_stage_0_baseline_2026-08-31.md),
with screenshots alongside it. Every row of the drift table above carries a
disposition there, plus three further rows of the same class found while
measuring.

It is a document and not a test fixture on purpose: pinning these counts in CI
would contradict the paragraph above it, and Stage 5 already scopes drift
detection correctly as "the absence of the known stale phrases" — an assertion
about wording rather than arithmetic.

The counts were **re-measured, not carried forward**. Six of the drift table's
2026-08-17 values are stale, and the audit column above is left as the dated
historical record it is:

| Subject | 2026-08-17 | 2026-08-31 |
|---|---:|---:|
| Airflow DAGs | 15 | **15** (19 files; 4 define no DAG) |
| dbt models | 22 | **23** (5 / 9 / 9) |
| Flyway migrations | 42 | **49** |
| Tests collected | 2,553 | **3,661** |
| Compose services | 26 | **34 defined / 28 non-profiled / 28 expected running** |

Two measurements settled a question rather than just refreshing a number:

- **The Compose denominator.** Two different sets both have 28 members, and
  "28 without a profile gate" is the wrong one — it excludes `trawl` and
  `redis-trawl`, the live scrape path, while including the `airflow-init` and
  `flyway` one-shots. `EXPECTED_SERVICES` is the honest set, and it is already
  test-enforced against Plan 142's manifest. Public copy prints no integer at
  all: **"more than two dozen long-running services."**
- **Mobile layout.** The page does **not** overflow at 360 px
  (`body.scrollWidth` 356 ≤ 360). A plain headless screenshot appears to show
  severe clipping, but the layout viewport floors at 512 px on this host, so
  that image is a 512-wide layout cropped to 360. The committed screenshot
  renders inside a 360 px iframe. Trusting the first image would have written a
  false defect into the baseline.

Counts are replaced with rounded phrasing throughout rather than refreshed to
new exact values, so this gate does not simply reload the drift it exists to
remove. §3's own "2,500+ tests" example was updated to "3,000+" for the same
reason.

Two findings recorded rather than fixed, both outside this gate: `/info` is
served uncompressed (a Stage 3c number, captured here as the before-state), and
the stats tile's `(stale)` marker is Plan 143's contract, not this plan's. The
two Gate 0b findings — `.env.example:49-57` and the missing internal-only
markers — remain open and are untouched by this gate. *(Both closed later the
same day; see the Gate 0b section below.)*

### Source documents for Stage 1a

Two current-state overviews were written on 2026-08-28, after this plan's
2026-08-17 audit and against a later tree. They are the strongest material this
plan has for Stage 1a, and they are **long-form narrative, not front-door
copy** — public documents like the rest of the repository, written at a
register the README and landing page do not use.

| document | what Stage 1 draws from it |
|---|---|
| [ARCHITECTURAL_OVERVIEW.md](../ARCHITECTURAL_OVERVIEW.md) | 1a.3 accurate production data flow — the ingestion-to-mart diagram and the Bronze / Operational / Staging / Silver / Mart shape table; the "Production today vs platform evolution" split; its own "Current-source truth versus older summaries" section, which independently reaches the archiver/HOT row in the table above |
| [OPERATIONAL_ENGINEERING_OVERVIEW.md](../OPERATIONAL_ENGINEERING_OVERVIEW.md) | 1a.5 case studies — deploy drain (§3.2–3.4), dictionary compression and packed storage (§4.2–4.3), functional-liveness learning (§2.5–2.6); 1a.7 test strategy without brittle totals (§5.1, §5.4); and the narrowed alerting language the truth contract's §3 requires (§2.5–2.7) |

**Boundary.** These inform `README.md`. They do **not** feed the public landing
page or the project-updates snapshot. `ARCHITECTURAL_OVERVIEW` §1 documents the
anti-detection path in detail — TLS fingerprint matching, `cf_clearance`
acquisition, the credential cache, 403 handling — and both documents print
production object prefixes. That is precisely the material the non-goals bar
from public surfaces. Drawing on them for the README means paraphrasing the
architecture, never lifting §1 or the prefix listings.

**They are sources, not authorities, until reconciled.** A truth pass cannot
draw from a document that carries the same drift it exists to remove.
As written on 2026-08-28, `ARCHITECTURAL_OVERVIEW` §1 stated that FlareSolverr
performs the browser bootstrap; `docker-compose.yml:174` marks that container
**vestigial** and names `trawl` as the live scrape path (resolved — see the
Gate 0b evidence below). The mechanism described is accurate — the code
still speaks the FlareSolverr v1 protocol and the environment variable is still
`FLARESOLVERR_URL` — but a reader concludes the project runs FlareSolverr in
production, which it does not. Both documents also predate Plan 145's Stage 5
commit and the Stage 6 machinery.

**Gate 0b:** the scraper section of `ARCHITECTURAL_OVERVIEW` is reconciled
against `docker-compose.yml` and `scraper/processors/cf_session.py`, and both
documents' review dates are refreshed, before any Stage 1 copy is drawn from
them. Anything not reconciled is quoted from the source tree instead.

#### Gate 0b evidence — 2026-08-31 (CAR-37)

The naming drift is reconciled. `ARCHITECTURAL_OVERVIEW` §1 no longer implies
production runs FlareSolverr: seven passages changed, and a note after the
two-layer bullet list now records the three facts that were in conflict.

| Claim | Source of truth |
|---|---|
| The live solver is `trawl`, not `flaresolverr` | `docker-compose.yml:174` marks the container vestigial; `docker-compose.yml:209` defines `trawl`, digest-pinned under the `trawl` profile |
| The variable kept its old name | `FLARESOLVERR_URL=http://trawl:8191` in production — [`runbook_solver_oom_and_recycle.md:8`](../runbooks/runbook_solver_oom_and_recycle.md), and Plan 136 §132 records the same |
| The protocol really is FlareSolverr's | `cf_session.py:187` POSTs `{"cmd": "request.get"}` to `{FLARESOLVERR_URL}/v1`, which `trawl` implements |
| The vestigial container still runs | Zero requests served since 2026-07-07, per the runbook; retained deliberately so health coverage never exempts a service believed unused |

Review dates refreshed to 2026-08-31 on both documents.

**Two findings recorded rather than fixed**, because both are outside this
gate:

- `.env.example:49-57` still defaults `FLARESOLVERR_URL` to the vestigial
  container and describes `trawl` as an "Optional TRAWL solver trial". That is
  the same drift one layer down, in a file this gate does not name.
- Neither overview carries an internal-only marker, even though Stage 0 treats
  them as internal references and §1 documents the anti-detection path. The
  boundary currently lives only in this plan document.

**Both findings closed 2026-08-31**, and the second one closed differently than
it was written. The bullets above are left as the dated record of what the gate
found.

The first is [Plan 167](plan_167_solver_config_default_truth.md) Stage 0, which
also found that `docker-compose.yml:267` carries the identical stale default —
the finding named one line and there were two.

The second was recorded on the wrong axis. It asks for an "internal-only
marker", but the repository is public (`gh repo view` reports visibility
PUBLIC), so both overviews already *are* a public surface and no label can
change that. `scraper/processors/cf_session.py` is public too, which means §1
narrates in prose a mechanism the source documents in code. The real
distinction is **register, not disclosure**, and the non-goal above was
rewritten to say so. Both documents now carry a marker on that axis. Two
corrections fell out of the re-reading: the finding says "both documents print
production object prefixes" and only `ARCHITECTURAL_OVERVIEW` does (`:50-52`,
`:338`, `:365`, `:384`); and §5's concern that recaps name `trawl` is the same
category error, so Stage 1e inherits a narrower question than the one written
there.

The staleness against Plan 145 Stage 5/6 noted above is **not** addressed here;
this gate covered the scraper section only.

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
   loaded progressively from the source-controlled public roadmap snapshot, plus
   a static link to the recap index for the long-form account.
7. Four or five decision stories, including packed cold storage and the
   liveness lesson.
8. Testing/evidence section and CTA.

Replace "Cloudflare bypass" and "anti-detection" marketing language with precise,
neutral language about browser-assisted session bootstrap, TLS-compatible HTTP
clients, bounded retries, and adaptive cooldown. Keep the deeper implementation
details in the technical README.


#### Stage 1b evidence — 2026-08-31 (CAR-39)

`ops/templates/info.html` is rebuilt in the eight-part sequence above. The page
now carries the same claims as the README, so Stage 1c's cross-surface
requirement is met by construction: twelve shared claims were checked as
normalized strings across both files, and all twelve agree — the archiver not
populating HOT tables, the backoff living in `ops.ops_detail_scrape_queue`, the
dashboard reading DuckDB, Iceberg as a migration track, `trawl` as the live
solver, `hourly_analytics_refresh` owning the scheduled sequence, functional
liveness still open, the salted email hash, the absent `LICENSE`, the Cars.com
non-affiliation, and the two rounded counts both surfaces print.

**What the sequence changed, beyond copy.** Three of the eight items did not
exist on the page at all: the platform-evolution callout (item 5), the recent-work
lists (item 6), and the testing/evidence section (item 8). Two items existed but
were wrong rather than merely stale:

- **The data journey named five stages and mislabeled two of them.** "Bronze"
  pointed at *Processing → Postgres*, which is the operational layer, not bronze;
  bronze is the stored HTML that no stage named. The strip now runs the plan's six:
  Fetch → Bronze HTML → Parse/HOT state → Silver Parquet → dbt/DuckDB marts →
  Dashboard.
- **The decision stories were five cards, and two of them were not decisions.**
  "The Data Journey" restated the section above it, and "10M+ observations, one
  server" was mostly volatile counts. Both are gone. Packed cold storage and the
  liveness lesson take their places, as §1b item 7 requires, and the single-server
  point survives inside the storage story where it is a conclusion rather than a
  statistic.

Every phrase Gate 0 assigned for deletion is gone, and no exact repository count
is printed anywhere on the page. The scrape-path language is rewritten to
description rather than marketing: "anti-detection layer (Chrome TLS
fingerprinting + Cloudflare bypass)" becomes an HTTP client whose TLS behavior
matches a mainstream browser, with bounded retries and an adaptive per-listing
cooldown governing refusals.

**Two scope decisions, both recorded rather than assumed.**

1. **The recent-work lists ship as structure, not as a feed.** §1b item 6 wants
   them loaded from the roadmap snapshot, and that snapshot is Stage 1d's
   deliverable: neither `scripts/build_public_roadmap.py` nor
   `ops/static_ops/project-updates.json` exists yet. Fetching an artifact that
   is not there would put a 404 on every public page load, which Stage 5's "no
   failed asset requests" bar would then have to remove. So the section ships
   with both columns, the `work-planned` and `work-completed` list ids that 1d's
   renderer will target, and static content that points a reader at the build
   order and the archive on GitHub — the real sources, reachable today.
2. **The recap pointer resolves to GitHub, not to `/recaps`.** The route does
   not exist until Stage 1e generates the pages and Stage 2 opens the path. The
   truth contract's §5 already establishes that the recaps are public in the
   repository, so the GitHub link is accurate now and 1e/2 will retarget it.

**Three counts on the page were stale in the same way the drift table's rows
were, and were not in it.** Found while writing the copy, measured against
`a458877`:

| Old claim | Measured | What the page says now |
|---|---:|---|
| "Nine alert rules" (`info.html:639`) | 22 rules in `grafana/provisioning/alerting/rules.yml` | "More than twenty alert rules" |
| "all five Python services" ship logs (`:638`) | 6 — `test_promtail_all_services_present` names ops, scraper, processing, dbt_runner, archiver, pack-worker | "every Python service" |
| "Twelve DAGs" (`:557`) | 15 `dag_id=` values | "More than a dozen DAGs" |

**One correction to the Gate 0 baseline.** Row 11 disposes of "Every service
exposes a `/ready` endpoint" and its evidence table says "Exactly three do."
**Four do:** `scraper/app.py:272`, `processing/app.py:29`,
`archiver/app.py:651`, and `dbt_runner/app.py:88`, whose `/ready` returns 503
while jobs are in flight — the same drain contract, not a health alias. The
disposition itself is unaffected, because the replacement claim it assigned
prints no integer: "long-running worker services expose `/ready` and participate
in deploy draining" is true at three and true at four. The baseline document is
a dated record and is left as written; this is the correction, in the stage that
found it.

**Tests.** `tests/ops/routers/test_info.py` gains
`TestLandingPageStructure` — nine tests over the rendered page: the eight
sections in the plan's order, the six journey stages in order, the archiver and
backoff mechanisms stated correctly, Iceberg labeled a migration track, both
work lists present with the recap pointer, no barred phrase surviving, the
tracked-pair count appearing exactly once, and the whole narrative surviving an
empty stats snapshot (Goal 6). The barred-phrase list is an enumeration, which
the testing contract's first rule normally forbids; it is sanctioned here
because Stage 5 scopes public-surface drift detection as "the absence of the
known stale phrases", which is an assertion about wording rather than about the
repository.

Each new test was mutation-checked rather than trusted: reintroducing "anti-detection
… without manual intervention" fails the barred-phrase test, renaming a
`data-layer` fails the journey test, and moving the recent-work section after the
decision stories fails the order test. `tests/ops/` passes at 380.

**A second pass judged the page as a portfolio piece, not only as a truth
pass.** Four changes came out of it, all within §1b's own items:

- **The storage-layer table moved onto the page** (§1b item 3). The pipeline
  strip says what happens; it does not say what a *row* is. The README's
  Layer / Physical home / Grain / Why-it-exists table is the strongest
  data-modelling artifact either surface has, and it was README-only. It is now
  on both, which also widens Stage 1c's agreement rather than narrowing it.
- **Three measured outcomes were promoted out of the collapsed panels.** The
  45-second dbt build that blocked scheduling, the roughly two-thirds fall in
  inode pressure, and the outage's eight hours were all written where only a
  reader who clicked "Read more" would find them. They are dated results, not
  the volatile inventory §3 bars, so a reader who never clicks now sees what the
  decisions produced.
- **The hero names the constraint.** "All of it runs on a single host" is the
  premise most of the decisions below answer to, and the page previously left a
  reader to infer it from the architecture section.
- **The recent-work placeholder was cut roughly in half.** Standing at item 6
  it occupies prime position, and until Stage 1d generates the feed its content
  is necessarily a pointer. Two short pointers read as a deliberate window into
  the record; two paragraphs about planning read as filler in the place the
  reader expected work.

**The linear strip became a diagram, and §1b item 3 is why.** Item 3 specifies
the journey as a linear string, and a strip implemented it literally — but the
strip cannot express the two facts that make this pipeline worth reading about:
the scraper writes the page and its pointer separately, and processing writes
current state and its event **in one transaction**, beginning two paths that
never rejoin. The left path is a *loop* — HOT state feeds the scrape queue view,
which decides the next fetch — and a strip cannot draw a cycle at all.

The strip is replaced by an inline SVG carrying the same journey plus both forks
and the loop, and its dead CSS is deleted rather than left orphaned. This is an
addition to item 3, not a substitution for it: the specified sequence is the path
through the diagram.

**Two things made this the right call rather than scope creep**, and both came
from outside this stage:

- **§1b item 1 already made "Explore the architecture" the primary CTA.** The
  page sends its main call-to-action at the architecture, so the architecture is
  what has to reward the click. A linear strip did not.
- **The dashboard cannot carry the visual weight, and will not soon.**
  [Plan 150](plan_150_analytics_product_and_bi_serving_layer.md) records the same
  judgment in its problem statement — "the public product presents only a small
  Streamlit dashboard with basic graphs", a mismatch that "limits the portfolio
  value of work that already exists" — at priority 68 and effort XL,
  research-gated. So the platform *is* the public product for now, and the
  diagram is the honest place to spend the page's visual budget.

**This has a consequence for Stage 3b**, which is now written into that
stage rather than restated here: if the dashboard is not yet the product, the
prior question is whether the hero video should exist at all, not what format it
takes. Found here; decided there.

**Stage 3a's colour rule was met on the way in rather than retrofitted.** Every
node states its layer as text in a badge, so the stroke colour is reinforcement
and never the signal, and the control loop is dashed as well as labelled. The
`<svg>` carries `role="img"`, a `<title>`, and a `<desc>` that states both forks
and the loop in prose — the diagram's whole argument reaches a reader who cannot
see it. Three tests hold that: the node sequence, the description's three
claims, and the non-colour encoding of the loop.

One defect found by rendering rather than by reading: `parse` and `decides` had
no `--layer-color`, so `stroke: var(--layer-color)` computed to nothing and both
nodes rendered with no border at all. The rule now carries a fallback, and both
layers have a colour. It is only visible in a screenshot, which is why one was
taken in each theme.

**Layout re-measured, by the method the baseline had to invent.** The page was
rendered with representative stats and probed inside a 360 px iframe served over
a local HTTP origin — an iframe because headless Chrome floors the top-level
layout viewport at 512 px on this host, and HTTP rather than `file://` because a
`file://` iframe is an opaque origin whose document the probe cannot read:

```text
innerWidth=360  body.scrollWidth=360  documentElement.scrollWidth=360
widest element: div.pipeline-stage, right edge 796
```

`scrollWidth (360) <= innerWidth (360)`, so there is still no page-level overflow.
The widest element is still `div.pipeline-stage`, now reaching 796 rather than the
baseline's 666 because the strip gained a sixth stage; it scrolls inside its own
`overflow-x: auto` container, which is the same intentional behaviour the baseline
recorded rather than a regression.

**One presentation observation left for Stage 3.** In several service cards the
"Why?" affordance wraps onto its own line, because `.why-cta`'s `margin-left:
auto` pushes it to the card's right edge and the title, badge, and affordance
together exceed the text column at a three-up grid width. This is pre-existing —
it is in the baseline's committed desktop screenshot — and shortening seven
over-long badges in this stage reduced it without removing it. Recorded here
rather than fixed, because card presentation is Stage 3's.

**Not in this stage**, per CAR-39: Stage 2's routes and search metadata, Stage
3's accessibility and asset work — the Pico and simple-icons CDN dependencies
the baseline recorded are still there and are Stage 3c's to remove — and Stage
4's snapshot presentation. The stats block's markup is carried across unchanged
for that reason; only the freshness note beneath it is new.

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
`scripts/build_public_roadmap.py` to parse exactly two explicitly named Markdown
tables, **which live in two different files**:

| List | Table | File |
|---|---|---|
| `planned` | **Default build order** | `docs/PLANS.md` |
| `completed` | the archive table | `docs/planning/completed_plans.md` |

Do not implement a general Markdown crawler and do not infer status from prose
in every plan file. **There is no Completed table in `docs/PLANS.md`** — Plan 146
replaced it with a pointer, so a generator that looks for one there finds no rows
and silently publishes an empty list rather than failing.

`PLANS.md`'s third table, **Current closeout**, is deliberately not an input. Its
rows are deployed work whose evidence is still pending, which is neither "planned
next" nor "recently completed"; such a plan appears in **neither** list until its
completion row lands in the archive. That is the operational form of the truth
contract's §4 rule, and it is why the feed needs no third state.

The generator writes deterministic `ops/static_ops/project-updates.json` with a
small versioned schema:

```json
{
  "schema_version": 1,
  "as_of": "2026-08-30",
  "planned": [
    {
      "plan": "161",
      "title": "Testing contract",
      "order": 1,
      "priority": 87,
      "effort": "S",
      "state": "planned",
      "summary": "Land the reviewer skill and the test that asserts the contract.",
      "href": "https://github.com/whitewalls86/new_car_tracker/blob/master/docs/plans/plan_161_testing_contract.md"
    }
  ],
  "completed": [
    {
      "plan": "147",
      "title": "Scrape state ownership",
      "date": "2026-08-30",
      "state": "completed",
      "summary": "Separated the fetch and enrichment timestamps so a stalled processor no longer causes the same listings to be re-fetched.",
      "href": "https://github.com/whitewalls86/new_car_tracker/blob/master/docs/plans/plan_147_scrape_state_ownership.md"
    }
  ]
}
```

The real output contains at most four planned and four completed items. Use the
roadmap's `as of` date — the one in `PLANS.md`'s **Current State** heading,
`2026-08-30` at the time of writing — instead of wall-clock generation time, so
unchanged input produces byte-identical output. Map the ordered table's **Next
executable slice** column to the public `summary` field. Normalize plan
identifiers to strings because the archive contains identifiers such as `V029`
and `14.11`.

**The two tables do not carry the same columns, and the completed side is the
awkward one.** The archive is `| Plan | Description | Date |`: no title, no
priority, no effort, and a bare number rather than a link in the Plan cell. So a
completed item is assembled differently from a planned one.

- `title` is the **bolded lead** of the Description cell, not the whole cell.
- `priority` and `effort` do not exist for completed work and are not emitted.
  Only planned items carry them.
- `href` is synthesized from the plan number by globbing `docs/plans/plan_<n>_*.md`.
  **That glob is ambiguous** — `plan_145_*` matches nine files, the main document
  plus eight stage handoffs. Prefer the main document and treat any remaining
  ambiguity as a build failure rather than picking one silently.
- `summary` is the sentence that follows the bolded lead. This is the one field
  a generator cannot be trusted with alone: those cells run to several hundred
  words of incident narrative naming migrations, services, columns and object
  paths, which is what §4 bars from the feed. Extraction is the default, and
  **Gate 1d requires a human to read the four rows actually published.** Four
  rows is a cheap review; the alternative — a new column in an archive Plan 146
  owns — is a change to someone else's file for a four-row problem.

The script supports `--check`: regenerate in memory, compare with the committed
JSON, and exit non-zero on drift. CI runs that mode and also validates unique
build order, score range 0-100, newest-first completion dates, local plan-link
existence, and the public schema. This makes a roadmap edit fail visibly when its
public projection was not refreshed.

**Effort is validated on its leading token, not on the whole cell.** The build
order's effort column carries qualifiers that plan sizing needs and the public
schema does not — `M + first observed window`, `S + 7d observation`, `XL,
research-gated`, `XS each`. Five of the twenty-two rows would fail a strict
`XS|S|M|L|XL` match, so a naive vocabulary check fails on the day it is written.
Parse and emit the leading token; leave the qualifier in `PLANS.md`, where it is
doing real work.

### 1e. Weekly recap projection

`docs/recaps/` holds one file per complete week, unbroken from `2026-02-01.md`
to `2026-08-30.md` — 31 files at the time of writing. [Plan
146](plan_146_planning_system.md) carries a 2026-09-14 gate on that habit
continuing, so the corpus grows on its own and needs no new discipline from this
plan.

**This is Stage 1d's pattern pointed at a second directory:** source-controlled
Markdown, a deterministic build-time projection, a static artifact under
`ops/static_ops/`, a `--check` mode CI runs, and no repository call in a request.
Three things differ, and they are the whole of the work.

**1. Rendering happens at build time, in Python, and produces HTML.**
`project-updates.json` is a small structured extract that `info.js` renders with
`textContent`. A recap is long-form prose with tables, headings and internal
anchors; `textContent` cannot render it and `innerHTML` is barred by Stage 4
item 3 and by Stage 3c's CSP. So `scripts/build_public_recaps.py` renders each
file to static HTML at image-build time, adding one Markdown library to the ops
build. No Markdown reaches the browser and no Markdown is parsed in a request.

**2. Links must be rewritten, and the rewrite is a correctness rule.** Recaps
link relatively to plan documents (`../plans/plan_145_april_cutover_reconciliation.md`)
and use internal anchors (`#merges`). **Classify on the `docs/`-relative path,
not on the `plans/` directory alone.** The corpus holds 129 `../plans/…` links
*and* 4 `../planning/…` links — a rule that recognises only the first refuses to
build against files that are already committed. Any repository-relative path
resolves to the same GitHub blob URL Stage 1d already emits; internal anchors
stay internal. **A relative link the generator cannot classify is a build
failure, not a passthrough** — a silent passthrough is how a `docs/`-relative
path becomes a 404 on the public site.

**3. Publication is an editorial decision, made once, and recorded.** Per the
truth contract's §5, the recaps are already public in the repository, so this is
not about disclosure. It is about which of 31 weeks is the reading experience
worth advertising, and about the §3 narrowing that authored copy obeys and
recaps predate. Two candidate policies:

| Policy | Cost |
|---|---|
| Publish from a chosen start date forward | One decision; the archive stays on GitHub; the site's oldest page is a week the author chose |
| Publish all 31 after a read-through | 31 files of review before PR A can land, against prose that will not be revised afterwards |

The generator must not decide this. It reads an explicit published-from date (or
an explicit allow-list) from a committed source, and a recap outside it is not
rendered.

`--check` regenerates in memory and exits non-zero on drift, exactly as 1d's
does. CI additionally validates: every rendered file has a parseable week date
matching its filename; the index is newest-first with no gap between the
published-from date and the newest file; no unclassifiable relative link
survives; and no recap outside the published set produced output.

**Gate 1e:** the publication policy is written down and committed, the generator
refuses an unclassifiable link rather than emitting it, and `--check` fails on a
new recap that has not been regenerated.

### 1f. Reconcile against the published writings

**The corpus, as supplied 2026-08-31.** URLs are recorded without their
`trackingId` and `lipi` query parameters, which are per-session identifiers from
the referring profile view and are not part of the article's address.

| # | Title | Published | URL |
|---|---|---|---|
| A | I Built a Vehicle Pricing Data Platform From Scratch. Here's What I Actually Learned About Data Engineering. | 2026-05-08 | [`/pulse/…-heres-miller-hrb7e`](https://www.linkedin.com/pulse/i-built-vehicle-pricing-data-platform-from-scratch-heres-miller-hrb7e) |
| B | Which Rows Belong in a Test Fixture, and What Has to Come With Them | 2026-07-21 | [`/pulse/…-andrew-miller-i2z8c`](https://www.linkedin.com/pulse/which-rows-belong-test-fixture-what-has-come-them-andrew-miller-i2z8c) |
| C | I Thought Compression Was a Codec. A Full Disk Taught Me It Was Architecture. | 2026-08-17 | [`/pulse/…-andrew-miller-nxvrc`](https://www.linkedin.com/pulse/i-thought-compression-codec-full-disk-taught-me-andrew-miller-nxvrc) |
| D | *(planned)* The Plan 145 cutover, loosely based on [its post-mortem draft](../evidence/plan_145_post_mortem_draft.md) | — | not yet written |

#### First pass, 2026-08-31: what the corpus is worth as a source

**All three are better front-door prose than anything this plan has drawn from
so far**, and C in particular reaches conclusions the surfaces state more weakly:

- **C names the cost model error better than the README does.** "I had built a
  cost model with one variable: bytes. Adding the cost of an object's mere
  existence reversed the decision. My estimate was not wrong about bytes. It was
  measuring the wrong noun." That is the storage lesson in three sentences.
- **C supplies the framing for why bronze exists at all** — bronze is what
  arrived, silver and mart are opinions about what it means, so an optimization
  that keeps only what today's parser understands "quietly turns bronze into
  another opinion."
- **C also records a failure the surfaces do not mention**: section-level
  deduplication was built, proven lossless, and *rejected* because per-object
  overhead made it 223% worse. A portfolio page that shows a rejected design is
  more credible than one that shows only what worked.
- **B is the source for the testing section's fourth layer.** Cross-engine
  equivalence is currently one sentence on the landing page; B explains what a
  selector is and why coverage's arrow is reversed — branches enumerated first,
  then data shopped for in production to reach them.
- **A is the weakest as a source and the strongest as a warning.** See below.

#### The audit: A is a public surface carrying the drift this plan removes

Article A is dated 2026-05-08 and its "The Numbers" section states, as current
fact, **ten claims the Gate 0 disposition table has since disposed of**:

| Claim in A | Gate 0 row | Current |
|---|---|---|
| "13+ make/model pairs" | 1 | live stat, 14 |
| "12 Airflow DAGs" | 2 | 15 |
| "17 dbt models" | 3 | 23 |
| "39 Flyway migrations" | 4 | 49 |
| "974 tests" / "950+" / "705 unit" / "37 API integration" | 5 | 3,661 collected |
| "six services" | 6 | more than two dozen long-running |
| "9 Telegram alert rules" | *(found in 1b)* | 22 |
| "FlareSolverr handles JavaScript challenge pages" | 13 | `trawl` is the live solver |
| "concurrent scraping with **anti-detection**" | §1b | the barred marketing register, twice |
| "30-day bronze retention window … reprocessing is only possible within that window" | — | **superseded by C**: packing retains every page, with ~3.5 years of runway |

The last row is the sharpest, because **A and C contradict each other** and both
are published under the same name. A tells a reader bronze is discarded after 30
days; C tells the same reader every page is retained and explains the packing
that made it possible. No repository change can reconcile that; only an edit or
a dated note can.

A also closes with "Live at: cartracker.info/info", which Stage 2 turns into a
permanent redirect. That one degrades gracefully and needs no action.

**This is the finding the scope question was asked about**, and it is no longer
hypothetical: the drift this plan is removing from two surfaces is live on a
third, in an article that is the most likely entry point a reader has.

#### What the corpus caught in *this* repository

Reconciliation ran both directions, and the corpus won one.

**"Inode pressure fell by roughly two thirds" is unsourced, and the correction
first attempted here was wrong in the same way.** The phrase entered in Stage 1a
and is not drawn from `OPERATIONAL_ENGINEERING_OVERVIEW`, which discusses inodes
only qualitatively. C supplies measurements, and its campaign-start figure
cross-checks exactly against [Plan 131](plan_131_packed_cold_storage.md) §156 —
9,101,670 inodes used, in both.

The first correction replaced it with "fell by more than half", from the
whole-volume reading. **That is true and it answers the wrong question**, which
is the same defect as the original, one denominator over:

| Denominator | Before | After | Fall |
|---|---:|---:|---:|
| Inodes the packed artifacts themselves cost | 6,053,495 | 497 | **99.99%**, a 12,173× reduction |
| Inodes used on the whole volume | 9,101,670 | 3,897,963 | 57.17% |

Both are computed from C: 2,702,453 source objects across April–June became 222
packs and sidecars, at the ~2.24 inodes per object C measures for this bucket.
The per-month form C states independently agrees — "an unpacked month costs
roughly 2.2 million inodes; a packed month costs about 300", and May's 1,021,266
objects do compute to ~2.29M.

**The 849,290-inode gap between the two rows is the pipeline continuing to
work.** Packing should have freed ~6.05M; the volume only shows ~5.20M
recovered, because bronze kept arriving throughout — C says so directly, "while
scraping never stopped". So the whole-volume percentage charges packing for
ongoing ingestion and for every unrelated consumer on the filesystem, and
reports a modest 57% for a mechanism that removed 99.99% of what it was pointed
at.

**This is the Compose-denominator problem again**, and this plan has already
been caught by it once. Gate 0 found that "two different sets both have 28
members" and that the defensible-looking one excluded the live scrape path. Same
class: an arithmetically correct number answering a question nobody asked.

**Four candidate framings. The surfaces currently carry (d), the conservative
one, pending a decision:**

| | Framing | Cost |
|---|---|---|
| **a** | "2.7 million bronze objects became 222" | Directly measured, one obvious denominator, no inode arithmetic and no per-object constant. Historical and dated, so it cannot drift |
| **b** | "An unpacked month costs ~2.2 million inodes; a packed month costs about 300" | States the *mechanism* rather than a campaign total, so it stays true for every future month. Needs the reader to care about inodes |
| **c** | "Inodes stopped being the binding constraint; bytes are again" | The engineering conclusion, and the one C itself lands on. Carries no number to drift, and says the least to a skimming reader |
| **d** | "The volume's inode use fell by more than half" | True, and understates the mechanism by a factor of roughly seven hundred |

**Decided 2026-08-31 and applied to both surfaces: (a) as the headline, (c) as
the consequence, and no percentage at all.** Both now read "the first three
packed months turned 2.7 million bronze objects into 222, and inodes stopped
being the binding constraint — bytes are again."

(a) is the number a reader remembers and the only one whose denominator needs no
explanation; (c) is what it bought. The phrasing is anchored to **the first
three** packed months rather than to "the packed months", so a fourth does not
make a true sentence false — July is still unpacked and C records that there is
no recurring lifecycle DAG yet.

(b) stays available and belongs in
[OPERATIONAL_ENGINEERING_OVERVIEW](../OPERATIONAL_ENGINEERING_OVERVIEW.md) §4.3
rather than on a landing page: "an unpacked month costs roughly 2.2 million
inodes; a packed month costs about 300" states the mechanism, so it stays true
for every future month, and a runbook reader is the one who needs it.

**No percentage survives on either surface**, which is the durable part of this
decision. Both wrong answers here were percentages whose denominators were
unstated; the object count has one denominator and it is in the sentence.

Note what this means for the corpus's standing: **C is more current than the
plan documents.** Plan 131 records the campaign's start; nothing in `docs/`
records where it finished. The article does.

#### Still open after this pass

- **The surface question is unanswered.** The three options above stand, now
  with a measured ten-row audit behind them rather than a hypothesis.
- **Nothing from B or C has been drawn onto a surface yet.** This pass audited
  and reconciled; it did not rewrite copy. The specific candidates named above —
  C's cost-model sentence, C's bronze-as-arrival framing, C's rejected-design
  story, B's reversed coverage arrow — are the shortlist for a copy pass, and
  each still owes Gate 1f its verification against the tree.
- **Article D is planned, and it is not like the others.** Its source is
  [`docs/evidence/plan_145_post_mortem_draft.md`](../evidence/plan_145_post_mortem_draft.md),
  **not** the Plan 145 document — a distinction that changes what the gate has
  to do.

  A plan says what should happen and goes stale when the tree moves past it. A
  post-mortem says what did happen on dated evidence, which is the truth
  contract's §5 category: correct as of its date and never revised to match a
  later truth. **So D barely needs the staleness check A–C needed.** Its facts
  are historical by construction.

  What it needs instead is the opposite filter, and a stronger one. The draft is
  795 lines of internal-register material: §10 is operational incidents, §6.4
  draws on working transcripts, §3.1 reconstructs a revert from inside, and §9
  carries cost and population arithmetic. The truth contract's §4 bars exactly
  this from public surfaces — "incident payloads, approval records, production
  object keys, and other operational detail". A–C were published first and
  reconciled afterwards; **D is internal first and published afterwards, so its
  risk runs the other way**: not "is this still true?" but "what in here should
  never leave the repository in this register?"

  The draft anticipates this. §12 is "Draft lessons — for the maintainer to cut
  or keep" and §13 is "Open questions for the narrative", so the document is
  already staged for exactly this pass rather than needing to be mined for it.

  One thing D should carry that A–C did not: **the post-mortem's honesty is the
  reason to publish it at all.** §7 records that the parser control the whole
  plan rested on failed, and §8 that the plan's own explanation of the
  near-duplicate cohort was wrong. An article that keeps those is worth more
  than one that keeps the recovery and drops the two admissions.

  **Gate for D, before publication rather than after:** every claim about
  *current* state — as opposed to what was true during the cutover — is checked
  against the tree, and the §4 register filter is applied to the incident,
  transcript, and arithmetic sections. This is the cheapest point in the cycle
  to catch both, and the only one where fixing costs nothing. Article A is the
  standing argument for doing it here: it has carried ten disposed-of claims
  since May because nothing checked it before it went out.


## Stage 2 — Make the landing page discoverable

1. Add the exact-root public Caddy handler and keep `/dashboard*` protected.
2. Redirect external `/info` requests to `/` and add a canonical link.
3. **Add public Caddy handlers for `/recaps` and `/recaps/YYYY-MM-DD`**, served
   without OAuth on the same unauthenticated path as the root. The route table
   above, Gate 2 and Stage 5 all require these routes; they need a handler of
   their own because the authenticated catch-all would otherwise swallow them,
   which is the same trap item 1 describes and fails the same silent way.
4. Serve public `robots.txt` and `sitemap.xml` without OAuth.
5. Add a descriptive title and meta description.
6. Add Open Graph and Twitter metadata using the existing cover artwork or a
   purpose-built static preview.
7. Add favicon links and JSON-LD for the software project and author.
8. Update README, dashboard sidebar, email links, and other first-party links to
   use the canonical root or explicit `/dashboard` route as appropriate.

Search metadata must describe only the public page. Protected application paths
should not be listed in the sitemap. The recap index and every published recap
page are public and belong in the sitemap; the generator in Stage 1e emits that
URL list so the sitemap cannot drift from what was actually rendered.

### The catch-all is what serves the dashboard, and this stage is what moves it

**Read this before editing the Caddyfile.** Item 1 above is not a small change,
because the `/dashboard*` block is not what makes the dashboard work.

`dashboard/Dockerfile` runs Streamlit with no `--server.baseUrlPath`, so
Streamlit believes it is mounted at `/` and serves its own machinery from the
root: `/_stcore/health` (which the Compose healthcheck calls at exactly that
path), `/_stcore/stream` for the websocket, and the static bundle. Nothing
rewrites those paths. They resolve today only because the Caddyfile's final
`handle { … reverse_proxy dashboard:8501 }` catches everything unmatched and
sends it to Streamlit.

So the current root behaviour is load-bearing in a way no configuration states
and no test asserts. This stage takes the root away from it.

The requirement:

9. Keep an authenticated catch-all reaching `dashboard:8501` for the Streamlit
   root-served paths, and make the public root an **exact match** on `/` rather
   than a prefix that swallows them. Removing or narrowing the catch-all without
   giving Streamlit a base path breaks the dashboard's assets and websocket while
   leaving `/dashboard` itself returning 200.

That last clause is the trap. **A broken dashboard would still pass Gate 2 as
originally written**, because Gate 2 only asserts that `/dashboard` *enters
OAuth* — it never completes a sign-in and never loads a page. The failure would
be invisible to every check in this plan and would surface as a blank dashboard
after deploy.

[Plan 165](plan_165_service_subdomain_routing.md) is the eventual fix: its own
host gives Streamlit its whole origin back and removes the catch-all dependency
entirely. Until then this plan must preserve the coupling deliberately rather
than by accident, which means asserting it.

**Gate 2:** a fresh unauthenticated session gets HTTP 200 at `/`; `/dashboard`
enters OAuth; `/info` redirects once to `/`; the recap index and one recap page
return 200 without an OAuth redirect; robots and sitemap return their real
content rather than a Google sign-in page; **and an authenticated `viewer`
session loads the dashboard with its websocket connected and no failed
`/_stcore/*` request** — verified by loading the page, not by a status code on
`/dashboard`.

## Stage 3 — Accessibility and static-asset performance

**Stage 1b settled three of this stage's items early and moved a fourth's
before-number.** They are marked below rather than deleted, so the stage reads as
what is left rather than as a list where the reader has to work out which parts
already happened.

### 3a. Semantic interactions

- Replace clickable service-card and highlight `<div>` elements with buttons plus
  associated panels, or native `<details>/<summary>` elements.
- Expose `aria-expanded`, `aria-controls`, focus state, and Escape/collapse
  behavior where custom controls remain.
- ~~Restore a valid heading hierarchy (`h1` -> `h2` -> `h3`).~~ **Done in 1b for
  the landing page.** The outline opened two `h2` -> `h4` gaps, at the decision
  cards and the evidence cards; card titles moved to `h3` and their in-panel
  subheads to `h4`. `test_the_heading_outline_skips_no_level` walks the rendered
  outline and fails on any jump greater than one, so this cannot regress. **What
  remains is 3d's generator**, which must not emit a skipped level from source
  Markdown — a different mechanism, still open.
- Do not depend on colour alone for **active state**. *(The pipeline-layer half
  of this item is done: 1b replaced the strip with a diagram whose every node
  states its layer as badge text and whose control loop is dashed as well as
  labelled, held by `test_the_control_loop_is_not_signalled_by_colour_alone`.)*
  Active state is still colour-only — `.service-card.active` and
  `.highlight-card.active` signal with `border-color` plus a `box-shadow` ring
  and nothing else — and the buttons this stage introduces are where that gets
  fixed, since `aria-expanded` carries it for free.
- Respect `prefers-reduced-motion` and avoid autoplay for those users.
- **Preserve the diagram's text equivalent.** The `<svg>` carries `role="img"`,
  a `<title>`, and a `<desc>` stating both forks and the control loop in prose.
  That description *is* the diagram's argument for a reader who cannot see it,
  and is asserted by
  `test_the_diagram_is_described_for_a_reader_who_cannot_see_it`. Any rework of
  this section keeps it.

### 3b. Demo media

**Decide whether the hero media should exist at all, before deciding its
format.** This item was written as a size problem, and Stage 1b's portfolio pass
found the premise underneath it is the open question.

The checked-in `demo.mp4` is 41,699,885 bytes and shows the Streamlit dashboard.
[Plan 150](plan_150_analytics_product_and_bi_serving_layer.md) records that the
dashboard is the project's *weakest* public surface — "the public product
presents only a small Streamlit dashboard with basic graphs", a mismatch that
"limits the portfolio value of work that already exists" — at priority 68 and
effort **XL, research-gated**. So it will not improve on this plan's timescale.

If that judgment holds, the page currently leads with its weakest asset, and
compressing the video only makes a weaker first impression arrive faster. 1b
already moved the visual weight to the architecture diagram, which is the
honest hero while the platform is the product. **Three options, and this stage
picks one:**

| Option | Cost |
|---|---|
| Remove the hero media entirely | The diagram carries the section; nothing to encode, and the largest asset on the page disappears |
| Keep a still, not a video | One poster image, an accessible caption, kilobytes rather than megabytes |
| Keep a video, re-encoded | The full list below, for an asset whose subject Plan 150 says is not yet worth showing |

If a video survives that decision, it needs:

- a poster image that communicates the dashboard before playback;
- a WebM primary plus compressed MP4 fallback;
- `preload="metadata"` or `preload="none"`;
- controls, an accessible label, and a short text transcript/caption;
- an 8 MiB maximum per video asset and no eager full-video transfer.

**Do not delete `demo.mp4` from the repository as part of removing it from the
page.** Those are separate decisions, and the second one is not this plan's.

### 3c. Local assets and response policy

- Extract inline CSS and JavaScript into versioned `static_ops` files.
- Self-host PicoCSS and required icons, preserving license notices, so the public
  page can operate under a same-origin CSP.
- Cache fingerprinted static assets for one year with `immutable`; keep HTML
  uncached or on a short revalidation policy.
- Enable gzip or Brotli for HTML, CSS, JavaScript, SVG, and JSON/XML responses.
- Apply public-route headers: CSP, `X-Content-Type-Options`, `Referrer-Policy`,
  `Permissions-Policy`, and `frame-ancestors 'none'`.

**This stage's before-number moved, and it moved the wrong way.** The Stage 0
baseline measured `/info` at 54,352 bytes of uncompressed HTML. Stage 1b rebuilt
the page and the rendered document is now roughly 78 KB — the section count
grew, the diagram is inline SVG, and the stylesheet gained the diagram, table,
and evolution blocks. **Compression is therefore worth more than the baseline
implied, not less**, and the extraction in item 1 now has a larger stylesheet to
move.

Two things deliberately did *not* get worse. The diagram is inline SVG and the
layer table is markup, so **1b added no new third-party request** — the CDN
problem is still exactly the two hosts the baseline named, PicoCSS on jsdelivr
and twelve icons on simpleicons. And because the SVG is inline it needs no
`img-src` allowance and satisfies a same-origin CSP as written.

Do not apply a landing-page CSP blindly to Grafana, Airflow, Streamlit, MinIO, or
OAuth routes; scope the header block to the public handlers.

### 3d. Recap presentation

The recap pages are the one place on the public surface with a long-form reading
requirement, and they get it from the same local stylesheet rather than a second
design.

- A generated index at `/recaps`: newest first, each row the week and a
  one-line lead taken from the file, with no client-side fetch. This page is
  static HTML, unlike the "Recent work" section, because it has no freshness
  contract to honour.
- Each recap page carries its week, an explicit "point-in-time record" note per
  the truth contract's §5, and a link back to `/`.
- Reading measures: a bounded line length, the same heading hierarchy rule as
  3a (`h1` → `h2` → `h3`, and the generator must not emit a skipped level from
  the source Markdown), and tables inside a horizontally scrollable container so
  the recaps' wide commit tables cannot overflow the page at 360 px.
  **Reuse 1b's container rather than inventing a second one:** `.layer-table-wrap`
  and `.diagram-wrap` are an `overflow-x: auto` parent around a `min-width` child,
  which is the pattern that keeps a wide element scrolling inside its own strip
  while `body.scrollWidth` stays at 360.
- The pages share `info.css`, load no JavaScript, and satisfy Stage 3c's CSP
  without exception. A recap page that needs a script has been over-built.

**Gate 3:** all page functions are usable with keyboard only, reduced-motion
users do not receive autoplay, no third-party request is required to render the
page, the initial page view does not download the full demo *(or the demo is
gone, per 3b)*, and every recap page renders with JavaScript disabled and no
horizontal overflow at 360 px.

Gate 3 does not re-litigate what 1b already holds under test: the heading
outline, the diagram's text equivalent, the non-colour encoding of the control
loop, and the absence of page-level overflow at 360 px.

## Stage 4 — Present the Plan 143 snapshot without request-time dependencies

As written on 2026-08-17, the handler opened DuckDB independently for four stats
and derived "last pipeline run" from completed queue rows that hourly cleanup
removes. Plan 143 owned removing both reads, producing the versioned serving
snapshot, replacing that field with mart-derived `analytics_data_through_iso`,
and loading an immutable presentation cache inside `ops`.

**Plan 143 completed on 2026-08-20**, so that work has landed and this stage
consumes a contract that already exists rather than waiting on one. Re-read the
shipped snapshot schema before implementing; do not implement against the shape
sketched in this document, which predates it.

This plan owns only the public presentation contract:

1. Render full, partial, stale, and empty Plan 143 snapshots without sleeping,
   retrying, opening a database, or calling an upstream service in the request.
2. Label the mart-derived timestamp "Analytics data through."
3. Show a subtle "temporarily unavailable" or stale state where appropriate;
   missing analytics must never make the narrative fail.
4. Keep freshness wording consistent with Plan 143's snapshot schema rather
   than inventing a second page-only timestamp.
5. Do not create `ops` SQL, a DuckDB connection, or another background analytics
   collector while implementing the visual refresh.

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

**Gate 4:** `/` performs no database or upstream-network call, remains responsive
during a dbt write lock and Postgres outage, clearly distinguishes stale cached
stats from fresh ones, and loads recent work without a runtime repository call.

## Stage 5 — Regression coverage

Add tests for:

- Plan 143 snapshot formatting and full, partial, stale, empty, and
  unsupported-version presentation; aggregation, persistence, concurrency, and
  failure isolation are tested in Plan 143;
- landing-template rendering with full, partial, stale, and empty stats;
- public-roadmap generation, deterministic `--check`, schema validation, score
  and effort constraints, ordering, item caps, and broken local plan links;
- recap generation: deterministic `--check`, filename-to-week agreement, index
  ordering and gap detection, plan-link rewriting to GitHub, internal-anchor
  preservation, **build failure on an unclassifiable relative link**, heading
  hierarchy without skipped levels, and a recap outside the published set
  producing no output;
- project-updates progressive enhancement with valid, unavailable, malformed,
  empty, and unsupported-schema JSON;
- semantic controls, required metadata, canonical URL, media fallback, and the
  absence of the known stale phrases;
- Caddy route ordering and access requirements;
- **the Streamlit root-path coupling**: that the public root matches `/` exactly
  and does not swallow `/_stcore/*`, and that an authenticated catch-all still
  reaches `dashboard:8501`. This asserts a behaviour the tree currently relies on
  without stating, and it is the one Stage 2 regression that a status code on
  `/dashboard` cannot detect;
- public security and cache headers;
- robots and sitemap content;
- README links and the production-versus-experimental wording contract.

### Open question: is there anything to assert about denominators?

**Not a requirement, and possibly not a test at all.** Recorded here because
Stage 5 is where drift detection lives, and this plan has now been caught twice
by a defect its current scope would not have found.

Both times the number was arithmetically correct and answered a question nobody
asked:

| Where | The number | Why it was wrong |
|---|---|---|
| Gate 0, Compose services | "28 without a profile gate" | A true count of the wrong set — it excluded `trawl` and `redis-trawl`, the live scrape path |
| Stage 1f, inodes | "fell by roughly two thirds", then "by more than half" | Whole-volume readings for a mechanism that removed 99.99% of the inodes it was pointed at. **The first correction repeated the original's mistake** |

Stage 5's scope is "the absence of the known stale phrases", which is an
assertion about wording. Neither of these was a stale phrase. Both were *new*
wordings, individually true, and wrong about what they were counting — so a
phrase list would have passed all four of them.

**The reason this is a question and not a task** is that the hard part is not
mechanical. A test can assert that a published number appears somewhere with its
denominator named; it cannot know which denominator is the honest one. That
judgment is what Gate 0 exercised when it chose `EXPECTED_SERVICES` over the
profile count, and it is what picked the object count over the volume
percentage. Encoding it as a rule risks a test that passes on a stated-but-wrong
denominator, which is worse than no test, because it certifies the judgment it
cannot make.

Some possible answers, none of them owed:

- **Nothing.** Two instances is a pattern of two. Both were caught by review, which
  may be the correct control for a judgment call.
- **An editorial line, not a test.** Stage 1c already creates a review checklist;
  "every published count names the set it counts" is one line there and costs
  nothing.
- **A narrow assertion.** The surfaces currently publish almost no bare integers
  by design — the rounding rule in the truth contract's §3 did most of this work
  already. A test that fails on a *new* bare integer entering public copy would
  catch the shape without judging the denominator. It would also be noisy, since
  dates, versions, and the deal score's 0–100 are all legitimate.

Stage 5 may reasonably conclude that the §3 rounding rule plus review is
sufficient and that this needs no coverage. Recording the question is the point;
answering it is not a gate.

CI verification should include:

```text
GET /                 -> 200, no OAuth redirect
GET /info             -> 308 -> /
GET /recaps           -> 200 text/html, no OAuth redirect
GET /recaps/2026-08-30 -> 200 text/html, no OAuth redirect
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
2. Confirm the Plan 143 presentation cache has either a fresh snapshot or a
   valid empty state.
3. Confirm the project-updates JSON matches the roadmap, has the expected cache
   policy, and renders both lists without blocking the page.
3b. Confirm the generated recap set in the image matches `docs/recaps/` under the
   published-from policy, that the index has no gap, and that the sitemap lists
   exactly the pages that were rendered.
4. Apply the Caddy route change.
5. Run the unauthenticated route matrix from an external client.
6. Sign in as `viewer`, `observer`, and `admin` and verify existing boundaries.
   As `viewer`, **open the dashboard and confirm the websocket connects and no
   `/_stcore/*` request fails** — the root move is the change most likely to break
   it, and a status code on `/dashboard` will not show it.
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
| `ops/routers/info.py` | Render the Plan 143 presentation cache; canonical public responses |
| `ops/public_stats.py` | **Plan 143-owned** snapshot reader/cache; this plan changes presentation only |
| `ops/app.py` | Preserve the Plan 143 cache lifecycle; no analytics collector added here |
| `ops/templates/info.html` | Correct copy and semantic markup |
| `ops/static_ops/info.css` | Extracted page styles |
| `ops/static_ops/info.js` | Accessible progressive enhancement |
| `ops/static_ops/project-updates.json` | Deterministic public projection of planned and completed work |
| `ops/static_ops/*` | Local vendor assets, poster, optimized video, favicon/social image |
| `scripts/build_public_roadmap.py` | Parse the build order and the completion archive, validate them, and generate/check the JSON snapshot |
| `scripts/build_public_recaps.py` | Render `docs/recaps/` to static HTML, rewrite links, emit the index and sitemap URL list, and `--check` for drift |
| `ops/routers/info.py` or a recap router | Serve the generated recap index and pages as static responses |
| `ops/requirements.txt` | One Markdown rendering library, used at build time only |
| `dashboard/app.py` | Canonical portfolio and dashboard links |
| `ops/email.py` | Canonical destinations where needed |
| `tests/ops/routers/test_info.py` | Stats and template behavior |
| `tests/test_observability_config.py` or focused Caddy test | Public/protected route contract and headers |
| `docs/PLANS.md` | Ordered/scored plan source; the completion archive stays in `docs/planning/completed_plans.md` |
| `docs/recaps/*.md` | Unchanged as a source; the recap publication policy is committed alongside them |
| `.github/workflows/ci.yml` | Reject stale or invalid project-updates and recap snapshots |

## Recommended PR sequence

1. **PR A — Truth and roadmap pass:** README and landing copy, accurate
   architecture, current versus experimental, scored roadmap, deterministic
   public projection, the recap publication policy and its generator, and CI
   drift checks; no routing change.
2. **PR B — Public root:** Caddy route contract, canonical metadata, robots,
   sitemap including the generated recap URLs, link updates, and route tests.
3. **PR C — Frontend quality:** semantic interactions, dynamically loaded work
   feed, recap index and page presentation, extracted/local assets, optimized
   media, CSP, caching, and accessibility evidence.
4. **PR D — Stats presentation:** consume the already-landed Plan 143 snapshot,
   add stale/partial/empty UI states, and verify no request-time dependency.

PR A can ship independently. PRs B and C should be reviewed together for CSP and
asset-path compatibility. PR D requires Plan 143's snapshot contract, which
landed on 2026-08-20, and must preserve the current soft-failure behavior
throughout.

The recap work splits across A and C on the same seam as the roadmap work —
generation with the other build-time projections, presentation with the other
frontend. If the publication policy in Stage 1e turns out to need a long
read-through, the generator can land in PR A behind a published-from date that
admits only recent weeks, and the date widened later without touching code.

## Completion criteria

Plan 138 is complete only when:

- `/` is the canonical public landing page and `/dashboard` remains protected,
  with the Streamlit root-path coupling asserted by a test rather than held by
  catch-all ordering;
- `/info`, robots, sitemap, and every first-party link follow the route contract;
- README and landing copy contain none of the audited factual contradictions;
- production and experimental architecture are visibly separated;
- public requests do not connect to DuckDB or Postgres;
- the page remains useful with no stats available;
- planned and recently completed work load from a deterministic, CI-validated
  source-control snapshot, with a useful no-JavaScript/failure fallback;
- the weekly recaps are published under a written policy, generated from
  `docs/recaps/` at build time, reachable from the landing page, and readable
  with JavaScript disabled at 360 px;
- adding a recap to `docs/recaps/` without regenerating fails CI;
- the demo is bounded, lazy, accessible, and cached;
- interactive content works with keyboard and screen reader semantics;
- scoped public security headers and local assets are in production;
- automated tests and the external route matrix pass;
- a mobile Lighthouse report and screenshots are recorded in the closing PR;
- `docs/PLANS.md` and `docs/planning/completed_plans.md` record the final deployment date
  and measured before/after results.
