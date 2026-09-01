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
| **1c** Cross-surface consistency (CAR-56) | Built 2026-08-31 as a review skill and commit hook **rather than the tests §1c specifies** — see the evidence below for the drift record that decided it. Exit check 2 is unmet as written |
| **1d** Public roadmap projection (CAR-57, PR #326) | Merged to `master` on 2026-09-01, **soaking and undeployed** — the template and the artifact changed, the live page has not. Gate 1d closed on four authored `## Public summary` sections rather than a one-time read: the generator names every plan it had to extract, so the gate is a shrinking worklist rather than a recurring one |
| **1e** Weekly recap projection (CAR-58, PR #331) | **Built 2026-09-01, in review** — 20 of 31 weeks published behind a per-file `**Publish:**` marker, which is the policy this slice was asked to decide. The classifier turned out to have **four** classes, not three: the fourth is six sibling links between recaps, which no `../` rule covers. Exit check 2's "image-build time" is unmet as written — the artifact is committed and `--check`ed, as 1d's is — and check 8's "no gap from the published-from date" has no referent once the policy is a marker rather than a date |
| **1h** Ask at closeout whether the landed work moved a surface | **Raised 2026-09-01, not started.** 1c's gate fires only when a surface is *staged*, so a plan that changes the system and edits no prose never reaches it — which is the class every Gate 0 defect came from. Adds one cheap question to the `close-out` skill, in the same mechanism/name/quantity taxonomy 1c already uses, that proposes and never writes |
| **3d** Recap presentation | Not started. Carries two open decisions raised 2026-09-01: the stylesheet question (3d says the recap pages share `info.css`; the shipped generator inlines `_STYLE`), and whether `/recaps` leads with the newest week in full above the index. Both block 1g's markup |
| **1g** Link the published writing from the landing page | **Raised 2026-09-01 out of 1f, not started.** 1f decided the corpus is not maintained here; 1g links it from `/` anyway, under a list that carries only immutable facts — title, date, URL — because a per-article annotation is a new drift surface that rots every time the tree moves. The weight of the stage is the **add-an-article procedure**: a two-way reconciliation against both surfaces, held by a commit gate rather than by memory |
| **1f** Reconcile against the published writings (CAR-59) | **Audit done 2026-08-31, copy pass built 2026-09-01, in review.** Four framings drawn from the corpus onto both surfaces, each fact verified against the tree and named below. **The scope question has its answer: the articles are out of scope, as point-in-time artifacts** — so the ten disposed-of claims in A and its bronze-retention contradiction with C are accepted and recorded, not fixed |

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
architecture. **That inventory was deliberately bounded on 2026-09-01** — the
author's published writing is public and covers some of the same claims, and
[Stage 1f](#1f-reconcile-against-the-published-writings) recorded the decision
that it stays out of scope: an article is a point-in-time artifact, correct as of
its date and not maintained afterwards. The drift that leaves standing is written
down in 1f rather than treated as an oversight here.

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

#### Stage 1c evidence — 2026-08-31 (CAR-56)

**This stage did not build what it specified, and the reason belongs here
rather than in a commit message.**

Both specified artifacts were built first: a `tests/public_surface.py` contract
module holding the barred-phrase list, thirteen shared claims as regexes and the
shared URLs, and a Layer 0 `tests/test_public_surface.py` asserting them across
both surfaces. Twenty tests passed. Four mutations were checked — a barred
phrase appended to the README, a reworded claim, a URL dropped from the page, a
Gate 1 question left unanswered — each failing the right test alone. Then they
were deleted, on this evidence:

**Every drift this plan has recorded is a surface disagreeing with the
repository, not with the other surface.** Gate 0 found 36 Flyway migrations
against 49, 266 integration tests against 468, 971 tests against 3,661, eleven
containers against more than two dozen. Cross-surface disagreement has *no*
recorded instance: 1b compared twelve shared claims across both files and all
twelve agreed, and a re-check during this stage found the same set still
agreeing. The one README-versus-page disagreement this plan describes is the
deploy lag, and no test closes that.

**Stage 5's open question had already said why a phrase list is insufficient** —
both denominator defects were *new* wordings, individually true, so a phrase
list would have passed all four. Asserting the remainder in regexes also
inverts the incentive: reword "The dashboard reads DuckDB" to "DuckDB backs the
dashboard" and an identical claim fails, with pattern-editing as the cheap
repair. The deleted test's failure message had to instruct the reader not to do
that, which is a design arguing with itself.

**What was built instead.** `.claude/skills/public-surface-check/` is §1c's
review checklist in executable form: it names the two surfaces, takes
`git diff --cached` over them as its entire input, and checks changed claims
against the repository and against the other surface. The scoping is deliberate
— both files are long, and a check that re-derives the architecture on every
commit gets switched off. `scripts/public_surface_gate.py` is a `PreToolUse`
hook blocking a commit that stages either surface until the skill stamps that
staged digest; re-staging invalidates the stamp, so a pass never covers unread
content. Verified by direct invocation: blocks (2), clears on stamp (0),
re-closes on re-stage (2), passes through non-commit commands and malformed
stdin (0), and fails open on unparseable input. `.gitignore` now tracks
`.claude/settings.json` — hooks are repository policy rather than per-machine
preference, and this is the repository's first.

**What the hook does not do.** It is a Claude Code hook on the Bash tool, not a
git hook: a commit typed in a terminal is unaffected. And it enforces a stop,
not the check — it blocks and names the skill, and something must still choose
to run it honestly. That is the trust `commit-plan-attribution` already runs on.

**The optional source module was declined.** `README.md` is static markdown and
can import nothing, so such a module could feed at most one surface. Templating
the page's URLs through `ops/routers/info.py` would move prose plumbing into a
production request path while leaving the README hand-written and the agreement
still unasserted. The agreement is created by the check, so the check is what
got built.

**Exit check 2 is unmet as written.** "Tests prevent the known conflicting
numeric phrases from returning" has no test behind it. The argument for moving
it to Stage 1f — where the ten disposed-of claims in the published writings are
the actual vector for stale text re-entering — is recorded but not acted on.

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

**Revised 2026-09-01, during Stage 1d's build.** The paragraph above is kept as
written because its reasoning still holds and the mechanism it describes is
still the fallback. What it missed is a third option it did not consider: not a
column in Plan 146's archive, but a **`## Public summary` section in the plan's
own document**, which is nobody else's file. The generator prefers that section
and falls back to extraction where there is none.

The change is worth more than the four rows that prompted it, for two reasons.

- **It moves the writing to the moment the writer is already there.** The
  archive Description is authored at the `closeout → archive` transition, by a
  person with the whole plan in their head. Asking for a second, shorter,
  outward-facing sentence at that moment costs almost nothing. Asking a
  generator to manufacture one later, from prose written for a different
  reader, is where §4 was going to be breached.
- **It makes Gate 1d self-extinguishing.** The generator prints the plans it had
  to extract, and that list is the gate's worklist. Under the original design
  the same four rows needed re-reading on every build, forever; under this one
  the gate shrinks each time a plan archives with its section written, and a
  build where every published plan authored its own copy needs no read at all.

`.claude/skills/close-out/SKILL.md` carries the rules for writing one, and the
generator caps a published summary at 320 characters — which fails the build
rather than publishing a paragraph, and points at writing the section as the
fix. The 118 plans archived before this existed have no section and are not
being backfilled; they reach the feed through extraction if they reach it at
all, and the gate names them when they do.

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

#### Stage 1e evidence — 2026-09-01 (CAR-58)

`scripts/build_public_recaps.py` emits `ops/static_ops/recaps/` — 20 pages and a
newest-first `index.html` — from the 31 files in `docs/recaps/`. CI runs
`--check` in the documentation job, beside 1d's.

**The publication policy, decided here: a per-file marker.** Each recap carries
`**Publish:** true|false` in its header block, and that marker is the whole of
what the generator reads. Neither candidate policy in the table above was taken.

- **A published-from date** is one decision and self-maintaining, but it can only
  cut the corpus at one point, and the weeks worth withholding are *scattered
  through* it rather than bunched at the start. Eleven of the thirty-one weeks
  hold no commits — `2026-02-08` through `2026-03-08`, then `05-17`, `06-07`
  through `06-28`, and `08-02` — so every date that clears the early empties
  still publishes five or six later ones.
- **A central allow-list** can say "not this week", but it lives away from the
  thing it describes and needs an edit every week that Plan 146's gate produces
  a recap. That is the second place that goes stale.

The marker travels with the file `plan-week` writes, so the decision is made
where the week is written. It is **required, not defaulted**: a recap with no
marker fails the build, because both defaults are wrong — `true` publishes an
unread week the moment it lands, `false` drops one off the site silently.
`_REQUIRED_RECAP_FIELDS` in `tests/test_planning_docs.py` enforces its presence
alongside `**Window:**` and `**Recapped:**`, reusing a check the documentation
job already ran rather than adding a new one.

**Zero commits was the seeding rule, not the policy.** The initial 20/11 split
was set by commit count and then committed as explicit data. A week with commits
that should stay internal is now a `false` someone writes deliberately, and
nothing recomputes it. `.claude/skills/plan-week/SKILL.md` carries the rule for
new recaps and says so in those terms.

**The section above under-counted the link classes, and missed one entirely.**
Measured against the corpus: 128 `../plans/`, 4 `../planning/` and 1
`../reference/` — the stage table's row was right and the prose here said 129
and named two directories. The fourth class is the one neither said: **six
sibling links between recaps** (`2026-08-02` and `2026-08-09` linking forward to
later weeks), which resolve to `docs/recaps/*.md` and are not covered by a rule
about `../`. They resolve to the neighbouring *page* when that week is published
and fall back to GitHub when it is not — a published page must never link to one
the projection deliberately did not build. All six point forward in time, so no
cross-boundary case exists in the corpus today; the rule exists because the next
one need not be so tidy.

**Links are rewritten in the token stream, not in the text.** A regex over
Markdown cannot tell a link from the same characters inside a fenced code block,
and the corpus holds 42 of those, full of shell commands and paths. Walking
markdown-it's tokens also gives the heading ids the one internal anchor needs:
`2026-08-30.md`'s `#merges` resolves to `<h2 id="merges">`.

**`MarkdownIt("commonmark")` enables raw HTML, and the first draft of this
generator shipped it.** The preset sets `html=True`, because the CommonMark spec
admits raw HTML — so a recap containing a `<script>` tag would have had it copied
verbatim onto a public page, which is exactly what Stage 4 item 3 and Stage 3c's
CSP bar. The generator now passes `html=False` explicitly. It was caught by the
test asserting the property rather than by reading the code, and the docstring
that had confidently called it "the default" was wrong. Regenerating after the
fix produced byte-identical output, which is the evidence that the corpus holds
no HTML tags today.

**Gate 1e is met.** The policy is committed, in the marker and in this section.
The generator raises `RecapBuildError` rather than emitting an unclassifiable
link — covered for absolute paths, `mailto:`, `tel:`, foreign schemes, targets
that do not exist in the tree and targets that escape the repository. `--check`
fails in all three directions, each driven through the real subprocess CI runs:
a new recap nobody regenerated, a page whose recap stopped being published, and
a page edited by hand. `tests/scripts/test_build_public_recaps.py` is 45 tests.

**Not done here, and deliberately.** No route serves these pages yet — Stage 2
opens the path and Stage 3d presents them. The artifacts are committed and the
ops image picks them up through its existing `COPY`, so nothing was added to
`ops/requirements.txt`; `markdown-it-py` is a build-time dependency and is
declared in `requirements-dev.txt` and named in the CI job.

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

#### Stage 1f evidence — 2026-09-01 (CAR-59)

**The surface scope question is answered: the published articles are out of
scope.** They are point-in-time artifacts. An article states what was true on
the day it was published and is not maintained afterwards, so it is read the way
the truth contract's §5 reads a weekly recap — correct as of its date, never
revised to match a later truth. This plan owns `README.md` and
`ops/templates/info.html`, and it does not own the corpus.

What that decision leaves standing, recorded rather than resolved:

- **A and C contradict each other on bronze retention**, and both stay
  published. A tells a reader bronze is discarded after 30 days; C explains the
  packing that retains every page. The audit's ten-row table above is the
  measure of the drift being accepted, and the "as of 2026-05-08" reading is the
  only thing reconciling it.
- **A remains the most likely entry point a reader has.** Accepting the drift
  means accepting that a reader may arrive at the surfaces from a document that
  disagrees with them.
- **Article D is untouched by this call.** D is unpublished, so its
  pre-publication gate is not a maintenance question. If anything this decision
  sharpens it: an article that will never be corrected after publication makes
  the check before publication the only one there is.

**The copy pass: what was drawn onto a surface, and what verified it.** Framing
came from the corpus; every fact was checked against the tree.

| Drawn on | Surface | Verified against |
|---|---|---|
| The cost model had one variable — bytes; adding the cost of an object's existence reversed a settled decision | README "Storage economics", landing `#highlight-storage` summary | [Plan 114](plan_114_sectioned_html_artifact_audit.md) §134, "the bytes-only estimate is optimistic by roughly 8 KB per object", and the reversal itself in the §469 result table |
| Section dedup was built, is lossless, and measured **−223%** | README "Storage economics", landing `#highlight-storage` detail | Plan 114 §43 and §537 (lossless, "correct… simply not a storage win"); §469 (−223% vs like-for-like zstd-9); §245 (~8 KB/object floor = 4 KB directory + 4 KB-rounded file, single-drive backend); §471 (556 section objects for 60 artifacts); §494–497 (256 B chunking: 70.5% gross, −622.9% net). `processing/html_sections.py` confirmed present in the tree |
| Bronze is what arrived; silver and mart are opinions about what it means | README "Storage economics", landing `#highlight-storage` detail | Framing only — no numeric claim. Its consequence is the row below |
| The ~96% projection win is refused, because it flips blocked and unlisted pages to `active` | README "Storage economics", landing `#highlight-storage` detail | [Plan 130](plan_130_parser_input_projection.md) §17 (~82% alone, ~96% stacked with Plan 129); §9 (the only irreversible option on the table); §65 (parser-equivalent 60/60 on active listings); §109–113 (blocked → `active`, unlisted → `active`, unlisted fields lost); §116 ("Both fail toward `active`, which is the worst direction") |
| Coverage's arrow is reversed: branches first, then production data shopped for to reach them | README "Test strategy", landing "How it is tested" | [Plan 120](plan_120_ci_lake_snapshot_delivery.md) §277–287 (branches → named selectors → SQL finding production entities → coverage assertions); §294–317 (the selector table). **Counted in the tree, not read from the plan:** `archiver/config/lake_snapshot_selectors.yml` defines exactly 22 selectors and their names match the plan's table one for one. Closure over VINs/listings/artifacts at §328–348; `tests/integration/dbt/test_selector_dbt_equivalence.py` confirmed present |
| A coverage shortfall is a *reported* shortfall | README "Test strategy", landing "How it is tested" | Deliberately worded to match `require_selector_coverage` being opt-in validation policy (Plan 120 §124) rather than a hard CI failure |

Both surfaces already carried the packing headline decided in the pass above;
this slice did not restate it, and **no percentage was added to either surface**
outside the two dated measurements that name their own denominators (−223%
against a like-for-like zstd-9 baseline, ~96% against unprojected HTML).

**One caveat carried but not surfaced.** Plan 114 §542 notes the 8 KB floor is
backend-specific and that a packed-object layout "would change the arithmetic,
though not the 0.65% cross-listing reuse". Packing is now in production, so the
floor that killed section dedup is no longer the only layout available. The
surfaces report the measurement as what it is — a dated result against the
backend it was run on — and do not claim the conclusion is permanent. Re-opening
sectioning on top of packing is not proposed here; it is noted so the record
shows the caveat was read rather than missed.

**Gate 1f is met on both halves:** the verification table above, and the
recorded answer to the scope question.

Not done, and not in this slice: the surfaces still say nothing about Article D,
and D's own pre-publication gate — the §4 register filter over the post-mortem
draft's incident, transcript, and arithmetic sections — remains unowned by any
issue.

### The writing surface — what 1e/3d and 1g share

**Raised 2026-09-01, before 1g is built.** Two slices are each adding a place to
read long-form writing about this system: 1e/3d publish the weekly recaps, 1g
links the published articles. Designed apart they become two bolted-on content
surfaces with two date treatments, two framings and two links off the landing
page, and a reader looking for "more depth" has to find both. This subsection is
what they hold in common, written once so neither has to guess.

It is written **after 1e shipped its generator and before 1g starts**, which is
the only ordering where alignment is free: 1e's shapes are real code, so 1g
conforms to them rather than both negotiating.

**One section on `/`, two destinations behind it.** The reader sees a single
place to go for more depth. Inside it, the articles are listed *inline* and the
recaps are reached through their index at `/recaps`. The asymmetry is
principled rather than incidental: **inline what is small and finite, index what
grows.** Four articles gaining one or two a year belong on the page; 20 recap
pages growing weekly need an index of their own. This also avoids renaming a
route that Stage 2's contract and the sitemap already carry.

**Three shapes 1g reuses rather than reinvents**, all of them now real in
`scripts/build_public_recaps.py`:

| Shape | Where it lives | Why 1g takes it |
|---|---|---|
| The index row — `<li><a>title</a><span class="meta">…</span></li>` | `render_index`, class `.index-list` | An article entry is title, link and date. That is the same row with a different meta line. Two list treatments for two lists of writing is the fragmentation this subsection exists to prevent |
| The §5 note block — `.note`, text from `_POINT_IN_TIME` | `_page`'s `note` argument | 1g already committed to reusing 3d's point-in-time framing. This names the mechanism instead of the intent |
| The date-in-the-meta-line convention | `Week ending {week_end}` | An article's meta line is its publication date. Same position, same weight, same reason — a dated record must show its date without the reader hunting |

**One thing they do not share, and the page must show it.** A recap link stays on
this site; an article link leaves it for a third-party platform. Listing both
under one heading without marking that is a small lie of omission, and it is
exactly the defect that only appears when the two are designed together — each
alone is internally consistent. **Outbound links are visibly outbound.**

**Both are §5 records, for different reasons, and the framing must not flatten
them.** A recap is a dated record *of a week*, generated from history. An article
is a dated artifact *of the author's understanding*, written by hand and never
revised. The shared sentence is that neither is revised to match a later truth;
it is not that they are the same kind of document. `_POINT_IN_TIME`'s wording is
specific to a week and should not be stretched to cover an article — 1g writes
its own sentence in the same register, in the same `.note` block.

**What must stay separate, stated so nobody unifies it later.** Alignment is
about what the reader sees, not about collapsing two mechanisms into one:

- **The generator.** Recaps project a repository source that grows on its own.
  The corpus has no repository source and four hand-written entries. 1g's refusal
  of a generator stands, and a later attempt to "finish the job" by generating
  both would be machinery for its own sake.
- **The publication policy.** A recap is published by its own `**Publish:**`
  marker, required and never defaulted. An article is published by being in the
  list. Both are one decision in one place; they are not the same decision.
- **The gate.** Recaps are held by CI's `--check` drift assertion. Articles are
  held by 1g's two-way reconciliation. Different questions, and merging them
  would weaken the stronger one.

**One drift to resolve in 3d, not here.** 3d specifies that the recap pages
"share `info.css`"; the shipped generator inlines its own `_STYLE` and loads no
external stylesheet, which is the more CSP-friendly choice and may well be right.
1g cannot conform to a rule the tree contradicts, so **3d owes a decision on
which is true** before 1g writes any markup. Flagged here rather than resolved,
because 1e is in review and this is its stage's call.

### 1g. Link the published writing from the landing page

**Raised 2026-09-01, out of 1f.** 1f decided the articles are not a surface this
plan maintains. It did not decide whether the landing page *points at* them, and
nothing else in this plan does either: Stage 2 is discoverability of `/`, Stage
3d presents the recaps, and the non-goals bar blog-platform features on the
**recap** surface. Linking an external corpus appears in no stage, no non-goal,
and no backlog row. That is a hole, not a deferral.

**The tension this stage exists to resolve.** A portfolio landing page that does
not link the author's writing about the system is leaving its best explanatory
prose unreachable — 1f measured that directly, and found all three articles are
better front-door prose than what the surfaces carried. But linking them spends
part of 1f's decision. The moment `/` points at Article A, the landing page is
handing a reader ten claims the Gate 0 table disposed of, and doing it from a
surface this plan *does* maintain. "Out of scope to maintain" and "safe to
recommend" are different properties, and 1f only established the first.

**The contract this needs, and it already exists.** Stage 3d gives each recap
page "an explicit *point-in-time record* note per the truth contract's §5". That
is the same contract an article needs and for the same reason, so 1g reuses it
rather than inventing a second one: **every linked article displays its
publication date, and the section states once that these are dated artifacts,
not maintained pages.** A reader who sees "2026-05-08" next to A has what they
need to read its numbers as history.

**What that contract does not cover, and this stage must decide.** A dated note
explains why A's numbers are stale. It does not explain why A and C, both linked
from the same section, *contradict each other* on whether bronze is retained —
one says discarded after 30 days, the other explains the packing that keeps
every page. A date reconciles a surface with the past. It does not reconcile two
linked documents with each other. Three ways to answer, and this stage owes one:

| | Answer | Cost |
|---|---|---|
| **a** | Link all four with dates and the §5 note; accept that a diligent reader can find the contradiction | Cheapest and most honest about the corpus being unmaintained. A reader who finds it learns the project ships stale writing |
| **b** | Link the corpus but annotate A specifically — one line naming what has since changed | Preserves A's genuinely good narrative sections while refusing to launder its numbers. Costs a per-article annotation field that only one article uses |
| **c** | Link only the articles that do not contradict a maintained surface, and say the list is curated | Cleanest page. Silently drops the author's most-read piece, and "curated" without a stated rule is the kind of unstated judgment this plan exists to remove |

**Decided 2026-09-01: (a) — dates for all, annotations for none.** (b) was
recommended first and is wrong, for a reason worth writing down because it
generalises.

**An annotation is state about an article relative to a tree that moves.** "A
says bronze is discarded after 30 days; this has since changed" is true today
and silently rots the next time the storage story moves — when Plan 125 lands,
when packing changes again, when a claim nobody was tracking becomes false.
Nothing would tell you. That is a **new drift surface, inside the surface whose
drift this plan exists to remove**, and it is worse than the original because it
is drift about drift. It also scales with the corpus times the rate of change,
which is the shape of a maintenance treadmill.

A **publication date is immutable.** It is a fact about the article, not about
the article's relationship to a repository, so it never needs revisiting. That
is the whole difference, and it gives the list its rule:

> **An entry carries only immutable facts about the article — title,
> publication date, URL. Anything that would need revisiting when the tree moves
> does not belong in the list.**

The contradiction between A and C is then handled where contradictions actually
get handled: **once, when the article is added**, by the procedure below — not by
a permanent note that has to be kept true forever. A is the worked example. It
was reconciled in 1f, its ten disposed-of claims are recorded in the audit table
above, and the surfaces were corrected as a result. That reconciliation already
happened; a per-article annotation would be a second, decaying copy of a record
this plan already holds properly.

#### Adding an article is a procedure, not a list edit

The list is the cheap half. The half that carries the value is what has to happen
*when* an entry is added, and it runs in both directions — which is exactly what
1f did by hand for A, B and C, and what nothing currently repeats for D or
anything after it.

1. **The entry goes in the data file.** Title, publication date, URL with the
   per-session `trackingId`/`lipi` parameters stripped. Mechanical, and a test
   holds it: an entry without a date fails.
2. **The article is checked against the surfaces, both ways.**
   - **Drift** — does the article assert something the README or landing page
     now contradicts? A one-time reckoning at add time, whose output is a
     correction to a surface or a recorded disposition, never a note attached to
     the article. This is what would have caught A and C disagreeing *before*
     the page linked both.
   - **Harvest** — does the article carry framing or a story the surfaces should
     have? 1f found all three existing articles were better front-door prose
     than what the surfaces carried, and drew four framings across. An article
     is written when the thinking is freshest; the surfaces should not be the
     last to hear it.
3. **What the check produces lands on a maintained surface or in this plan's
   record.** Never as durable per-article state. That is the rule that keeps the
   list immutable and the treadmill from starting.

**This is a skill and a commit gate, not a test** — and Stage 1c already argued
why, in the same words: judging whether a claim is still true "needs the tree
read with judgment, which is a skill's job", and "a check you must remember is
weaker than one you cannot forget". The machinery exists and needs extending, not
inventing: `scripts/public_surface_gate.py` holds a commit that stages a surface
until `public-surface-check` has read that exact staged content, keyed by a
digest stamp so passing once buys nothing for content nobody looked at. The
corpus data file becomes a third gated path on the same mechanism.

The direction differs, and the extension has to respect it. Today's gate asks
*surface → tree*: "is this claim still true?" The corpus gate asks *article →
surfaces*, twice: "does this contradict them?" and "should they have taken
something from it?" Same hook, same stamp, different questions — so this is a
second skill or a second mode, not a wider glob on the existing one. The
existing skill scopes itself out of this explicitly: "Not `docs/`, not the
overviews, not the published articles — those are Stage 1f's problem and they
have their own reckoning." **1g is that reckoning.**

**Scope.**

- **Landing page only.** The README is the technical entry point and links
  runbooks, not essays. Adding the corpus there is not proposed and would need
  its own reason.
- **No generator.** 1d and 1e each project a repository source that already
  exists and grows on its own (`docs/PLANS.md`, `docs/recaps/`). The corpus is
  four hand-written entries that change a few times a year and have no
  repository source to project. A build-time generator over a four-row list is
  machinery this plan does not need, and 1e's own experience is that the
  generator's cost is in the policy, not the rendering. **A committed data file
  read at render time, with a test asserting every entry carries a date**, is
  the whole mechanism.
- **Presentation defers to Stage 3**, the way 1e defers to 3d: whatever markup
  1g adds must satisfy 3a's heading rule, 3c's CSP, and render with JavaScript
  disabled at 360 px. 1g decides *what is linked and how it is framed*; Stage 3
  holds it to the same bar as everything else on the page.
- **Placement and shape are set by [the writing surface](#the-writing-surface--what-1e3d-and-1g-share)
  above**, not decided here: one section on `/`, articles inline and the recap
  index linked, reusing `.index-list`, the `.note` block and the date-in-the-meta
  -line convention that `scripts/build_public_recaps.py` already establishes.
  Outbound links are visibly outbound, and 1g writes its own §5 sentence rather
  than stretching `_POINT_IN_TIME`, which is worded for a week.

**Gate 1g:** the landing page carries one writing section, not two, and it reuses
1e's list row, note block and date convention rather than a second treatment;
every linked article renders with its publication date and the §5
point-in-time framing; outbound links are visibly outbound; no entry carries anything beyond the immutable facts; a
test fails if an entry is added without a date, and no linked URL carries the
per-session `trackingId`/`lipi` parameters 1f stripped when it recorded the
corpus; and **adding an entry without the two-way reconciliation having run
against that exact staged content is blocked by the gate, not by memory** —
demonstrated by adding Article D through the procedure rather than by hand.

### 1h. Ask at closeout whether the landed work moved a surface

**Raised 2026-09-01.** There are three ways a public surface and the repository
can come apart, and after 1c and 1g there is still only coverage for two:

| | Trigger | Direction | Held by |
|---|---|---|---|
| 1 | Someone edits a surface | surface → tree: "is this claim still true?" | Stage 1c's commit gate |
| 2 | Someone adds an article | article → surfaces, both ways | Stage 1g's gate |
| 3 | **Someone changes the system and touches no surface** | **tree → surfaces: "did the front door just become wrong?"** | **nothing** |

**The third is the one that has actually produced every defect this plan
caught.** `public_surface_gate.py` fires on `git diff --cached --name-only`
containing `README.md` or `ops/templates/info.html`; a commit that touches
neither never reaches it. So the gate is structurally blind to a plan that adds
migrations, adds tests, adds containers, or replaces a solver, and edits no
prose. The `public-surface-check` skill says what that blindness cost:

> Gate 0 found "36 Flyway migrations" against 49, "266 integration tests"
> against 468, "971 tests" against 3,661, "eleven Docker containers" against
> more than two dozen. **Not one of those was a disagreement between the two
> surfaces** — each was a surface that had drifted from the code.

Not one of those was a bad edit. Each was a surface standing still while the
tree moved underneath it, and the only thing that ever caught them was Gate 0 —
a one-time, expensive, retrospective audit of accumulated drift. **Gate 0 is
what this stage exists to stop needing a second time.**

**Closeout is the right moment, and the argument is availability rather than
rigour.** At closeout the person knows what the work changed and why; the
`close-out` skill's Phase 1 has already gathered the slice's commits and
evidence, so the marginal cost is one question against material already on
screen. Every alternative moment is worse: a test cannot judge materiality, CI
cannot see intent, and a periodic sweep is Gate 0 again — expensive, late, and
measuring drift that has already been published.

**The question, in the taxonomy that already exists.** `public-surface-check`
defines a claim as a sentence that would be false if the repository changed, in
three kinds — a **mechanism**, a **name**, or a **quantity**. So 1h asks the
same taxonomy from the other end:

> Did this work change a mechanism, a name, or a quantity that `README.md` or
> `ops/templates/info.html` states?

Reusing that taxonomy is the point. A closeout that finds "yes, a quantity" is
handing the 1c skill exactly the shape of input it is built to check.

**It must be cheap and it must usually end in "no".** Most plans change nothing
either surface claims, and a step that stops every closeout to deliberate will
be skipped within a month — the same failure `public_surface_gate.py` designed
its digest stamp to avoid. The 1c skill's discipline applies unchanged: most
invocations end in one line. The prompt is answerable from the slice's own
evidence and does not re-read either surface unless the answer is yes.

**What it produces, and what it must not.** The `close-out` skill "gathers and
proposes first and writes nothing until the user approves", and 1h inherits that
without exception — **a closeout must never silently edit a public surface.**
Three outcomes, all recorded:

- **No** — recorded in one line in the evidence section, so the record shows the
  question was asked rather than skipped.
- **Yes, and small** — the correction is proposed in the same approval stop the
  closeout already makes, and lands with it. It then stages a surface, so
  Stage 1c's gate fires on it in the normal way. The two gates compose.
- **Yes, and larger than this closeout** — a ticket, not a rushed edit. A
  closeout is a bad place to rewrite a section of the front door.

**State the weakness plainly rather than overselling it.** This is a skill step,
not a hook: nothing forces a closeout to happen, so unlike 1c's gate it is a
check you can still forget by never closing out. It is worth doing anyway
because it rides a ritual that already exists rather than asking for a new one,
and because the alternative on offer is another Gate 0. **1c remains the
enforceable gate; 1h is the one that catches what 1c cannot see.**

**Gate 1h:** the `close-out` skill asks the question as a named step, in the
taxonomy above; its three outcomes are written into the skill including the
requirement to record a "no"; the step is demonstrated on a real closeout whose
answer is "no" and one whose answer is "yes"; and the skill's "what this must
never do" section names silently editing a public surface.


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

**Read [the writing surface](#the-writing-surface--what-1e3d-and-1g-share) first.**
Stage 1g links the published articles from the same section of `/` that reaches
this index, and takes its list row, note block and date convention from what 1e
built. Two items there land on 3d: the landing page carries **one** "more depth"
section rather than a link per corpus, and **3d owes a decision on the stylesheet
question** — this stage says the recap pages share `info.css`, while the shipped
generator inlines `_STYLE` and loads no external sheet. 1g cannot conform to a
rule the tree contradicts.

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

#### Open: does `/recaps` lead with the newest week in full?

**Raised 2026-09-01.** The shipped index is a bare list of 20 links. The
alternative is the newest published recap rendered **in full** at the top of
`/recaps`, with the index beneath it. This is a 3d decision — it is presentation
— but it is not free: `render_index` currently emits only `<li>` rows, so
choosing this changes what 1e's generator produces.

**The case for it.** An index of twenty dates is a poor place to land. A reader
arriving from the landing page's writing section has to gamble on a week before
seeing whether any of this is worth reading, and the newest week is both the most
current and the most likely to be the one they wanted. It also gives the page
characterisable content — a list of links has nothing in it to summarise, quote
or preview. And it is the same instinct the writing surface already applies one
level up: **put content where the reader lands, not only links.** Articles are
inline on `/` because they are few; the newest recap is inline on `/recaps`
because it is the one most worth reading.

**Three consequences a bare index does not have, and each needs an answer:**

1. **The newest recap would exist at two URLs** — `/recaps` and its own page.
   Stage 2 owns canonical metadata and the sitemap, so this needs a `rel=canonical`
   to the recap's own page, or a deliberate decision that the index is canonical
   and the page is not. Duplicate content published without either is the kind of
   thing Stage 2's route contract exists to prevent.
2. **The §5 note has to be per-week, not per-index.** The index's current note is
   about the collection ("each recap is a point-in-time record"). A recap
   displayed in full needs its own, naming *its* week, in `_POINT_IN_TIME`'s
   words. One page would then carry two notes, or one merged note that does both
   jobs without blurring them.
3. **"Newest" can be old, and displaying it in full makes it look current.** 1e
   measured that 11 of 31 weeks hold no commits, and unpublished weeks are
   skipped — so the newest *published* recap can be several weeks behind today.
   A link in a dated list carries that honestly. A full-bleed article at the top
   of a page reads as "here is where things stand," which it is not. **Whatever
   is displayed must state its week at the top, prominently**, not only in a note
   below the fold.

**One overlap to weigh rather than ignore.** 1b item 6 already gives `/` a
"Recent work" section fed by the roadmap projection, so "what has been happening
lately" has an answer before a reader reaches `/recaps`. The registers differ —
that one is planned/completed rows, this is prose about a week — but the intent
overlaps, and two answers to the same question in two places is how surfaces
start disagreeing. Deciding this means deciding which of the two is the front
door for recency.

**Recommended: yes, with (3) as the binding condition.** The landing cost of a
bare index is real, and the fix is cheap. But the staleness point is what makes
it a truth-contract question rather than a layout preference, and it is the one
that would be easy to implement and forget. If the week is not stated at the top,
this change makes the surface less honest than the list it replaced.

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
| A committed published-writing data file | Stage 1g's corpus entries — title, publication date, URL, and nothing that a moving tree could falsify |
| `scripts/public_surface_gate.py` | Extend Stage 1c's commit gate to the corpus data file, on the same digest-stamp mechanism |
| A published-writing reconciliation skill | Stage 1g's two-way check: article against both surfaces, for drift and for harvest |
| `.claude/skills/close-out/SKILL.md` | Stage 1h's step: did this work change a mechanism, name, or quantity either surface states |
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
frontend. Stage 1g splits on that same seam: the corpus data file and its
date-assertion test belong in PR A, the section's markup in PR C. If the publication policy in Stage 1e turns out to need a long
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
- the published writing is linked from the landing page with publication dates
  and point-in-time framing, and an entry without a date fails a test;
- adding an article to that list without reconciling it against both surfaces is
  blocked by the commit gate;
- closing out a slice asks, and records, whether the landed work changed a
  mechanism, name, or quantity either surface states;
- the demo is bounded, lazy, accessible, and cached;
- interactive content works with keyboard and screen reader semantics;
- scoped public security headers and local assets are in production;
- automated tests and the external route matrix pass;
- a mobile Lighthouse report and screenshots are recorded in the closing PR;
- `docs/PLANS.md` and `docs/planning/completed_plans.md` record the final deployment date
  and measured before/after results.
