# Plan 138: Public Surface Refresh

## Status

**DRAFT — audit complete; implementation has not started.** Written 2026-08-17
after comparing the live `https://cartracker.info/info` page and `README.md`
against `master` at `6f6a2ba`.

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
  operational endpoints that are not already intentionally public. This now has a
  named instance: the internal overviews recorded under Stage 0 document the
  anti-detection path and production object prefixes, and neither may reach a
  public surface.

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

### Internal source documents

Two current-state overviews were written on 2026-08-28, after this plan's
2026-08-17 audit and against a later tree. They are the strongest material this
plan has for Stage 1a, and they are **internal engineering references, not
public copy**.

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
- The pages share `info.css`, load no JavaScript, and satisfy Stage 3c's CSP
  without exception. A recap page that needs a script has been over-built.

**Gate 3:** all page functions are usable with keyboard only, reduced-motion
users do not receive autoplay, no third-party request is required to render the
page, the initial page view does not download the full demo, and every recap
page renders with JavaScript disabled and no horizontal overflow at 360 px.

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
