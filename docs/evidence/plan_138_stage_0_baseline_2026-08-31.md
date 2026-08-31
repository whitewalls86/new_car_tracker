# Plan 138 Stage 0 baseline — 2026-08-31

The pre-change record of the public surface, captured before any Stage 1 copy is
written. Every number here was re-measured on 2026-08-31 against
`origin/master` at `0c3e5dd` and the live site. **Nothing was carried forward
from the plan's 2026-08-17 audit column**, which is stale in six of its rows.

All live observations were read-only `GET` requests to `https://cartracker.info`.
No production configuration, container, or datum was changed.

This document is evidence, not product copy. The plan's Stage 0 says exact counts
may change during later plans without forcing a prose rewrite, so **no test pins
these numbers**. Drift detection belongs to Stage 5, which is scoped correctly as
"the absence of the known stale phrases" — an assertion about wording, not about
arithmetic.

## Method

```bash
git ls-files 'airflow/dags/*.py' | wc -l          # files, not DAGs — see below
grep -n 'dag_id="' airflow/dags/*.py              # the actual DAG count
git ls-files 'dbt/models/**/*.sql' | wc -l
git ls-files 'db/migrations/*.sql' | wc -l
python -m pytest --collect-only -q | tail -1
python -c "import yaml; d=yaml.safe_load(open('docker-compose.yml')); ..."
curl -s -o /dev/null -w '%{http_code} %{redirect_url} %{size_download}' <url>
```

Counts were taken with `git ls-files` rather than `find`, because the working
tree carries `.claude/worktrees/` copies that inflate every recursive glob.

Layout measurements were taken by injecting a `scrollWidth` probe into a local
copy of the served HTML and reading it back with `chrome --dump-dom`. See
[*Mobile layout*](#mobile-layout) for why the screenshot alone was not evidence.

## Repository inventory

Each row names its denominator, because most of these have more than one
defensible number and the ambiguity is what let the old claims drift.

| Subject | Measured 2026-08-31 | Denominator that produced it | Plan's stale 2026-08-17 column |
|---|---:|---|---:|
| Airflow DAGs | **15** | distinct `dag_id=` values | 15 |
| — files in `airflow/dags/` | 19 | `*.py`, including 4 helper modules | — |
| dbt models | **23** | `dbt/models/**/*.sql` | 22 |
| — staging / intermediate / marts | 5 / 9 / 9 | subdirectory | 5 / 9 / 8 |
| Flyway migrations | **49** | `db/migrations/V*.sql`, V001–V049 contiguous | 42 |
| Tests collected | **3,661** | `pytest --collect-only -q`, full suite | 2,553 |
| — excluding `integration` | 3,193 | `-m "not integration"` (468 deselected) | — |
| Test modules | 149 | `tests/**/test_*.py` | — |
| Compose services | **34 / 28 / 28** | see the next section | 26 |

**The DAG count is 15, not 19.** Nineteen is the `.py` file count; four files in
`airflow/dags/` define no DAG at all — `coordination_contract.py` (admission
surfaces), `notifications.py` (Telegram callbacks), `pools.py` (pool names), and
`sensors.py`, whose only `with DAG(...)` is inside a docstring. The plan's
2026-08-17 figure of 15 was correct, and correct for the wrong reason: the tree
has since gained four DAGs and four non-DAG modules.

The test total is environment-dependent — measured here on Python 3.13.2,
Windows, full collection. It is the least stable number in this table and the
one that most deserves rounding in public copy.

## Compose service denominators

This is the row with a real decision in it, and the numbers are a trap: **two
different sets both have 28 members.**

| Set | Count | Contents |
|---|---:|---|
| Defined in `docker-compose.yml` | 34 | every `services:` key |
| Without a profile gate | 28 | 34 minus the 6 profiled |
| Expected running | 28 | `container_health/expected.py::EXPECTED_SERVICES` |

They are not the same 28:

| Difference | Services | Why |
|---|---|---|
| non-profiled ∖ expected | `airflow-init`, `flyway` | one-shot init jobs; they run and exit, never "running" |
| expected ∖ non-profiled | `trawl`, `redis-trawl` | **the live scrape path**, profile-gated |

So "28 services without a profile gate" is the wrong public number in the most
embarrassing possible way: it excludes the solver that actually does the
scraping. The remaining four profiled services — `april-processor`, `dbt`,
`dbt_test`, `snapshot-worker` — are genuinely on-demand.

`EXPECTED_SERVICES` is the honest set. It is not a number invented for this
plan: Plan 140 publishes absence against it and Plan 142 Stage 3 gates host
maintenance on it, and `TestExpectedServicesMatchTheManifest` asserts it equals
Plan 142's `maintenance-running-set.txt` by exact set equality.

**Decision (Gate 0):** public copy says **"more than two dozen long-running
services"** and prints no integer. The exact 28 is recorded here and in
`container_health/expected.py`, where it is already test-enforced. A hardcoded
number on the landing page would be a fourth place to drift, and this table is
the reason to believe it would.

## Live route and access behavior

Measured 2026-08-31, unauthenticated, from outside the VM.

| Route | Status | Result |
|---|---:|---|
| `/` | 302 | → `/oauth2/sign_in?rd=https://cartracker.info/` |
| `/info` | 200 | `text/html`, 54,352 bytes |
| `/robots.txt` | 302 | → OAuth sign-in |
| `/sitemap.xml` | 302 | → OAuth sign-in |
| `/dashboard` | 302 | → OAuth sign-in |
| `/request-access` | 302 | → OAuth sign-in |
| `/static_ops/demo.mp4` | 200 | `video/mp4`, 41,699,885 bytes |

The bare domain, `robots.txt`, and `sitemap.xml` all enter the Google OAuth flow.
This is the route drift Stage 2 exists to fix, and it is recorded here as the
before-state for the Stage 5 route matrix.

`/static_ops/*` is already public and unauthenticated — the 41.7 MB video is
reachable by anyone, with no sign-in, today.

## Transfer weight

| Asset | Bytes | Note |
|---|---:|---|
| `/info` HTML | 54,352 | **served uncompressed** — no `Content-Encoding` on a `gzip, br` request |
| `ops/templates/info.html` source | 55,819 | on disk |
| `ops/static_ops/demo.mp4` | 41,699,885 | eagerly referenced |
| `ops/static_ops/dbt-bit-standalone.png` | 18,241 | |

Third-party runtime dependencies, both uncontrolled and both fetched on every
public page load:

- `cdn.jsdelivr.net` — Pico CSS v2, `ops/templates/info.html:7`
- `cdn.simpleicons.org` — 12 service logos, each with an `onerror` hide handler

The `onerror` handlers mean a CDN outage degrades silently rather than visibly,
which is why the absence of these hosts is worth asserting in Stage 5 rather
than eyeballing.

## Mobile layout

**The page does not overflow horizontally at 360 px.** Measured inside a
360 px-wide iframe:

```text
innerWidth=360  clientWidth=345  body.scrollWidth=356  documentElement.scrollWidth=356
```

`scrollWidth (356) <= innerWidth (360)`, so there is no page-level overflow. The
widest element on the page, `div.pipeline-stage`, extends to x=666, but it sits
inside a container with `overflow-x: auto` (`info.html:76`) and scrolls within
its own strip. That is intentional and is not page overflow.

This needed a real measurement, and the first attempt was wrong. A plain
`chrome --headless --window-size=360,...` screenshot **appears** to show severe
clipping — but the probe reports `innerWidth=512`. Both the old and the new
headless modes floor the top-level layout viewport at 512 px on this host, so
that capture is a 512-wide layout cropped to 360, not a 360-wide layout. The
committed screenshot renders the page inside a 360 px iframe instead, which
gives a true 360 px layout viewport.

Recording "overflows at 360 px" on the strength of the cropped image would have
put a false defect into the baseline and sent Stage 3 chasing it.

## Screenshots

In [`plan_138_baseline_2026-08-31/`](plan_138_baseline_2026-08-31/):

| File | Capture |
|---|---|
| `desktop-1440.png` | live `/info`, 1440 px window (1403 px layout), full page 4,467 px tall |
| `mobile-360.png` | live `/info` in a 360 px iframe, full page 5,955 px tall |

Both are full-page. An earlier desktop capture truncated at 2,400 px and was
replaced.

## Claims being removed

Every hardcoded claim on a public surface, with its location. This is the
"deliberately removed" list Stage 0 owes Stage 1.

### `ops/templates/info.html`

| Line | Claim | Measured truth |
|---|---|---|
| 323 | "continuous collection across **13+** make/model pairs" | live stat reads 14 |
| 372 | `{{ stats.make_model_pairs }}` → renders **14** | correct; this is the only one that should survive |
| 446 | "surveys Cars.com across **40+** make/model pairs" | live stat reads 14 |
| 800 | "one Airflow scheduler, **eleven Docker containers**" | 28 expected running |
| 787 | "**Every service exposes** a `/ready` endpoint" | three do: `scraper`, `processing`, `archiver` |
| 451 | anti-detection "keeps the collection running **without manual intervention**" | Plan 128 and the 2026-08-14 solver outage both contradict this |

Three different make/model values render on one page load, and the desktop
screenshot shows all three above the fold region.

### `README.md`

| Line | Claim | Measured truth |
|---|---|---|
| 3 | "**40+** make/model pairs" | 14 |
| 38 | "**FlareSolverr** bootstraps `cf_clearance` cookies" | `trawl` is the live solver — see Gate 0b |
| 58–66 | DAG table: `dbt_build` Hourly, `flush_staging_events` 15 min, `flush_silver_observations` 5 min | all three are `schedule=None`; `hourly_analytics_refresh` owns the sequence |
| 58–66 | table lists 9 DAGs | 15 exist |
| 74 | "The archiver bulk-flushes them **to HOT tables** and Parquet" | archiver exports the event buffer to Parquet only |
| 78 | backoff "lives **entirely in a dbt** staging model" | executable backoff is inlined in `ops.ops_detail_scrape_queue` |
| 80 | "**FlareSolverr** handles active Cloudflare JS challenges" | `trawl` does |
| 82 | "**36** Flyway migrations" | 49 |
| 156 | "**971 tests** across two suites" | 3,661 collected |
| 167 | "Unit tests (**705**)" | 3,193 non-integration |
| 169 | "Integration tests (**266**)" … "applies all **36** Flyway migrations" | 468 integration-marked; 49 migrations |
| 208 | tree comment "**12** Airflow DAGs" | 15 |
| 209 | tree comment "**36** Flyway migrations (V001–**V036**)" | 49, V001–V049 |
| 211 | tree comment "**266** integration tests" | 468 |

## Gate 0 — disposition of every drift-table row

Every row of the plan's drift table (§*Why this work is needed*) now carries an
assigned replacement or an explicit decision to delete the claim.

Counts are replaced with **rounded, non-brittle** phrasing throughout, per the
truth contract §3. No public surface prints an exact repository count; the exact
figures live in this document, which is dated.

| # | Drift-table row | Disposition | Replacement claim |
|---:|---|---|---|
| 1 | 13+, 14, 40+ make/model pairs | **Replace + delete** | Delete both hardcoded values (`info.html:323`, `:446`, `README.md:3`). The live `stats.make_model_pairs` tile is the single source. |
| 2 | 12 Airflow DAGs | **Replace** | "more than a dozen Airflow DAGs" |
| 3 | 15 dbt models | **Replace** | "20+ dbt models across staging, intermediate, and mart layers" |
| 4 | 36 Flyway migrations | **Replace** | "40+ versioned Flyway migrations, applied automatically on deploy" |
| 5 | 971 tests | **Replace** | "3,000+ tests" — see the note below on the contract's own example |
| 6 | 11 Docker containers | **Replace** | "more than two dozen long-running services" (see [*Compose service denominators*](#compose-service-denominators)) |
| 7 | Staging events flushed into HOT tables | **Replace** | "Processing updates the HOT row and appends its event in the same Postgres transaction; the archiver later exports the event buffer to Parquet and deletes the exported rows." Source: `ARCHITECTURAL_OVERVIEW.md:60-63`. |
| 8 | Exponential cooldown lives entirely in dbt | **Replace** | "The executable backoff that decides whether a listing can be claimed lives in the Postgres `ops.ops_detail_scrape_queue` view; dbt uses the event stream for cohort, funnel, and block-rate analysis." Source: `ARCHITECTURAL_OVERVIEW.md:64-68`, `V029__plain_postgres_ops_views.sql:87-89`. |
| 9 | Silver and dbt DAGs on their old schedules | **Replace** | Rebuild the README DAG table from `schedule=` in the source. State that `hourly_analytics_refresh` owns the scheduled flush/build sequence and that `dbt_build`, `flush_staging_events`, `flush_silver_observations`, and `export_ci_lake_snapshot` are manual-only. |
| 10 | All failures surface as notifications | **Delete** | Delete the claim. Narrower true statement: "Container health, scrape volume, and log-based alerts route to Telegram; functional liveness is a separately measured concern." Plan 128 and the 2026-08-14 outage are the evidence against the broad claim. |

Three further rows found during this baseline, not in the plan's 2026-08-17
table, disposed of under the same rule because they are the same class of defect:

| # | Claim | Disposition | Replacement |
|---:|---|---|---|
| 11 | "Every service exposes a `/ready` endpoint" (`info.html:787`) | **Replace** | "Long-running worker services expose `/ready` and participate in deploy draining." Exactly three do. |
| 12 | "without manual intervention" (`info.html:451`) | **Delete** | Named by the truth contract §3 as a barred phrase. No replacement; the surrounding sentence stands without it. |
| 13 | FlareSolverr named as the live solver (`README.md:38`, `:80`) | **Replace** | Same reconciliation Gate 0b applied to `ARCHITECTURAL_OVERVIEW`: the live solver is `trawl`, the variable kept the name `FLARESOLVERR_URL`, and the wire protocol genuinely is FlareSolverr's. |

### One correction the contract needs

The truth contract §3 offers "2,500+ tests" as its own rounding example. That was
written against the 2026-08-17 measurement of 2,553 and is now understated by
more than a thousand. Row 5 assigns **"3,000+ tests"**, and §3's example is
updated to match so Stage 1 is not drawing from a stale example while writing
copy whose whole purpose is to stop being stale.

## Findings recorded, not fixed

Both are outside CAR-44's scope and neither blocks Gate 0:

- `/info` is served uncompressed. A 54 KB HTML response with no
  `Content-Encoding` is a Stage 3c concern, not a Stage 0 one, but the
  measurement is here so Stage 3 has a before-number.
- The live stats tile renders `Analytics data through (stale)` with a
  2026-08-31 14:00 timestamp. Whether that staleness marker is correct behavior
  is Plan 143's contract, not this plan's; recorded only because it is visible
  in both committed screenshots.

The two findings carried forward from Gate 0b — `.env.example:49-57` still
defaulting to the vestigial container, and neither overview carrying an
internal-only marker — remain open and are unaffected by this gate.
