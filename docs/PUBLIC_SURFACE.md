# The Public Surface Contract

**Owner:** [Plan 138](plans/plan_138_public_surface_refresh.md), Stage 8.
**Measured against the repository on 2026-09-02.**

This is the standard, not a description of the site. Where the two disagree the
repository is wrong, and [the gap list](#the-gap-list) says so by name.

[Plan 138](plans/plan_138_public_surface_refresh.md) points here and holds no
second copy. Its truth contract and its route table **moved** into this document
rather than being duplicated: a rule kept in two places is a rule that drifts,
and this repository has already paid for that lesson twice — at the
`plans`/`close-out` seam Stage 1h found, and at Stage 7's publish procedure. A
plan is a change. It completes, archives into
[`completed_plans.md`](planning/completed_plans.md), and stops being where
anyone looks. These rules do not.

The §1–5 numbering is preserved from Plan 138 so that document's existing
references to "the truth contract's §3" and "§5" still resolve.

Three things carry this contract, and they are meant to be the same contract:

| Form | Where | Owner |
|---|---|---|
| For a person | this document | Plan 138 / CAR-67 |
| For a coding agent | [`.claude/skills/public-surface-check/`](../.claude/skills/public-surface-check/SKILL.md) | Plan 138 / CAR-56 |
| For the commit gate | [`scripts/public_surface_gate.py`](../scripts/public_surface_gate.py) | Plan 138 / CAR-56 |

**There is no fourth row, and `docs/TESTING.md` has one.** No CI job asserts
anything in this document. See [Specified here, not yet
asserted](#specified-here-not-yet-asserted); it is the largest structural
weakness this contract has, and stating it is cheaper than implying coverage
that does not exist.

---

## The surfaces

**Authored** surfaces are written by a person and say things. **Generated**
surfaces project a repository source and say only what that source says.

| Surface | Kind | Public from |
|---|---|---|
| `README.md` | authored | the moment it merges — the repository is public |
| `ops/templates/info.html` | authored | when the `ops` container is **deployed**, which lags `master` |
| `ops/static_ops/generated/recaps/` | generated from `docs/recaps/` | `git pull` on the VM |
| `ops/static_ops/generated/project-updates.json` | generated from `docs/PLANS.md` and the archive | `git pull` on the VM |
| `docs/PLANS.md`, **Next executable slice** cell, top four build-order rows | source of the above | `git pull`, after regeneration |

**The last row is the one that surprises people.** A one-cell edit to the build
order changes copy on the live landing page, because the cell is the `summary`
field of the roadmap projection and the top four rows are what the generator
publishes. Stage 1h found this by asking, and the `plans` skill is where the
rule now lives.

**Only the two authored surfaces are covered by the commit gate.** Everything
below §1–§5 applies to all five; the enforcement does not. That asymmetry is
recorded as [P4](#the-gap-list) rather than quietly widened, because widening a
gate's glob has a cost and it is a decision, not a typo.

---

## §1 Production today

- Airflow orchestrates scraping, processing, archival, maintenance, and analytics.
- Postgres owns current operational state and short-lived event buffers.
- MinIO holds replayable bronze HTML and permanent Parquet history.
- dbt and DuckDB currently build and serve the analytical marts used by the
  landing page, metrics, and Streamlit.
- Caddy, oauth2-proxy, and the ops authorization check protect application routes.

## §2 Proven but not production-serving

- Production-shaped CI lake snapshots.
- Iceberg tables registered through Lakekeeper and exercised through Spark.
- dbt-Spark parity work and MLflow experiment provenance.
- Adaptive-refresh feature and backtesting foundations.

These belong under a heading such as "Platform evolution," never in language that
implies the public dashboard already reads Iceberg.

## §3 Volatile operational numbers

Active listings, observations, throughput, tracked pairs, and analytical freshness
come from the public stats snapshot. They must not also be hard-coded in hero or
service-card prose. Repository inventory may use rounded statements such as
"more than a dozen DAGs," "20+ dbt models," and "3,000+ tests," with an explicit
"verified on" date where precision adds value.

Avoid the phrases "without manual intervention," "every failure alerts," and
"every service exposes `/ready`." The narrower, true claim is that long-running
worker services participate in deploy draining, while functional liveness remains
a separately measured concern.

**An exact repository count is a promise to update it, and nobody keeps that
promise.** Plan 138's Gate 0 re-measured five of this rule's own examples within
two weeks of writing them.

## §4 Recent and planned work

The public feed is a projection of the roadmap, not a second roadmap:

- "Planned next" comes from the ordered rows in `docs/PLANS.md`'s **Default
  build order** table. Publish only the first four executable rows.
- "Recently completed" comes from the newest-first table in
  [`planning/completed_plans.md`](planning/completed_plans.md). Publish only the
  first four rows. **This is a different file from the build order.** Plan 146
  removed `docs/PLANS.md`'s duplicate Completed table; what remains under that
  heading is a pointer, and a generator that parses it finds no rows.
- Titles, short public summaries, priority, effort, state, and source links may
  be shown. Internal hostnames, incident payloads, approval records, production
  object keys, and other operational detail must not be copied into the feed.
- A plan is not presented as complete until its completion row exists. A merged
  implementation with an outstanding production gate remains "verification" or
  "closeout," not "completed."

The plan document remains the detailed source. The landing page is a small,
current window into it.

## §5 The weekly recaps

The recaps are a fourth kind of fact, and the one with the least editorial
control: they are written weekly by the `plan-week` skill against git history,
not authored as public copy.

- **They were already public before the site published them.** The repository is
  public and `docs/recaps/` is in it. Publishing them on the site changes their
  **prominence and framing**, not their disclosure status. Review of a recap is
  an editorial gate, not a leak gate.
- **A recap is a record of a week, not evergreen prose.** It is correct as of its
  date and is never revised to match a later truth. Every published recap carries
  its week and a statement that it is a point-in-time record, so §1's
  production/experimental split is not contradicted by a six-month-old page.
- **They may name what §3 narrows.** The scrape path is barred from *authored*
  public copy; recaps predate that narrowing and are not rewritten to match it.
- **Publication is a per-file `**Publish:** true|false` marker**, required and
  never defaulted, decided in Plan 138 Stage 1e. A recap with no marker fails the
  build: `true` by default publishes an unread week the moment it lands, and
  `false` by default drops one off the site silently. The marker travels with the
  file the writer is already editing, which is why it is not a central list.

The same point-in-time framing governs the author's published articles, for a
different reason: an article is a dated artifact of the author's understanding,
written by hand and never revised. Plan 138 Stage 1f decided the article corpus
is **out of scope to maintain**. Out of scope to maintain and safe to recommend
are different properties, and only the first has been established.

---

## What a claim is

A sentence that would be false if the repository changed. Three kinds, and the
third is the one that has actually bitten:

1. **A mechanism** — what a service does, what writes where, what owns a schedule.
2. **A name** — a container, DAG, view, table, or environment variable.
3. **A quantity** — any count, ratio, duration, or size.

### Two rules on quantities

- **Round it.** §3 bars exact repository counts on a public surface. See above
  for why.
- **Name the set.** A number is wrong if it counts the wrong thing, however
  correct the arithmetic. Plan 138's Gate 0 published "28 services without a
  profile gate" — true, and the wrong set, because it excluded `trawl` and
  `redis-trawl`, the live scrape path. Stage 1f said inodes "fell by roughly two
  thirds" from a whole-volume reading of a mechanism that removed 99.99% of the
  inodes it was pointed at, **and the first correction repeated the mistake.**
  If you cannot say what a number is a fraction *of*, that is the finding.

### Where the truth lives

| Claim about | Check |
|---|---|
| Airflow DAGs, and what is scheduled | `airflow/dags/`, and the `schedule=` argument in the source |
| dbt models | `dbt/models/` |
| Flyway migrations | `db/migrations/` |
| Long-running services | `container_health.expected.EXPECTED_SERVICES`, which `tests/test_observability_config.py` derives |
| Alert rules | `grafana/provisioning/alerting/rules.yml` |
| The live solver | Compose — the live container is `trawl`; `flaresolverr` is retained and vestigial |
| Scrape backoff | The `ops.ops_detail_scrape_queue` view |
| Mechanism, generally | [`ARCHITECTURAL_OVERVIEW.md`](ARCHITECTURAL_OVERVIEW.md) |

### The four questions

A reader of both authored surfaces must be able to answer these without
reconciling anything:

- What runs in production?
- What is experimental?
- Where does history live?
- What requires authentication?

The two surfaces **do not owe each other identical copy** — the README is a
technical document and the landing page is a portfolio piece. They owe each
other agreement on substance.

---

## Route and access contract

| Route | Access | Behavior |
|---|---|---|
| `/` | Public | Canonical portfolio landing page, HTTP 200 |
| `/info` | Public | 308 to `/` |
| `/recaps` | Public | Generated recap index, newest first, HTTP 200 |
| `/recaps/YYYY-MM-DD` | Public | One generated recap page, HTTP 200 |
| `/writings` | Public | The author's published articles, one card each, linking out. HTTP 200. **Specified 2026-09-03, not yet built** — Plan 138 Stage 1g |
| `/static_ops/*` | Public | Versioned local assets with long-lived caching |
| `/robots.txt` | Public | Allows the public root and references the sitemap |
| `/sitemap.xml` | Public | Contains only canonical public URLs |
| `/request-access*` | Google-authenticated | Existing request workflow |
| `/dashboard*` | `viewer`+ | Streamlit, mounted at `/dashboard` |
| `/admin*` | `observer`+ with current mutation rules | Unchanged |
| infrastructure tools | Existing role requirements | Unchanged |

**The public root is an exact match on `/`, matched before the authenticated
catch-all.** The browser-visible canonical URL is `/`, and the `/info` redirect
does not loop through it.

**Streamlit no longer owns the origin root.** `dashboard/Dockerfile` runs it with
`--server.baseUrlPath=dashboard`, so it serves its machinery and its relatively
linked assets under `/dashboard/*` and returns 404 at `/`. Before that change the
catch-all was load-bearing in a way no configuration stated: taking `/` away
broke `/dashboard` while every other route check passed. `tests/test_dashboard_base_path.py`
holds it. The history is in Plan 138's Stage 2 evidence; the rule here is that
**a route change near `/` is verified by loading the dashboard, not by a status
code on `/dashboard`.**

**An unauthenticated public route must not be given a request-time database or
upstream dependency.** The landing page renders from an immutable presentation
cache and stays useful when the snapshot is empty, stale, or unavailable.

---

## The destination inventory

The route table says what resolves. It does not say where a reader is meant to
**go**, and that gap is how `/recaps` shipped with a canonical route, a sitemap
entry, and no inbound link — in that order.

> **A public route that nothing links is not a destination. It is an artifact
> with a URL.** Either something links it, or the inventory records why not.

| Destination | For | Shape | Linked from |
|---|---|---|---|
| `/` | The explanation of the system, for someone who arrived with no context | page | README, external links |
| `/recaps` | The long-form account of what happened, week by week | page (index) | **nothing** — [P2](#the-gap-list); Plan 138 Stage 1g gives it a door from `/` |
| `/recaps/YYYY-MM-DD` | One week's record | page | its index |
| The live stats block | What the system is doing right now | section of `/` | **nothing; it has no anchor id** — [P3](#the-gap-list) |
| `/dashboard` | The application a granted role grants | page, `viewer`+ | **nothing** — [P1](#the-gap-list) |
| `/request-access` | The way to ask for a role | page, Google-authenticated | `/` hero and footer |
| `/writings` | The author's own account of the work, in their own register | page (cards, linking out) | **not yet built** — Plan 138 Stage 1g gives it a door from `/` |
| The published articles | One article, on the third-party platform that hosts it | external, third-party | `/writings`, and the newest from `/` — Plan 138 Stage 1g |

**Outbound links are visibly outbound**, and an item that scrolls rather than
navigates is not presented as though it navigates. A list that flattens an
anchor, a page and a third-party URL into one undifferentiated set is a small
lie of omission.

### Open — these are not decided

| # | Question | Settled by |
|---|---|---|
| ~~D1~~ | ~~Does `/recaps` survive as its own destination, or become a section of `/`?~~ **Answered 2026-09-02: it survives, and gains weight.** Plan 138 Stage 3d decided `/recaps` leads with the newest published week rendered in full, with its week stated at the top and `rel=canonical` pointing at that week's own page. A destination you land on and read is not a candidate for folding into `/` | Plan 138 Stage 3d |
| ~~D2~~ | ~~Is long-form writing one place or two?~~ **Answered 2026-09-03: two, and neither is inline on `/`.** `/recaps` is the account of what happened; `/writings` is the author's own articles. `/` carries one "more depth" section holding a door to each plus the newest article as a card, so the reader still has a single place to look. The withdrawn answer — articles inline, reusing the recap index's row — failed for two reasons recorded in Plan 138: 3d gave `/recaps` real weight, so pairing a page against four inline links was not two destinations; and the row reuse was never shared code, since `.index-list` lives in the recap generator's own stylesheet, which `/` cannot load | Plan 138 Stage 1g |
| D3 | What earns a destination slot at all? Without a rule, the next generated artifact repeats the route-then-sitemap-then-no-link sequence | Plan 138 Stage 3d |
| D4 | Does a public navigation element exist, and on which surfaces? A nav shared with the recap pages changes what `scripts/build_public_recaps.py` emits | deferred behind D1–D3 |

**D3 is recorded here rather than answered.** Answering it by omission is what
produced the gap list below. D1 was answered on 2026-09-02 and D2 on
2026-09-03, within a day and two days of this contract first recording them,
which is the mechanism working: the questions were written down where the
stage that owns them would see them.

**D2's answer adds a destination, which is a data point D3 still has to
generalise.** `/writings` earned a slot on the argument that it is a
destination a reader lands on and reads, the same test D1 applied to
`/recaps`. That is a precedent, not yet the rule D3 asks for.

---

## Specified here, not yet asserted

- **No CI job asserts any rule in this document.** `docs/TESTING.md` has
  `tests/test_testing_contract.py`; this contract has no equivalent.
- **The commit gate is a Claude Code hook on the Bash tool, not a git hook.** A
  commit typed in a terminal is unaffected. It also enforces a *stop*, not the
  check: it blocks and names the skill, and something must still choose to run
  that skill honestly.
- **Nothing asserts that a public destination is linked from anywhere.** Every
  row of the gap list below would have been caught by one assertion of that
  shape, and it is the cheapest coverage this contract is missing.
- **No test holds the two authored surfaces in agreement.** Plan 138 Stage 1c
  built one, measured that cross-surface disagreement had no recorded instance,
  and deleted it in favour of the review skill. That reasoning is recorded in
  Stage 1c and is not re-opened here.

---

## The gap list

Measured violations of the contract above, as of 2026-09-02. Recorded here,
fixed elsewhere. An entry is deleted when it is repaired, not marked closed.

| # | Violation | Owner |
|---|---|---|
| P1 | **`/dashboard` is linked from no public surface.** `ops/templates/info.html` has no `<nav>` and mentions the route nowhere; its only calls to action are `/request-access`, at the hero and the footer. A visitor who requests access, is granted a role, and returns has no path to the thing they were granted | Plan 138, deferred navigation stage |
| P2 | **`/recaps` has a canonical route, a sitemap entry, and no inbound link.** The landing page's only recap mention resolves to GitHub | Plan 138 Stage 3d / D1 |
| P3 | **The live stats section carries no anchor id**, so nothing can link to it — including the roadmap section directly above it | Plan 138 Stage 4 |
| P4 | **The commit gate covers two of five public surfaces.** `public_surface_gate.py` fires on `README.md` and `ops/templates/info.html`; the generated artifacts and the `docs/PLANS.md` slice cell are outside it. The slice cell is covered instead by the `plans` skill, which knows it is publishing; the generated artifacts are covered by their sources and their `--check` | Plan 138 |
| P5 | **Article A contradicts Article C on bronze retention**, and both stay published under the same name. Accepted, dated, and recorded by Plan 138 Stage 1f: an article is a point-in-time artifact. Listed here because a reader may arrive at the surfaces from a document that disagrees with them, and because Stage 1g proposes to link both from `/writings` | Plan 138 Stage 1g |

---

## What this contract does not decide

- **Which destinations exist.** D3 and D4 above are open. D1 and D2 are
  answered, both by Plan 138's Stages 3d and 1g.
- **The navigation element's design.** A nav is a projection of the destination
  inventory; it is deliberately deferred until the inventory is closed.
- **Whether the published articles are maintained.** Plan 138 Stage 1f decided
  they are not. This contract records the consequence, not the decision.
- **Anything about authenticated surfaces' content.** The dashboard, admin UI,
  Airflow, Grafana, MinIO and pgAdmin are out of scope; only their *access*
  appears in the route table.
- **Fixing anything in the gap list.**
