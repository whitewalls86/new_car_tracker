# Plan 163: Documented Code Quality Fixes

## Status

**Build order, bottom, written 2026-08-30.** A register rather than a project:
small, unrelated code fixes that are real, that nobody wants to lose, and that
do not individually justify a plan.

It sits last on purpose and its priority is deliberately the lowest in the
index. **It should never displace scheduled work.** The right time to pull an
item off this list is when you are already in that file for another reason.

Priority and effort are proposed in [`docs/PLANS.md`](../PLANS.md), which owns
both; this document does not choose them.

## Why this exists

Plans [73](plan_73_scraper_refactor.md) and [106](plan_106_code_review_cleanup.md)
were both superseded into this one on 2026-08-30, for the same reason: each was
a four-month-old plan whose work had quietly drifted in under other numbers,
leaving a small remainder that was real but could not justify keeping a plan
open. Checked against the code that day, most of both was already done — by
work that never referenced them.

That is the failure mode this register is for. Without it the choice is to keep
stale plans alive so their last two items are not lost, or to close them and
lose the items. Neither is good, and the first is what actually happened for
four months.

## What belongs here

- A fix that is **specific and checkable** — a named file, a named divergence.
- A fix whose absence is **not currently hurting anything**, so it has no gate,
  no soak and no incident behind it.
- A fix small enough that writing a plan for it would cost more than doing it.

## What does not

- **Anything with an incident behind it.** That earns its own plan, or a stage
  in the plan whose territory it is. Plan 139 Stage F is the boundary case: it
  looks like a small test fix and it hung a production deploy, so it is a
  build-order row of its own, not a line here.
- **Anything with a gate, a soak or a measurement.** Those need a plan document
  that can hold evidence.
- **Anything that is really a refactor.** If an item grows a design question, it
  leaves this register and becomes a plan. Say so in writing when it happens.

## The register

Each item names where it came from, so provenance survives the plans that are
now superseded.

### From Plan 106 — code review cleanup (review dated 2026-05-04)

1. **`dashboard/db.py` diverges from `shared/db.py`** (was 106 C2). The
   dashboard's connection helper carries retry logic that the shared module does
   not, so two connection paths behave differently under the same failure.
   Decide which is correct and converge; `dashboard/db.py` still exists.
2. **`json.loads()` guards in the parsers** (was 106 B1 and B3). The original
   finding was bare `json.loads()` on HTML-embedded payloads, including an empty
   string reaching it in the card parsers. **Unverified as of 2026-08-30** — the
   files the plan named have moved, and only the 11 remaining call sites were
   counted, not read. Confirm before fixing; the finding may already be closed.

### From Plan 73 — scraper refactor (deferred 2026-04)

3. **Job management still lives in `scraper/app.py`** (`ThreadPoolExecutor` at
   line 36), the one seam of the four Plan 73 named that was never split. The
   file is 317 lines, down from the "significant scope" that plan described.
4. **`scraper/routers/` is an empty package** — `__init__.py` and nothing else.
   A seam someone created and never used. Either route the endpoints through it
   or delete it; an empty package that looks like structure is worse than
   neither.
5. **Type annotations on the refactored scraper modules**, which Plan 73
   deliberately coordinated with [Plan 70](plan_70_type_annotations.md). Plan 70
   is still in the backlog and is the better home if it is ever picked up; this
   is a pointer, not a duplicate.

## Already delivered — do not redo

Recorded because both superseded plans still describe this work as outstanding,
and a reader who opens them without this note will do it twice.

| Item | Plan | State on 2026-08-30 |
|---|---|---|
| Shared SQL query loader | 106 A1 | **Done.** `shared/query_loader.py`, adopted by 6 modules |
| Shared logging setup | 106 A2 | **Done.** `shared/logging_setup.py`, adopted by 6 modules |
| `cur.description` None guard | 106 B4 | **Done.** `ops/routers/scrape.py:198` |
| `PGPORT` int coercion | 106 B5 | **Done.** `shared/db.py:32-34`, raises a named `ValueError` |
| Structural split of the scraper | 73 | **Largely done** under Plan 61 and later work — `processors/` holds six modules, `models/`, `queries.py`, `sql/` and `db.py` are separate, and `advance_search_rotation` no longer exists anywhere |

### One item is obsolete, not done

**106 C1 — "standardize `/ready` across services" must not be implemented as
written.** Its premise has inverted. The plan complained that *"scraper's
`/ready` returns a 503 while every other service returns a 200."* Today scraper
returns 200, and `archiver` and `dbt_runner` return **503 deliberately** — that
is the job-in-flight readiness contract Plan 131 Stage 5 built and Plan 142's
drain depends on. Applying C1 as written would break it.

If `/ready` is ever standardized, the standard is the 503 contract, not the 200.

## How an item leaves

Fix it, delete the line, and say which item in the commit message. Items may
also leave by being **disproved** — item 2 is the likely candidate — in which
case record that rather than silently dropping it.

New items may be added by any plan that finds one and does not want it. An
addition should name its source the way the ones above do.

## Intersections

### Plans 73 and 106 — superseded into this

Both remain readable for their original reasoning. Neither should be reopened:
what survived them is items 1 through 5 above, and what did not is in the
delivered table.

### Plan 70 — type annotations

Backlog, and the proper home for item 5 if it is ever scheduled. Listed here so
the Plan 73 coordination is not lost, not to duplicate the work.

### Plan 139 — test suite maintenance

The boundary case for "what does not belong here." Stage F is small, looks like
a tidy-up, and hung a production deploy — which is why it is a build-order row
and not a line in this register.
