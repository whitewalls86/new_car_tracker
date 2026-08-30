# Plan 147: Scrape State Ownership — Separating Fetch From Enrichment

## Status

DRAFT, written 2026-08-23 after [Plan 142](plan_142_planned_host_maintenance.md)
Stage 0 found that pausing `results_processing` for a maintenance window would
put the detail scraper into a re-scrape loop.

Priority **75 (medium)**. Effort **S**. In the build order in
[`docs/PLANS.md`](../PLANS.md), which is authoritative for its position.

Re-verified against the codebase on **2026-08-29** before implementation began.
That pass corrected the migration numbering, the Stage 4 blast radius, and the
description of `release_claims`, and added the note that the only live caller
reports `ok` for every listing. Sections carrying a *verified* date were checked
against code on that date; everything else is as written on 2026-08-23.

Stages are ticketed in cycle 1 as **CAR-12** (Stage 1, 3 pts), **CAR-23**
(Stage 2), **CAR-24** (Stage 3) and **CAR-25** (Stage 4, 1 pt each).

This is a **data-ownership refactor, not a refresh-policy change.** Which
listings are eligible for a detail scrape, and how often, is deliberately
unchanged. See [Non-goals](#non-goals), which are load-bearing here.

## The problem

`ops.ops_detail_scrape_queue` selects listings whose
`price_observations.last_detail_scraped_at` is null or older than seven days.
That column is written in exactly one place —
[`processing/writers/detail_writer.py`](../../processing/writers/detail_writer.py),
the **processing service**. `POST /scrape/claims/release` **deletes** the claim
row once a batch finishes.

So the guard that stops a listing being fetched again lives two hops downstream
of the fetch, behind an async queue. Whenever anything breaks that chain:

1. `scrape_detail_pages` claims ~100 listings, fetches them, releases — claims
   deleted;
2. the artifacts sit `pending`; nothing sets `last_detail_scraped_at`;
3. the listings are still in the queue view with no live claim;
4. fifteen minutes later, the same listings are claimed again.

At `*/15` and `BATCH_SIZE=100` that is up to four redundant passes over the same
~100 listings per hour — real requests to cars.com, through the browser solver
[Plan 136](plan_136_solver_recycle_and_liveness.md) is currently nursing for
memory, carrying 403 and IP-reputation risk, and inflating the pending backlog
with duplicates.

### This is the third time, which is the actual evidence

[Plan 115](plan_115_detail_unenriched_circuit_breaker.md) (completed
2026-07-01) exists to stop *"listings being re-queued for detail scraping every
15 minutes after they have already been successfully detail-scraped."* Same
loop. It fixed one cause — a successful parse that yielded no `customer_id` —
by setting the column anyway.

The loop has since been reachable through at least three more doors:

| Cause | Status |
|---|---|
| Parse succeeded, no `customer_id` | Fixed by Plan 115 |
| **Processing paused** (a maintenance window) | Found 2026-08-23, unfixed |
| Processing crashed, or the queue backs up | Unfixed |
| Parser gap on a new page layout | Unfixed |

Each has its own patch. A defect that recurs through new causes after a
targeted fix is telling you the boundary is wrong, not that the fix was.

## Root cause: one column, two questions, wrong owner

`last_detail_scraped_at` is asked two different questions with two different
owners, and the name reveals it — *scraped* is a fetch word, but the column is
written on a parse outcome.

**The ownership rule this plan adopts:** *the component that spends the resource
records having spent it; the component that derives meaning records the meaning.*

Only the first needs to survive a downstream outage. Being stale-but-known is a
benign state; looping is not.

## Design — three facts, three owners

| Fact | Column | Owner | Drives |
|---|---|---|---|
| We have a price from some source | `last_seen_at` *(exists, unchanged)* | processor | `is_price_stale`, 24h |
| **We spent a detail request** | **`last_detail_fetched_at`** *(new)* | **scraper**, via `release_claims` | the loop guard |
| We got full detail data | `last_detail_enriched_at` *(renamed from `last_detail_scraped_at`)* | processor | `is_full_details_stale`, 7d |

`last_seen_at` already is the "last priced" fact — carousel writes set it while
deliberately leaving the circuit breaker alone
([`detail_writer.py`](../../processing/writers/detail_writer.py), the carousel
branch). It stays processor-owned, correctly: carousel and SRP prices are
*produced by the parser*, and it is not the guard against the expensive action.

### The scraper already reports what it needs to record

No new endpoint and no new call. `POST /scrape/claims/release`
([`ops/routers/scrape.py`](../../ops/routers/scrape.py)) already receives
`results: List[ReleaseResult]` with a per-listing `status` of `ok`, `failed` or
`skipped`, and already opens a `db_cursor` to delete the claim rows.
`last_detail_fetched_at` is set by adding one `UPDATE` inside that same cursor
block, so it commits or rolls back with the delete.

Two things about the handler as it stands today, so neither surprises the
implementer:

- **Its docstring is stale.** It says the handler "marks the run as finished";
  it does not. The claim `DELETE` is the handler's only statement, and there is
  no runs-table write anywhere in it. Do not go looking for one. Correct the
  docstring while adding the update.
- **Ops connects as `PGUSER=cartracker`** (`docker-compose.yml`), the owner of
  `ops.price_observations`, so the new write needs no additional grant —
  unlike the scraper role, which needed `V038`. Confirm rather than assume.

**Set it on `ok` and `failed`, not on `skipped`.** The column means "we spent a
request", and a failed fetch spent one. A `skipped` listing was never attempted.
Blocked/403 listings keep their existing cooldown path; this is not a
replacement for it.

**The only live caller sends `ok` for everything.**
[`scrape_detail_pages.py`](../../airflow/dags/scrape_detail_pages.py) builds its
release payload as `{"listing_id": ..., "status": "ok"}` for every claimed
listing, with the comment *"treat all listings as ok (scraper handles
per-artifact errors)"*, and the `release_claims` task carries
`trigger_rule="all_done"` so it fires even when the fetch task failed. So in
production today, `last_detail_fetched_at` will be stamped on every listing in
every released batch, regardless of what actually happened to it.

That is the correct behaviour for a loop guard — a claimed batch consumed
requests whether or not the fetches succeeded, and the backoff should apply to
all of it. It is called out because it means the `failed`/`skipped` branch is a
**contract that no caller currently exercises**: the three-way rule is worth
implementing so the endpoint stays honest, but its tests are endpoint-contract
tests, not evidence about the live path. **Teaching the DAG to report real
per-listing statuses is explicitly out of scope** — that is a behaviour change,
and this plan is a refactor.

### Queue predicate

Today:

```sql
WHERE (ovs.is_price_stale OR ovs.is_full_details_stale)
  AND ovs.current_listing_url IS NOT NULL
```

Gains a fetch backoff, which is the whole fix:

```sql
WHERE (ovs.is_price_stale OR ovs.is_full_details_stale)
  AND ovs.current_listing_url IS NOT NULL
  AND (ovs.last_detail_fetched_at IS NULL
       OR ovs.last_detail_fetched_at < now() - interval '6 hours')
```

Six hours is a starting value, chosen to be far longer than healthy
fetch-to-enrichment latency (processing runs `*/5`, so normally seconds to
minutes) and far shorter than the seven-day enrichment window, so it binds only
when something is wrong. **In a healthy pipeline this predicate never fires** —
enrichment sets `last_detail_enriched_at` first and the seven-day rule takes
over.

**Known limit, stated rather than hidden:** a permanently unparseable page still
loops, at 4x/day instead of 96x/day. A 24x improvement is enough to close the
acute defect, and escalating backoff or an attempt counter is deliberately
deferred until the new metric below shows whether that population is material.

### The failure becomes measurable

`last_detail_fetched_at` recent **and** `last_detail_enriched_at` stale is a
population that means *"we are spending requests and getting nothing back"* —
parser gaps, page-format drift, a stuck processor. Today that is invisible and
shows up only as traffic. Publish it as a gauge; it is the signal that decides
whether the escalating-backoff follow-up is needed, and it overlaps the parser
taxonomy gap that gates backlog [Plan 130](plan_130_parser_input_projection.md).

## Non-goals

These are what keep this a refactor rather than a policy change:

- **Carousel queue admission is unchanged.** A carousel-discovered listing that
  has never been detail-scraped still enters the queue, because a price without
  the full detail suite is not enrichment. A carousel write against an
  already-enriched listing still refreshes its price and still leaves it
  suppressed — today via the `COALESCE` in
  [`upsert_price_observation.sql`](../../processing/sql/upsert_price_observation.sql),
  afterwards explicitly. That is the carousel's purpose: keep prices fresh for
  similar vehicles from the same dealer and buy a day without a fetch.
- No change to the 24-hour price window or the 7-day enrichment window.
- No change to queue priority, the dealer/VIN partitioning, or the three pools.
- No change to the blocked/delisted cooldown paths.
- Not a rename of `last_seen_at`, which five dbt models consume.

## Rejected alternative — scraper writes, processor rolls back

The first shape considered: the scraper sets the timestamp at fetch, and the
processor clears it if parsing fails.

Rejected because **it fails on the exact case that motivates the plan.** A
compensating write from the processor inherits the dependency it is removing: if
processing does not run — paused, crashed, backed up — the rollback never
happens, and the listing is now suppressed for seven days having never been
enriched. That trades a loud failure for a silent one. A loop announces itself
in traffic, solver memory and 403 rates; silent suppression is invisible until
someone asks why coverage fell.

Splitting the column needs no compensating transaction at all, which is the
stronger reason to prefer it.

## Stages

### Stage 1 — Schema and view (expand)

Schema lives in Postgres and changes go through **Flyway**: versioned SQL in
[`db/migrations/`](../../db/migrations/), applied by the one-shot `flyway`
container that every other service gates on with
`service_completed_successfully`. Highest applied version is **`V047`**
(`V047__plan145_recovery_batch_receipts.sql`), and no unmerged branch carries a
`V048` or higher, so this is **`V048__scrape_state_ownership.sql`** and Stage
4's contract is **`V049`**. *(Verified 2026-08-29; re-check `db/migrations/`
before writing the file.)*

**Expand/contract, not a rename in place.** Flyway here is forward-only, there
is no down-migration, and there is no staging environment to rehearse against
([Plan 121](plan_121_staging_environment.md) is unbuilt). A hard
`ALTER ... RENAME COLUMN` would mean the schema changes the instant the stack
comes up, and any still-running `processing` container referring to the old name
fails its writes until it is recreated. Reverting the code would then leave a
renamed column and no way back short of a hand-written migration. Both risks
disappear if the old column simply keeps existing for one release.

`V048` therefore:

1. Adds `last_detail_fetched_at timestamptz` — null, meaning "never fetched",
   which is correct: the queue admits on the other two predicates exactly as it
   does today, so the migration is a **no-op for every existing row**.
2. Adds `last_detail_enriched_at timestamptz`, backfilled
   `= last_detail_scraped_at`. Existing values *are* enrichment timestamps.
3. **Leaves `last_detail_scraped_at` in place**, still written, so the old and
   new code paths are both valid during the deploy.
4. Rebuilds `ops.ops_vehicle_staleness` and `ops.ops_detail_scrape_queue` to
   read the new columns and add the backoff, preserving the
   `OWNER TO dbt_user` / `GRANT SELECT TO viewer` pattern from
   `V040__detail_scrape_circuit_breaker.sql`. The enrichment check reads
   through both columns for the length of the dual-write release — see
   [below](#the-enrichment-check-must-read-through-both-columns), which is
   load-bearing.

Rollback at this point is reverting the application commit; the extra columns
are inert and harm nothing.

#### The enrichment check must read through both columns

*Corrected 2026-08-29. As originally written, Stage 1 reopened the loop it
exists to close.*

Stage 1 rebuilds the view to read `last_detail_enriched_at`, but **Stage 2 is
what makes any writer set that column**, and they are separate tickets. In the
window between them a detail write still sets only `last_detail_scraped_at`, so
a freshly enriched listing with `customer_id IS NULL` has
`last_detail_enriched_at` still null, `is_full_details_stale` stays true, and it
is re-queued every fifteen minutes — precisely the [Plan
115](plan_115_detail_unenriched_circuit_breaker.md) regression. The new backoff
cannot catch it either, because `last_detail_fetched_at` is unwritten until
Stage 2 as well. The claim that `V048` is "a no-op for every existing row" is
true of the columns and false of the view.

So for the duration of the dual-write release the enrichment check reads
through both columns:

```sql
COALESCE(po.last_detail_enriched_at, po.last_detail_scraped_at)
```

in both the `is_full_details_stale` expression and the `stale_reason` `CASE`,
which are duplicate predicates in `V040` and must stay in agreement. The fetch
backoff needs no such treatment: `last_detail_fetched_at` has no legacy
counterpart, and all-null means the predicate simply does not bind, which is the
intended no-op.

This restores the property the expand step is for — `V048` is now genuinely
inert on its own, and Stage 1 and Stage 2 stay independently deployable and
independently revertable. `V049` collapses the `COALESCE` to
`last_detail_enriched_at` when it drops the legacy column.

### Evidence — Stage 1, 2026-08-30

`V048__scrape_state_ownership.sql` applied to production at **2026-08-30
05:35:06 UTC**, `success = t`, 1,098 ms, on VM commit `8b10160` (PR #284).

The deploy was a `git pull` plus `docker compose run --rm flyway` — nothing
else. The diff from the VM's prior commit `64631de` is documentation, this
migration and its tests, so no image was rebuilt and no service recreated.
Backfill sized 50,770 of 51,082 rows on a 19 MB table.

| Check | Expected | Observed |
|---|---|---|
| Flyway `V048` | success | `t`, 1,098 ms |
| Columns added | 3 `last_detail*` | `scraped_at`, `fetched_at`, `enriched_at`, all `timestamptz` |
| **V040 vs V048 enrichment predicate** | **0 disagreements** | **0**, across 51,081 rows |
| `last_detail_fetched_at` written | 0 | 0 |
| Backfill coverage | ~50,770 | 50,768 |
| View owner / `viewer` SELECT | `dbt_user` / `t` | both views |
| Claim path after the rebuild | 200 OK | `claim-batch` and `release` both 200 |

The predicate row is the load-bearing one. It evaluates V040's enrichment rule
and V048's `COALESCE` rule side by side, in one query at one instant, across
every live row, and finds no row classified differently — the Plan 115
non-regression asserted against production data rather than fixtures, which is
what [the `COALESCE`
section](#the-enrichment-check-must-read-through-both-columns) exists to
guarantee. The zero beneath it is the matching proof that the new backoff
predicate cannot yet bind: null everywhere means the clause short-circuits, so
queue membership is still exactly what `V040` produced.

#### Two rows disagree, by design — and it corrects two claims made above

Immediately after the migration, two rows carried `last_detail_scraped_at` with
`last_detail_enriched_at` still null. They are **not** backfill misses: the
backfill ran inside V048's transaction over every row whose
`last_detail_scraped_at` was non-null at that instant. They are post-migration
detail writes by the Stage-1-era processing image, which knows only the legacy
column. [`detail_writer.py:194`](../../processing/writers/detail_writer.py)
stamps `last_detail_scraped_at = fetched_at` — the *artifact's* fetch time, not
`now()` — which is why their timestamps (05:31) read as pre-migration although
the writes landed after it.

Both rows are covered by the `COALESCE`, which is why the predicate check is
still zero. Two consequences, neither of which changes Stage 1:

- **Test 8 is misstated.** It requires the two columns to "never disagree"
  during the dual-write release. They necessarily disagree between the Stage 1
  and Stage 2 *deploys*, and the count grows with every detail write until
  Stage 2 ships. The invariant begins holding when Stage 2 is deployed, not
  when `V048` is applied.
- **Stage 4 inherits a hazard.** `V049` collapses the `COALESCE` to a bare
  `last_detail_enriched_at`. Any row still carrying only the legacy column at
  that point — this window's rows, less those re-enriched inside seven days —
  would read as never enriched, go `is_full_details_stale`, and re-enter the
  loop this plan exists to close. `V049` should repeat the backfill immediately
  before collapsing the predicate. Cheap to add; silent if missed. Recorded
  here as an observation; amending Stage 4 is a separate change.

  Stage 2 narrows this but does not close it. Rather than passing one value
  under two names, its writers pass a single `last_detail_enriched_at` and the
  SQL binds it to both columns, so the two become physically unable to
  disagree — which is what its commit message means by making `V049`'s drop
  safe rather than hopeful. That guarantee covers every write *after the Stage
  2 image is deployed*. It cannot cover the rows written in the window this
  section measured, which is precisely the population `V049` would misread.

#### Gate — closed 2026-08-30, with one check reassigned

CAR-12's four Exit checks, as resolved:

- `V048` applied on prod with no row-count change to existing rows. **Met.**
- The view rebuild returns an identical rowset. **Met** — on fixtures in CI (93
  passed, PR #284) and on the full live table in production, per the table
  above.
- 24h soak, no spurious guard trips. **Waived by the maintainer**, on the
  ground that the guard cannot trip while `last_detail_fetched_at` is null on
  every row, which the run above measured directly.
- Plan 142 held set drops `scrape_detail_pages`. **Reassigned to Stage 3
  (CAR-24).** The drop is earned by the backoff actually binding, which needs
  Stage 2's `release_claims` write; with Stage 1 alone, pausing
  `results_processing` still loops the scraper, so
  [Plan 142](plan_142_planned_host_maintenance.md)'s three-DAG hold remains
  correct. Stage 3 already owns the production verification that licenses the
  change.

### Stage 2 — Writers

1. `release_claims` sets `last_detail_fetched_at = now()` for `ok` and `failed`
   results, in the transaction that deletes the claims.
2. Write **both** `last_detail_enriched_at` and the legacy
   `last_detail_scraped_at` in `detail_writer`, `srp_writer` and
   `upsert_price_observation.sql`, until Stage 4 drops the latter. The
   `COALESCE` semantics are preserved verbatim on both — SRP and carousel still
   pass null and must not advance enrichment.

### Stage 3 — Observability and verification

1. Publish the fetched-but-unenriched gauge.
2. Verify in production that a deliberately paused `results_processing` produces
   **no** re-claim of the same listings after the backoff window — which is this
   plan's whole point, and is directly reusable as Plan 142's Stage 0 item 3
   evidence.

### Stage 4 — Contract

Once Stage 3 has verified the new columns in production, a second migration
(`V049`) drops `last_detail_scraped_at` and the dual write, and collapses the
Stage 1 `COALESCE` in both view predicates to a bare
`last_detail_enriched_at`. Deliberately a separate deploy: it is the only
irreversible step, and it should not share a window with the change that proves
the replacement works.

Blast radius is small and was measured (re-verified 2026-08-29):
`last_detail_scraped_at` appears in three files under `processing/`
(`writers/detail_writer.py`, `writers/srp_writer.py`,
`sql/upsert_price_observation.sql`), in `V040`, and — the original measurement
missed these — in **five test files**: `tests/processing/test_detail_writer.py`,
`tests/processing/test_srp_writer.py`,
`tests/integration/processing/test_detail_processing.py`,
`tests/integration/processing/test_srp_processing.py` and
`tests/integration/sql/test_ops_views.py` (42 occurrences in total). Nothing in
`dbt/`, `dashboard/`, `ops/` or `scraper/` refers to it. Stage 4 is therefore a
larger test edit than a production-code edit.

## Tests

1. A listing fetched but not processed is not re-claimed inside the backoff.
2. It *is* re-claimed after the backoff — the guard is a delay, not a deletion.
3. `skipped` results do not set `last_detail_fetched_at`; `ok` and `failed` do.
   An endpoint-contract test — no caller sends anything but `ok` today, so this
   asserts the handler's rule, not observed production behaviour.
4. A carousel write against an enriched listing refreshes price and advances
   neither fetch nor enrichment.
5. A carousel-discovered listing with no prior detail scrape still enters the
   queue (the non-goal, asserted).
6. SRP writes advance neither fetch nor enrichment.
7. The queue is unchanged for every listing with `last_detail_fetched_at IS
   NULL`, which is the entire table on the day of the migration.
8. During the dual-write release, `last_detail_enriched_at` and
   `last_detail_scraped_at` never disagree — the check that makes Stage 4's drop
   safe rather than hopeful.
9. **Stage 1 alone does not regress Plan 115.** With `V048` applied and the
   Stage 2 writers *not* deployed, a detail write that sets only
   `last_detail_scraped_at` still suppresses the listing for seven days. This
   is the test that pins the `COALESCE` above; without it the regression is
   silent and only visible as traffic.

## Intersections

### Plan 115 — same goal, superseded mechanism

Plan 115's goal stands and its behaviour is preserved: a successful detail scrape
that yields no `customer_id` is still suppressed for seven days. This plan
replaces the *mechanism* by which the loop is prevented, moving the guard next
to the fetch so that new causes cannot reopen it. Plan 115 is complete and
archived; this does not reopen it.

### Plan 142 — the plan that found it

Plan 142 Stage 0 item 3 must currently hold `scrape_detail_pages` alongside
`results_processing`, purely because of this coupling. Once this lands, a
*processing* pause no longer implies a *scraper* pause and the held set can drop
back to the two DAGs originally proposed.

It does **not** remove the need to quiesce scrapes during a host reboot — you
still do not want fetches in flight while the machine goes down. It removes one
coupling, not the maintenance gate.

**Sequencing: this plan does not block Plan 142's window.** Plan 142's Phase A is
inert, and its corrected three-DAG hold is correct today whether or not this
lands.

### Plan 130 — parser taxonomy

Stage 3's gauge measures the population Plan 130's trigger depends on. Neither
blocks the other.

## Success criteria

1. A detail fetch is recorded by the component that performed it, in the same
   transaction that releases the claim.
2. With `results_processing` stopped for an hour, no listing is fetched more
   than once.
3. Carousel and SRP semantics are byte-for-byte unchanged in effect: same queue
   membership, same 24-hour price window, same 7-day enrichment window.
4. The fetched-but-unenriched population is published and non-mysterious.
5. No new endpoint, and no new call between the scraper and ops.
