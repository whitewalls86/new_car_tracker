# Plan 147: Scrape State Ownership — Separating Fetch From Enrichment

## Status

DRAFT, written 2026-08-23 after [Plan 142](plan_142_planned_host_maintenance.md)
Stage 0 found that pausing `results_processing` for a maintenance window would
put the detail scraper into a re-scrape loop.

Priority **75 (medium)**. Effort **S**.

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

No new endpoint and no new call. `POST /scrape/claims/release` already receives
`results: List[ReleaseResult]` with a per-listing `status` of `ok`, `failed` or
`skipped`, and already writes to the database in the same transaction that
deletes the claims. `last_detail_fetched_at` is set there.

**Set it on `ok` and `failed`, not on `skipped`.** The column means "we spent a
request", and a failed fetch spent one. A `skipped` listing was never attempted.
Blocked/403 listings keep their existing cooldown path; this is not a
replacement for it.

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
`service_completed_successfully`. Highest applied version is `V042`, so this is
**`Vn+1__scrape_state_ownership.sql`**.

**Expand/contract, not a rename in place.** Flyway here is forward-only, there
is no down-migration, and there is no staging environment to rehearse against
([Plan 121](plan_121_staging_environment.md) is unbuilt). A hard
`ALTER ... RENAME COLUMN` would mean the schema changes the instant the stack
comes up, and any still-running `processing` container referring to the old name
fails its writes until it is recreated. Reverting the code would then leave a
renamed column and no way back short of a hand-written migration. Both risks
disappear if the old column simply keeps existing for one release.

`Vn+1` therefore:

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
   `V040__detail_scrape_circuit_breaker.sql`.

Rollback at this point is reverting the application commit; the extra columns
are inert and harm nothing.

### Stage 4 — Contract

Once Stage 3 has verified the new columns in production, a second migration
(`Vn+2`) drops `last_detail_scraped_at` and the dual write. Deliberately a
separate deploy: it is the only irreversible step, and it should not share a
window with the change that proves the replacement works.

Blast radius is small and was measured: `last_detail_scraped_at` appears in
three files under `processing/` and in the migrations. Nothing in `dbt/`,
`dashboard/`, `ops/` or `scraper/` refers to it.

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

## Tests

1. A listing fetched but not processed is not re-claimed inside the backoff.
2. It *is* re-claimed after the backoff — the guard is a delay, not a deletion.
3. `skipped` results do not set `last_detail_fetched_at`; `ok` and `failed` do.
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
