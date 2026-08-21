# Plan 128: Cloudflare Challenge Pages Swallowed as Successful Detail Scrapes

## Status

**Draft.** Diagnosed 2026-07-20 from live VM data (Postgres `ops.blocked_cooldown`
/ `staging.blocked_cooldown_events`, DuckDB `mart_cooldown_cohorts` /
`mart_block_rate`, the live `cartracker-ops:8060` gauges, scraper `app.log`, and a
MinIO artifact pull).

## Summary

The blocks are **real**. Cloudflare serves `Just a moment...` 403 challenge pages
on the detail scraper's curl_cffi session path, and the scraper correctly records
them. The bug is downstream: **processing does not recognize a challenge page**,
so it writes the 403 artifact as a *successful* "active" observation — which
refreshes freshness timestamps and **clears the cooldown row the scraper just
wrote**. The result defeats exponential backoff, inflates the Grafana gauges, and
masks genuinely-blocked listings as fresh.

This corrects an earlier (wrong) hypothesis that FlareSolverr was misreporting
solved pages as 403. It is not — the FlareSolverr bootstrap always logs
`status=200`; the 403s come from the session fetch and are genuine challenges.

## Evidence (live, 2026-07-20)

Trace of one listing (`0da1b2f0…78c7`) in run `d06fc7d0`, from `app.log`:

```
16:00:18  scrape_detail_fetch: listing_id=0da1b2f0… run_id=d06fc7d0…
16:00:33  curl_cffi CF-session returned 403 … title='Just a moment...'
16:00:33  detail fetch HTTP 403 for listing_id=0da1b2f0…
16:00:33  blocked_cooldown updated (attempts=1)
```

The artifact written for that fetch (`de308cec….html.zst`, pulled from MinIO) is
**6,559 bytes, `<title>Just a moment...</title>`** — a Cloudflare interstitial,
no vehicle data. Yet:

- `staging.artifacts_queue_events`: that artifact went to status **`complete`**.
- `ops.price_observations` for the listing: `last_detail_scraped_at = 16:00:18`,
  `last_seen_at = 16:01:37`, `customer_id = 147153` (stale, preserved by COALESCE).
- `ops.blocked_cooldown`: **no row** for the listing now — cleared by processing.

Same pattern for all 8 listings blocked in that batch.

### Smoking gun for backoff

`mart_block_rate` shows `block_increments = 0` and `max_attempts_seen = 1` for
**every recent hour**. No listing ever reaches attempt 2 through the normal flow,
because processing deletes the cooldown row between blocks. The live table's
older rows at attempts 2–6 are frozen since 2026-04-27 — a prior population, not
current behavior.

### Gauge drift

| Metric | Gauge | Live truth (`ops.blocked_cooldown`) |
|---|---|---|
| `cartracker_cooldown_backlog` | 31,924 | 3,414 (attempts < 5) |
| `cartracker_cooldown_permanent` | 131 | 192 (attempts ≥ 5) |

## Root Cause

### 1. Processing has no Cloudflare challenge-page guard (primary)

`parse_cars_detail_page_html_v1` ([parse_detail_page.py:281-299](../processing/processors/parse_detail_page.py#L281-L299))
looks only for `_detect_unlisted` markers and the `initial-activity-data` JSON
blob. A challenge page has neither, so it falls through to
`listing_state = 'active'` with every field `None`. `_process_detail_page`
([batch.py:161-176](../processing/routers/batch.py#L161-L176)) then calls
`write_detail_active`, which:

- upserts `ops.price_observations` via `upsert_price_observation.sql`: COALESCE
  preserves the stale `customer_id`, but **refreshes `last_seen_at` and
  `last_detail_scraped_at`** to the fetch time;
- marks the artifact **`complete`**;
- runs `CLEAR_BLOCKED_COOLDOWN` ([detail_writer.py:270](../processing/writers/detail_writer.py#L270)),
  deleting the cooldown row the scraper wrote seconds earlier.

There is no detection of `Just a moment...` / `Attention Required` / CF markers
anywhere in the parse or write path.

### 2. `CLEAR_BLOCKED_COOLDOWN` emits no `'cleared'` event

Even for legitimate clears, the delete writes no lifecycle event, so
`mart_cooldown_cohorts` (state = `arg_max(num_of_attempts, event_at)` over the
event log, [mart_cooldown_cohorts.sql:13](../dbt/models/marts/mart_cooldown_cohorts.sql#L13))
never drops a resolved listing. The `'cleared'` value already exists in the
`event_type` CHECK constraint — it is simply never written. This is what makes
the gauges grow monotonically. (Volume of spurious clears drops sharply once #1
is fixed, but the accounting bug is independent.)

### 3. Orphaned rows for delisted vehicles

Only 111 of 3,606 live rows (3%) still exist in `ops.price_observations`; the
rest are delisted vehicles whose cooldown row can never clear. Nothing evicts
them. Independent backlog inflation.

## Consequences (maps to the three questions asked)

- **Exponential backoff is defeated.** The `12h · 2^(n-1)` formula is correct,
  but processing resets every genuinely-blocked listing to attempts=1 by clearing
  the row, so `fully_blocked` is essentially never reached and `block_increments`
  is 0. Backoff never engages for repeat offenders.
- **Grafana gauges are wrong** (`cooldown_backlog` ~9× high) — see #2.
- **Missed data masked as fresh** (the real worry): every genuine block is
  recorded as a successful detail scrape, refreshing `last_seen_at` and
  `last_detail_scraped_at`. This suppresses re-queueing via the Plan 115 / V040
  circuit breaker for 7 days and makes blocked listings look healthy.

## Fix

### Phase 1 — Recognize challenge pages in processing (primary)

Add a Cloudflare/interstitial detector and short-circuit before any write.

**Discriminator (validated against a real 3-page corpus):** a genuine detail
page carries a parseable `initial-activity-data` JSON blob; the challenge page
does not. That presence is the safety gate — if the page has real data it is
never a challenge, regardless of anything else it contains. Only when the blob
is absent do we fall back to the CF interstitial title (`Just a moment...`,
`Attention Required`, `Checking your browser`).

**Do NOT key on `cdn-cgi/challenge-platform`.** A first attempt used that marker
and had to be reverted from production: Cloudflare injects that script reference
into *every* cars.com page, so it matched 100% of valid detail pages and caused
the processor to `skip` real pages. Confirmed by pulling the wrongly-skipped
artifacts (192KB Honda CR-V page, title `New 2026 Honda CR-V… $41,675`, *has*
`initial-activity-data`, and *also* contains `cdn-cgi/challenge-platform`). The
captured pages are committed as regression fixtures under `tests/fixtures/html/`.

When detected, `_process_detail_page` must:

- **not** upsert `price_observations` (no freshness refresh, no COALESCE write);
- **not** clear `blocked_cooldown` (leave the scraper's row intact so backoff
  accumulates);
- mark the artifact a terminal non-success status (`blocked`/`skip`, not
  `complete` and not an infinite `retry`);
- optionally emit an `'incremented'`/`'blocked'` reconciliation event only if we
  decide processing (not the scraper) should own block accounting — default: no,
  the scraper already recorded it at fetch time; processing just stops undoing it.

Seam choice: implement in processing (it has the parser and is authoritative).
Alternative considered — have the scraper not enqueue 403 challenge artifacts as
`detail_page` work at all ([scrape_detail.py:206-231](../scraper/processors/scrape_detail.py#L206-L231));
noted but not chosen, since keeping the artifact for audit is useful and the
processing guard is needed regardless.

Tests: `tests/processing/test_parse_detail_page.py` (incl. a `TestRealCorpus`
that runs the three captured pages through the parser — real pages must not be
`blocked`, challenge must be) + `test_batch_functions.py` asserts the blocked
short-circuit does no price_observation write, no cooldown clear, `skip` status.

### Phase 2 — Emit `'cleared'` events on legitimate clears

Emit a `'cleared'` lifecycle event whenever `CLEAR_BLOCKED_COOLDOWN` actually
removes a row (use `RETURNING`/rowcount; skip no-op clears). Both call sites in
`detail_writer.py` (active + unlisted). Update `mart_cooldown_cohorts` to treat a
listing whose latest event is `'cleared'` as no longer in cooldown. Rebuild and
confirm the gauges converge on the live-table truth.

Tests: `tests/integration/scraper/test_blocked_cooldown.py` + a dbt cohort test.

### Phase 3 — Cleanup (implemented in the `plan-128-cleanup` PR)

Three durable maintenance jobs, all as `POST /maintenance/*` endpoints on the
ops service (runs as the `cartracker` owner — full privileges) driven by the
existing `orphan_checker` DAG pattern. ops already reads the DuckDB analytics
store (see `ops/metrics/duckdb_gauges.py`) and writes Postgres.

**3a. Stuck-`processing` artifact reaper** — nothing resets `ops.artifacts_queue`
rows stranded in `status='processing'` when the processing service dies
mid-batch (`orphan_checker` only reaps `detail_scrape_claims`). ~56 stuck since
2026-07-09. Endpoint `reap-stuck-processing`: select rows whose most recent
`processing` event is older than 2h; for each, `retry` if the MinIO object still
exists (`shared.minio.object_exists`), else `skip`. Wired into `orphan_checker`
(every 5 min). The 2h/last-event gate avoids reaping an actively-processing row
from a legitimately-old pending backlog. Also note: these rows inflate
`/deploy/status` `number_running`, so this unblocks the deploy-drain gate too.

**3b. Delisted-vehicle cooldown eviction** — `evict-delisted-cooldowns`: delete
`ops.blocked_cooldown` rows whose `listing_id` is absent from
`ops.price_observations` (~3,495 delisted, can never clear via a successful
scrape), emitting a `'cleared'` event for each. Naturally idempotent (deleted
rows are gone). Wired into `orphan_checker`.

**3c. Durable cohort reconciliation** (the ~32k gauge inflation) —
`reconcile-cooldown-cohorts`: the mart counts a listing while its latest
analytics event is `blocked`/`incremented`, but ~28k were deleted from the live
table long ago without a `'cleared'` event. Read the `blocked_cooldown_events`
parquet directly over S3 (`shared.duckdb_s3.get_duckdb_s3_connection` — a fresh
in-memory DuckDB, not the persisted `analytics.duckdb` view, which would contend
with dbt's write lock and whose plain connection lacks S3 credentials) for
listings whose latest event is `blocked`/`incremented`, anti-join the live
`ops.blocked_cooldown`, and emit a `'cleared'` event for each orphan.
**Idempotency:** skip listings that already have a pending `'cleared'` event in
`staging.blocked_cooldown_events` (not yet flushed), so re-runs before the next
flush+build don't duplicate. Runs on a **slow cadence** (hourly, after the
analytics build) — not every 5 min — because its effect lands only after the
staging→parquet flush and next mart build. Fixes the historical 32k and
self-heals any future drift.

### Phase 4 — Freshness repair: **audited, no action taken** (2026-07-21)

Pre-fix, challenge pages were written as successful detail scrapes, refreshing
`last_seen_at` and `last_detail_scraped_at` on no real data. Phase 1 stopped new
corruption; this phase audited whether the *existing* corruption warranted a
one-time repair. **Conclusion: it does not.** Evidence:

**Blast radius is ≤21 listings out of 52,884 (0.04%).** The V040 circuit breaker
only suppresses re-queue when `customer_id IS NULL`
(`is_full_details_stale = customer_id IS NULL AND (last_detail_scraped_at IS NULL
OR last_detail_scraped_at < now() - 7 days)`). A challenge page that hit a
listing which already had a `customer_id` changed nothing material — COALESCE
preserved the id, so the listing was never flagged unenriched regardless of the
refreshed timestamp. Only unenriched listings can actually be suppressed, and in
the risk window (last 7 days, before the 2026-07-20 18:50 fix deploy) that is 21
rows.

**Both corruptions self-heal, and are already rolling off.**
- `last_seen_at` drives `is_price_stale` on a 24h window → fully self-healed
  within 24h of the fix deploy.
- `last_detail_scraped_at` drives the 7-day recheck → the earliest affected rows
  (2026-07-14) rolled off on 2026-07-21; the latest (2026-07-20) roll off by
  2026-07-27. Nothing is permanently stuck.

**The corrupted subset can't be reliably identified, and a blanket reset would
do harm.** Only 2 of the 21 still have their `last_artifact_id` row in
`ops.artifacts_queue` (the rest were removed by `cleanup_artifacts`). Both
traceable artifacts turned out to be **real 172KB detail pages** with
`initial-activity-data` (`New 2026 Mazda CX-50 Hybrid Premium…`) — i.e.
*legitimate* Plan 115 circuit-breaker suppressions (a real scrape that genuinely
found no `customer_id`), not challenge corruption. Resetting
`last_detail_scraped_at` for the 21 would defeat the Plan 115 breaker for the
legitimate ones and cause exactly the repeated re-scraping that plan was built
to prevent.

**Decision:** let it age out. Actively mutating prod data for ≤21 self-healing
rows — where the traceable sample shows no corruption at all and a blanket reset
would undermine Plan 115 — is more risk than value. Re-open only if the
unenriched-suppressed population is still non-trivial after 2026-07-27.

## Verification

1. After Phase 1: process a captured challenge artifact → no `price_observations`
   write, cooldown row survives, artifact ends non-`complete`.
2. After a real batch: `mart_block_rate.block_increments` becomes non-zero and
   `max_attempts_seen` climbs past 1 for repeat-blocked listings.
3. After Phase 2: `cartracker_cooldown_backlog` matches
   `count(*) FILTER (WHERE num_of_attempts < 5)` within one refresh.
4. After Phase 3: live row count tracks only listings still in
   `ops.price_observations`.

## Notes

- Prod runs on the VM (SSH); this local checkout does **not** volume-mount into
  any container, so branch work here is safe.
- Scraper app logs are JSON in `/usr/app/logs/app.log`, shipped to Grafana via
  promtail/Loki (not stdout) — they are fully available, just not in
  `docker logs`.
- The scraper-side adaptive delay backing off on 403 (1→2→…→30s) is **correct**
  behavior against genuine blocks; do not change it.
- Do not change the backoff formula (`12h · 2^(n-1)`, permanent at 5+) — it is
  correct; the inputs feeding it were being erased.
