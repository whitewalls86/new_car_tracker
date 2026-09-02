# Plan 157: April Reprocessing Enrichment

## Status

**BACKLOG, written 2026-08-29.** Priority **57 (medium)**. Effort **M**.

Trigger: **Plan 125 is complete.** This plan writes row-level updates into
`silver_normalized/observations`, which is safe to attempt only once that table
is an Iceberg table with merge-on-read deletes and snapshot isolation. Plan
125's Gate C spike already proved the exact mechanism on this exact table (see
*Method*); this plan is its first real workload, not a new capability.

Depends on [Plan 145](plan_145_april_cutover_reconciliation.md) Stage 5 having
produced and retained its frozen parsed output, which it has.

## Problem

April 2026 silver is two datasets wearing one schema, and the older half is
missing data that still exists in bronze.

[Plan 100](plan_100_historical_data_migration.md) migrated the legacy
`detail_observations`, `srp_observations` and `detail_carousel_hints` tables
into MinIO silver for `fetched_at < 2026-04-21` — the date the Airflow
processing service went live. Rows on either side of that boundary have very
different column coverage, because the legacy schema simply did not have the
columns. Plan 100's `silver/detail` mapping carries `dealer_name`, `dealer_zip`
and `customer_id` and **nothing else dealer-side**: no `dealer_street`,
`dealer_city`, `dealer_state`, `dealer_phone`, `dealer_website`,
`dealer_cars_com_url`, `dealer_rating`, no `seller_id`.

Plan 145 Stage 4 reparsed the whole April population from the retained bronze
HTML with the current production parser. That output contains the missing
fields, because the bytes always had them — only the pipeline that consumed
them was poorer.

## Evidence

Measured 2026-08-29 during Plan 145 Stage 5 slice 3 Phase A, over 19,872
exact-distance `already_represented` rows sampled from compare run
`cmp-6c7c90d807bbdf13` (reservoir, seed 145), matched to silver on the same
key the parser control uses — same listing, same source, same microsecond:

| | rows | disagreeing with the reparse | mean fields |
|---|---:|---:|---:|
| `fetched_at >= 2026-04-21` | 11,665 | 4 (0.03%) | 0.00 |
| `fetched_at < 2026-04-21` | 8,404 | **8,404 (100.0%)** | **12.19** |

By attribution, joining the silver row's `artifact_id` to
`ops_normalized/artifacts_queue_events`:

| silver row | rows | disagreeing |
|---|---:|---:|
| artifact unmapped (no queue event) | 8,120 | 8,120 (100.0%) |
| same object as the one reparsed | 11,949 | 288 (2.4%) |
| different object | **0** | — |

Three things follow, and the third is this plan.

1. **The reparse is faithful.** Against rows production actually wrote from the
   same artifact, it reproduces production at 0.03%. This is not a plan to fix
   a parser.
2. **The pre-cutoff cohort is unreproducible by the legacy pipeline**, not
   wrong. 97% of those silver rows are not even attributable to a queue event,
   because Plan 100 wrote their events to `bronze/artifact_events/`.
3. **The recoverable data is roughly 12.19 silver fields per pre-cutoff row.**
   Pre-cutoff rows were 42% of the sampled exact-match population. Scaled
   across Plan 145's `already_represented` family of 4,977,697 rows, the order
   of magnitude is **~2M rows** — about 3× the 701,375 rows Plan 145 imports.
   The full-family rate is unmeasured; the split is temporal, so it should
   hold, but Stage 1 of this plan measures it rather than assuming it.

## Objective

Enrich the pre-cutoff April silver observations with the fields the reparse
recovered, without changing what was observed, without duplicating rows, and
with the pre-enrichment state remaining queryable.

**This is a better extraction from the same bytes, not a revision of history.**
That distinction is the plan's whole licence to write: `fetched_at`, `price`,
`listing_state` and every other observed value must be preserved exactly, and
only fields that were NULL because the legacy schema lacked the column may be
filled.

## Method

Plan 125's Gate C spike (`plan_125_duckdb_to_iceberg_migration.md`, the
`add_files` section) established every mechanical step against
`silver_normalized/observations` itself:

1. an Iceberg table can be created with `LOCATION` pointing at the existing
   prefix;
2. plain, Iceberg-unaware Parquet writes — exactly what `compact_silver.py`
   does — land inside it;
3. `add_files` registers those objects into the manifest **with no data copy**,
   filename preserved, no rewrite;
4. `SELECT` returns correct data;
5. a `DELETE` against rows in an `add_files`-imported file produces a
   **position-delete file rather than a rewrite**, under the merge-on-read
   decision the plan already records.

Step 5 is the whole reason this plan waits for 125. The enrichment becomes a
`MERGE INTO`: matched pre-cutoff rows are marked dead in metadata and the
enriched rows are appended, in **one atomic snapshot commit**. No compacted
object is rewritten, silver does not move, and the pre-enrichment April stays
queryable at the prior snapshot — so "what changed" is a query rather than an
archaeology exercise.

Contrast the pre-125 alternatives, both rejected:

- **Rewrite the compacted Parquet in place.** Non-atomic, races
  `compact_silver.py`, and destroys the ETag-based freeze that Plan 145's
  inventory depends on.
- **Insert the enriched rows as new rows.** Manufactures duplicate
  `(listing_id, fetched_at)` observations. Verified 2026-08-29 that nothing in
  the path deduplicates: `_INSERT_SQL` has no `ON CONFLICT`,
  `staging.silver_observations` has only a `bigserial` primary key,
  `flush_silver_observations.py` does not deduplicate, and
  `dbt/models/staging/stg_observations.sql` has no `distinct`, `row_number`,
  `qualify` or `group by`.

### Inputs, already frozen

Nothing needs re-parsing. Plan 145's Stage 4 output is retained and
fingerprinted:

| object | count | bytes |
|---|---:|---:|
| `recovery/plan145/parsed/rows/` | 1,204 | 242,126,394 |
| `recovery/plan145/parsed/inputs/` | 1,204 | 87,411,145 |

Both are ETag-fingerprinted in
`recovery/plan145/inventory/cmp-6c7c90d807bbdf13.json`. **Do not prune
`recovery/plan145/parsed/` on storage-pressure grounds** — at ~329 MB it is the
sole input to this plan, and regenerating it costs a ~17-hour reparse of
983,043 objects. No DAG currently touches the prefix; Plan 145 Stage 6 deletes
the *legacy* Parquet, not this.

### Stages, sketched

1. **Measure the real population.** The 42% / 12.19-field figures come from a
   19,872-row sample. Compute them across the whole `already_represented`
   family, per field, so the enrichment set is a measurement.
2. **Define the fill rule and prove it is non-destructive.** Only fill where
   silver is NULL and the reparse has a value; never overwrite a non-NULL
   silver value; never touch `fetched_at`, `listing_id`, `source`, `price`,
   `listing_state`. A field where both sides are non-NULL and differ is a
   **stop**, not a preference — that case should not exist post-cutoff and its
   appearance means the join is wrong.
3. **Dry-run the `MERGE INTO`** against a Plan 120 local snapshot, with a
   before/after diff by snapshot id.
4. **Apply**, in one commit, with the prior snapshot id recorded.
5. **Verify** by reading both snapshots and asserting that only NULL→value
   transitions occurred, and that row counts are unchanged.

## Gate

- Every enriched row differs from its pre-enrichment version only in fields
  that were NULL before.
- Row count is unchanged: no duplicate `(listing_id, fetched_at, source)`
  observation exists after the merge.
- `fetched_at`, `listing_id`, `source`, `price` and `listing_state` are
  byte-identical before and after.
- The pre-enrichment snapshot id is recorded and the prior state is queryable
  at it.
- No post-cutoff row is modified.
- dbt marts build clean against the post-merge snapshot.

## Out of scope

- **Any change to the parser.** The reparse is done and frozen.
- **Re-running Plan 145's Stage 4.** Its output is the input here.
- **Other months.** The Plan 100 migration covers everything before
  2026-04-21, so the same gap exists in March and earlier. April is the month
  with a retained bronze reparse; extending to March needs its own
  materialization and is a separate decision.
- **Enriching post-cutoff rows.** They already agree at 0.03%.
- **Deciding whether Plan 145's own imports should be enriched.** They carry
  full fields by construction.

## Relationship to other plans

- **[Plan 125](plan_125_duckdb_to_iceberg_migration.md)** is the hard
  prerequisite and this is arguably its best Gate D validation workload: a
  real, bounded `MERGE INTO` over a known population with a verifiable
  before/after, rather than a synthetic exercise.
- **[Plan 145](plan_145_april_cutover_reconciliation.md)** produces the input
  and discovered the gap. It deliberately stays append-only; this plan is where
  the update path lives. Plan 145 will leave April internally inconsistent —
  imported pre-cutoff rows carrying full dealer data beside migrated
  neighbours with the 12-field gap — and this plan is what resolves that.
- **[Plan 100](plan_100_historical_data_migration.md)** created the boundary.
  Note that its schema tables are **not** a reliable description of what is in
  the lake: its `silver/carousel` mapping lists no `make` or `model`, yet
  migrated carousel rows carry them, and no version of `detail_writer` sets
  them on a carousel row. The implementation diverged from the document.
