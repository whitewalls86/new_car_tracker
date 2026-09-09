# Plan 162 Stage Q — the greenfield census, and what production can actually seed

**Measured 2026-09-09.** CI's Postgres is empty and production's carries
hundreds of thousands of rows. [Stage
Q](../plans/plan_162_testing_census_and_restructure.md#stage-q-cis-services-are-productions-in-definition-and-in-contents)
owes *which suites depend on that emptiness* — the measurement and the handoff,
not the repair, because the rehearsal that closes it needs a deployed stack and
belongs to [Plan 121](../plans/plan_121_staging_environment.md).

## The seed nobody wrote

The first design for this measurement was to author a plausible set of
pre-existing rows, run each suite against an empty database and a seeded one,
and record what broke. **That was rejected before it ran, and the reason is a
rule this plan had already landed**: [Stage
W](../plans/plan_162_testing_census_and_restructure.md#stage-w-a-test-may-not-supply-both-halves-of-a-contract)
says a test may not supply both halves of a contract. A seed written here
decides what "populated" means, so the failure set that comes out is a function
of the invention rather than of production — a suite that survives authored rows
may still die on real ones, and one that dies on authored rows may be fine. The
measurement would not have been falsifiable.

So this census reads two things that already exist instead: what production can
seed today, and what the test corpus asserts.

## What production can seed today: two tables

`shared/lake_snapshot_postgres.py:36` is the whole allowlist.

```python
POSTGRES_SNAPSHOT_TABLES = (("public", "search_configs"), ("ops", "tracked_models"))
```

Those are real production rows, exported by production, pinned by id in
`.github/ci_lake_snapshot_pin.json` and loaded by `scripts/seed_lake_snapshot.py`.
Everything else the snapshot carries lands in MinIO as Parquet for dbt, never in
Postgres.

**The lake half is much further along than the Postgres half, and that was the
surprise.** `archiver/processors/lake_source_audit.py:22` defines four source
tables, and three of them are staging event tables' flushed contents:

| source table | path | what it is |
|---|---|---|
| `silver_observations` | `silver_normalized/observations/` | the parsed observations |
| `price_observation_events` | `ops_normalized/price_observation_events/` | `staging.price_observation_events`, flushed |
| `vin_to_listing_events` | `ops_normalized/vin_to_listing_events/` | `staging.vin_to_listing_events`, flushed |
| `blocked_cooldown_events` | `ops_normalized/blocked_cooldown_events/` | `staging.blocked_cooldown_events`, flushed |

`lake_snapshot_cohort.py` already closes the cohort over the first three by VIN
and listing_id, so the archive downloaded on every `snapshot-dbt` run **already
holds bounded, cohort-closed, internally consistent rows for three staging
tables.** What is missing is only a destination: `iter_postgres_plan` places
archive members under `postgres/` into Postgres and everything else goes to
MinIO. Backfilling `staging.*` is a second sink for members already downloaded,
not a reverse loader.

Both flush processors copy whole rows before deleting them, so the reversal is a
reconstruction rather than an approximation:

* `flush_staging_events.py` selects **all** rows up to a max-pk boundary, writes
  them, and deletes only on write success. Every column survives. Its own
  docstring names the one caveat — an interrupted flush can leave duplicate rows
  in Parquet, "acceptable for append-only event logs" — so a backfill must
  dedupe on pk rather than assume uniqueness.
* `flush_silver_observations.py` is the same shape with two documented deltas:
  `written_at` is stamped at flush time and never stored in Postgres, and the
  partition columns are derived from `source` + `fetched_at`.

The six staging tables with no cohort key — `artifacts_queue_events`,
`coordination_state_events`, `coordination_release_evidence`,
`detail_scrape_claim_events`, `silver_observation_events`,
`tracked_model_events` — are a different and smaller problem, because "scoped"
there means a time window rather than a cohort.

**A constraint for whoever extends the allowlist.** `public.authorized_users`,
`public.access_requests` and `ops.machine_tokens` must never enter
`POSTGRES_SNAPSHOT_TABLES`. The snapshot is downloaded in CI from
`cartracker.info` under a token, and `UnknownSnapshotTableError` is precisely
the guard that stops an over-broad archive placing rows. Extending the tuple is
the moment to write that exclusion down as a rule rather than leave it a habit.

## What the corpus asserts: 94 candidates

An assertion that a query returns nothing, or that a count is zero, is a
dependency on what was in the database before the test ran — unless the test
put every one of those rows there itself. Counting the three shapes that mean
"nothing else exists" (`== 0`, `== []`, `not <rows>`) over
`tests/integration/`:

| suite | emptiness-shaped assertions |
|---|---:|
| `sql` | 41 |
| `scripts` | 19 |
| `dbt` | 11 |
| `processing` | 9 |
| `archiver` | 7 |
| `ops` | 5 |
| `lakehouse` | 1 |
| `scraper` | 1 |
| **total** | **94** |

**The instrument's limit, stated rather than discovered later.** This is
syntactic, and it cannot tell a *scoped* emptiness assertion — "no rows for the
id this test just wrote" — from an *unscoped* one — "this table is empty". The
first is correct against any database; only the second breaks against
production. Separating them by reading each call site would be guesswork about
what the `.sql` file behind it binds; **running the suite against a populated
database separates them for free, and that run is exactly what Plan 121 owns.**
So 94 is a candidate population and an upper bound, not a defect count.

A first-pass scan that also counted positive equality (`== 28000`, `== 77`)
returned 300 and was discarded: those are value assertions on rows the test
inserted, and including them would have inflated the number with the one case
that is never a dependency.

## The handoff

**To [Plan 121](../plans/plan_121_staging_environment.md):** run the 94 against
a populated database. That single run reduces the candidate list to the real
one, and needs no instrument beyond the deployed stack the plan already
requires.

**Unowned, and named here rather than assigned:** extending what the snapshot
carries into Postgres. Tier 1 is appending non-transient tables to
`POSTGRES_SNAPSHOT_TABLES`, which the exporter and seeder already support
generically. Tier 2 is backfilling `staging.*` from the `ops_normalized/`
Parquet described above. **Plan 120 owned the snapshot artifact and archived
2026-08-18** (`docs/planning/completed_plans.md:51`), so neither tier has a home
today. The census above is what should scope them: it says which tables are
worth carrying, rather than leaving that to be guessed.
