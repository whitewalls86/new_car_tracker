# Plan 143: Analytics Serving Snapshot and Reader Consolidation

## Status

**Stages 0-4 implemented locally 2026-08-18; CI dbt-artifact verification,
deployment, and the 24-hour Stage 5 soak remain pending.** Implementation is in
commits `bbd9b23` and `2cfdb73`. The first commit establishes the shared
connection and saved-SQL boundary; the second publishes the atomic snapshot,
exports metrics directly from `dbt_runner`, and makes `ops` a snapshot-only
presentation consumer.

This plan was written during pre-PR review of the unshipped Plan 136 Stage 1
implementation. That first implementation proved the immediate lock can be
serialized, but review found that it would preserve three architectural
problems: analytics SQL embedded in Python, `ops` acting as a Prometheus proxy,
and a second `ops` DuckDB collector planned for the public page.

Priority **94 (critical)**. Effort **M plus a 24-hour production soak**.

No Plan 143 runtime change is deployed. Commit `584f100` remains prototype
evidence, not an accepted serving design and must not be promoted as-is.

This plan is the sole owner of work transferred from:

- [Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 1: fail-loud
  analytics gauges, a last-success timestamp, and removal of the recurring
  cross-process metrics-reader lock;
- [Plan 138](plan_138_public_surface_refresh.md) Stage 4: remove DuckDB and the
  transient `ops.artifacts_queue` freshness query from the public request path;
- [Plan 139](plan_139_test_suite_maintenance.md): cover the formerly
  25%-covered gauge producer while changing its failure contract; and
- [Plan 125](plan_125_duckdb_to_iceberg_migration.md) Gate D: establish the
  serving/cache seam before the authoritative tables move to Iceberg.

## The problem is ownership, not one lock

Production currently has several ways to read the same dbt-built DuckDB file:

1. dbt writes `analytics.duckdb` through `dbt_runner`.
2. The Streamlit dashboard opens it read-only for interactive queries.
3. `ops` opens it for seven Prometheus gauges.
4. `ops` opens it four more times while rendering `/info`.

DuckDB permits one read/write process or multiple read-only processes, not an
independent writer plus readers. The gauge loop made the conflict recurring,
while a public page view could create the same conflict on demand. Retrying or
serializing one caller treats the collision, not the ownership problem.

The first Plan 136 Stage 1 implementation moved the seven gauge queries into
`dbt_runner`, guarded them with the build lock, and had `ops` poll an internal
JSON endpoint every minute. That was safer than two processes opening the file,
but review rejected it as the final design:

- the seven queries were embedded in `dbt_runner/app.py`, contrary to the
  repository's saved-SQL convention;
- `ops` remained an unnecessary metrics intermediary;
- every poll still reopened DuckDB instead of reading a completed build
  artifact;
- `/info` still opened DuckDB independently and still queried Postgres on the
  public request path; and
- Plan 138 would later add another `ops` background DuckDB collector, undoing
  the ownership boundary this work is supposed to create.

There is also a semantic defect in `/info`. Its "last pipeline run" is derived
from `MAX(fetched_at)` over completed rows in `ops.artifacts_queue`. That table
is a transient hot queue; hourly cleanup removes completed rows. It cannot be
the durable truth for analytics freshness.

## Decision

`dbt_runner` publishes one small, versioned analytics serving snapshot after a
successful dbt build. Prometheus reads in-memory values derived from that
snapshot directly from `dbt_runner`. The public page reads a presentation cache
of the same persisted snapshot and performs no DuckDB or Postgres query while
handling a request.

```text
dbt build succeeds
        |
        v
saved SQL over completed marts
        |
        v
dbt_runner --atomic replace--> versioned serving snapshot
     |                              |
     |                              +--> ops presentation cache --> /info
     +--> in-memory gauges --> /metrics --> Prometheus --> Grafana
```

This is a serving extract, not a second analytics database. The authoritative
data remains the dbt-built marts today and Iceberg after Plan 125.

## Ownership contract

| Concern | Owner after Plan 143 | Explicitly not responsible |
|---|---|---|
| Build analytics marts | `dbt_runner` / dbt | Public-page rendering, Grafana |
| Produce and atomically persist the serving snapshot | `dbt_runner` | Generic ad hoc query API |
| Export the seven stable gauges and snapshot health | `dbt_runner` | Proxying through `ops` |
| Scrape and retain metrics | Prometheus | Opening analytics storage |
| Render the public page from a local presentation cache | `ops` | Querying DuckDB or analytics-related Postgres state |
| Interactive analytics | Streamlit dashboard | Snapshot production |
| Replace the authoritative DuckDB marts with Iceberg | Plan 125 | Changing public metric names |

`ops` remains in the `/info` chain because it owns the Jinja template, static
assets, and public route. It is removed from the metrics chain and from
analytics storage access.

## Repository conventions this plan must preserve

### SQL lives in `.sql` files

Snapshot aggregation SQL belongs under `dbt_runner/sql/` and is loaded through
`shared.query_loader.load_query`. Python owns orchestration, validation,
serialization, and failure handling; it does not own SQL strings.

Use a small number of queries at real failure-isolation boundaries, not one
file per scalar and not one monolith merely to minimize calls. At minimum:

- one saved query for the seven Prometheus values and their analytics
  `data_through` timestamp; and
- one saved query for the DuckDB-derived public statistics.

The exact saved SQL executed in production must run in an integration test
against the DuckDB artifact produced by dbt. Mocking `con.execute()` proves
Python branching, not the query contract.

### Shared code stops at the connection boundary

Add a narrow shared analytics connection factory used by `dashboard` and
`dbt_runner`. It owns current path/backend configuration and returns a
read-only connection. It does not own retries, locks, result shapes, caching,
Prometheus, or Streamlit DataFrames; those policies differ by service.

Do not extend `shared/duckdb_s3.py`. That helper intentionally creates an
in-memory DuckDB connection configured for raw MinIO Parquet. The analytics
helper opens the modeled serving target and is the future chokepoint for a
read-only Iceberg/Lakekeeper attachment or serving extract.

Migrate `dashboard/queries.py` from its private `_load()` implementation to the
existing shared query loader. This is mechanical consolidation, not a dashboard
query rewrite.

A shared Python factory does not coordinate locks across containers. The
ownership and post-build publication rules below solve concurrency; the helper
only prevents connection configuration and tests from diverging.

## Snapshot contract

The persisted document is JSON with an explicit schema version. The default
path should be a dedicated file under the shared analytics volume, configurable
through a neutral `ANALYTICS_SNAPSHOT_PATH`.

```json
{
  "schema_version": 1,
  "backend": "duckdb",
  "refresh": {
    "status": "ok",
    "attempted_at": "2026-08-18T18:00:00Z",
    "last_success_at": "2026-08-18T18:00:00Z",
    "duration_seconds": 0.12
  },
  "data_through": "2026-08-18T17:00:00Z",
  "metrics": {},
  "public_stats": {},
  "errors": {}
}
```

Required behavior:

1. Write a temporary file in the destination directory, flush it, and replace
   the published path atomically. Readers never see partial JSON.
2. Validate every required metric name and numeric value before publication.
3. Preserve `last_success_at` and the last known good public values after a
   failed attempt while recording the new failed attempt and its bounded error.
4. Failed or invalid metric fields export as NaN and never advance the
   last-success timestamp.
5. `data_through` comes from the mart/scrape-volume time boundary, not the
   transient artifact queue and not the wall-clock time of publication.
6. On process start, load and validate the persisted snapshot without opening
   DuckDB. An absent or unsupported snapshot is an explicit empty/not-ready
   state.
7. Do not place secrets, catalog credentials, SQL text, paths containing host
   identities, or unbounded exception bodies in the document.

The stable metric names are:

- `cartracker_observation_count_last_hour`
- `cartracker_artifact_count_last_hour`
- `cartracker_block_events_last_hour`
- `cartracker_extraction_yield_last_day`
- `cartracker_stale_listings_pct`
- `cartracker_cooldown_backlog`
- `cartracker_cooldown_permanent`
- `cartracker_metrics_last_success_timestamp_seconds`

The public snapshot replaces `last_pipeline_run_iso` with
`analytics_data_through_iso`. Existing active-listing, price-observation,
make/model, and throughput values retain their presentation semantics.

## Stages

### Stage 0 — Freeze the contract and retire the rejected prototype — BUILT 2026-08-18

1. Record the exact current metric names, Grafana consumers, public-stat keys,
   SQL dependencies, Compose mounts, and Prometheus jobs.
2. Treat commit `584f100` as a test/rationale source only. Remove its polling
   loop, `ANALYTICS_READER_URL`, and request-time `/analytics/metrics` design
   before deployment.
3. Add a test asserting that `ops` does not import the analytics gauge producer
   or start a metrics-refresh thread.
4. Add a test asserting that no public request opens DuckDB or Postgres.

**Gate 0:** the inventory maps every removed behavior to its replacement and no
consumer depends on the rejected internal endpoint.

### Stage 1 — Consolidate query loading and analytics connections — BUILT 2026-08-18

1. Add the narrow shared analytics connection factory with unit tests for path,
   read-only mode, explicit overrides, and error propagation.
2. Keep `shared.duckdb_s3` unchanged and test the distinction between raw S3
   reads and modeled analytics reads.
3. Make `dashboard/queries.py` use `shared.query_loader` without changing query
   text or result behavior.
4. Add `dbt_runner/queries.py` and saved snapshot SQL files using the same
   loader convention.
5. Extend the existing integration SQL layer to execute the exact snapshot SQL
   against a real dbt-built DuckDB.

**Gate 1:** dashboard SQL smoke tests remain green, snapshot SQL executes on the
real build artifact, and Python contains no snapshot aggregation SQL literals.

### Stage 2 — Publish a durable post-build snapshot — BUILT 2026-08-18

1. After the dbt subprocess exits and releases its file handle, execute the
   saved queries through the shared analytics connection.
2. Validate and atomically publish the versioned snapshot while `dbt_runner`
   still owns the build lifecycle.
3. Load the last valid snapshot at service startup without querying DuckDB.
4. Preserve the previous valid document on serialization, validation, or
   filesystem failure; write no partially valid replacement.
5. Make snapshot publication a named postcondition in the build response.
   Required metric publication failure is not reported as a fully successful
   analytics delivery, even when the dbt subprocess itself exited zero.
6. Record bounded attempt status and duration for diagnosis.

**Gate 2:** concurrent readers observe either the old or new complete document,
never a partial file; a failed build or failed publication cannot corrupt the
last good snapshot.

### Stage 3 — Export analytics metrics at their producer — BUILT 2026-08-18

1. Register the seven stable data gauges and freshness/refresh-health metrics
   in `dbt_runner`.
2. Expose the normal Prometheus `/metrics` surface from `dbt_runner`; scrapes
   read memory and never open DuckDB.
3. Add a direct Prometheus scrape job for `dbt_runner`.
4. Remove the analytics gauge module and refresh thread from `ops`.
5. Keep `ct-metrics-freshness` at 900 seconds, fail loudly on no data or query
   errors, and preserve existing Grafana metric names.
6. Add producer tests for complete, partial/invalid, failed, restart-loaded, and
   stale snapshots plus configuration tests for the scrape target and alert.

**Gate 3:** Prometheus obtains every stable metric from `dbt_runner`, `ops`
exports none of them, and repeated scrapes during a dbt build do not touch or
lock the analytics file.

### Stage 4 — Make `/info` a snapshot consumer, not a database reader — BUILT 2026-08-18

1. Replace the four DuckDB queries in `ops/routers/info.py` with a small
   thread-safe presentation cache loaded from the versioned serving snapshot.
2. Remove the `ops.artifacts_queue` query and its misleading
   `last_pipeline_run_iso` field.
3. Render `analytics_data_through_iso` as "Analytics data through."
4. Preserve the current soft-failure contract: full, partial, stale, and empty
   snapshots all return the public page immediately.
5. Keep snapshot refresh/file I/O out of the request handler. The handler reads
   an in-memory immutable value and never sleeps or retries.
6. Remove DuckDB configuration and dependency from `ops` when no other route
   requires them; mount only the serving snapshot read-only if a shared file is
   the selected transport.

**Gate 4:** `/info` performs no DuckDB, Postgres, or upstream-network call,
remains responsive during a dbt write lock and Postgres outage, and labels
staleness honestly.

### Stage 5 — Deploy, prove the lock is gone, and soak — NOT STARTED

Deploy `dbt_runner`, `ops`, and Prometheus configuration as one compatibility
change. Use the existing admin deploy-intent/drain procedure.

1. Before deployment, record the current gauge values, scrape targets, latest
   dbt build, `/info` fields, and recent `Conflicting lock` log count.
2. Deploy the new producer and consumer, then run one normal scheduled-equivalent
   dbt build to create the first production snapshot.
3. Verify the JSON schema/version, atomic file permissions, timestamps, and
   absence of secret material without publishing the full document.
4. Verify Prometheus reports `dbt_runner` up and all eight stable metric names;
   confirm the `ops` target no longer owns those series.
5. Load `/info` anonymously and confirm full, partial, and stale presentation
   tests were exercised before production; production must still return 200 if
   the snapshot is temporarily unavailable.
6. Observe at least one dbt build while Prometheus continues scraping. There
   must be no recurring DuckDB lock warning from `ops` or `dbt_runner`.
7. Soak for 24 hours across ordinary scrape, processing, flush, and dbt cadences.
   Confirm the freshness timestamp advances only with valid publications and
   the 900-second alert stays quiet while healthy.

**Gate 5:** the 24-hour evidence contains at least one successful snapshot
replacement during normal metric scrapes, no cross-process gauge/info lock
conflict, a responsive public page, and green alert evaluation.

## Rollback

Rollback restores the previous `dbt_runner`, `ops`, and Prometheus configuration
together. The versioned snapshot is additive and may remain on disk; old images
ignore it. Do not roll back only the producer after removing the `ops` gauge
module, or only Prometheus after moving the series owner.

The prototype implementation is not the rollback target. The rollback target
is the last production release before Plan 143.

## Relationship to Plan 125

Plan 143 deliberately advances the serving boundary, not the Iceberg migration.
Today the snapshot producer queries the completed DuckDB marts. At Plan 125 Gate
D, only the producer's connection/query adapter changes to an Iceberg-backed
reader or extract. The JSON schema, public presentation cache, Prometheus metric
names, Grafana rules, and failure semantics remain stable.

The shared connection factory must therefore centralize current backend
configuration without exposing DuckDB-specific settings to `ops` or page code.
It may continue using DuckDB as a read-only Iceberg client if Gate D chooses that
proven option; `analytics.duckdb` simply stops being authoritative.

This plan does not claim to eliminate every current file lock. The Streamlit
dashboard remains a separate read-only DuckDB process until Plan 125 migrates
its broader interactive reader. Plan 143 eliminates the recurring metrics read
and public-page read from that contention set.

## Relationship to Plans 136 and 138

Plan 136 retains solver outcome counters, the corrected volume alert,
drain-aware scheduled recycle, and gated automatic restart. It consumes the
freshness contract built here but owns none of its storage or exporter code.

Plan 138 retains public truth/copy, canonical `/` routing, `/info` redirect,
accessibility, assets, roadmap projection, security headers, and Lighthouse
evidence. Its templates consume the Plan 143 presentation cache; it must not
create another DuckDB collector inside `ops`.

## Expected files

| File | Planned change |
|---|---|
| `shared/analytics_connection.py` | Narrow modeled-analytics connection factory |
| `shared/query_loader.py` | Reused unchanged unless encoding/error context needs a tested clarification |
| `dashboard/db.py` | Use the shared connection factory; keep DataFrame/session policy local |
| `dashboard/queries.py` | Use the shared query loader |
| `dbt_runner/queries.py` | Load saved serving-snapshot SQL |
| `dbt_runner/sql/*.sql` | Metrics and public-stat snapshot queries |
| `dbt_runner/analytics_snapshot.py` | Validate, atomically persist, reload, and publish snapshot state |
| `dbt_runner/app.py` | Post-build publication and direct `/metrics` exposure |
| `dbt_runner/requirements.txt` | Add the chosen Prometheus ASGI/export dependency with an explicit compatible pin |
| `dbt_runner/Dockerfile` | Copy the runner modules and saved SQL, not only `app.py`; prove the image contains them |
| `ops/public_stats.py` | Read/validate the persisted snapshot into an immutable presentation cache |
| `ops/routers/info.py` | Render cached values; remove DuckDB and transient queue reads |
| `ops/app.py` | Remove analytics metric polling; manage only the public presentation cache if needed |
| `ops/requirements.txt` | Remove DuckDB only after the route/import inventory proves no remaining caller |
| `docker-compose.yml` | Neutral snapshot path, read-only `ops` mount, and removal of obsolete reader configuration |
| `prometheus/prometheus.yml` | Scrape `dbt_runner` directly |
| `grafana/provisioning/alerting/rules.yml` | Freshness alert with fail-loud no-data/error behavior |
| `tests/shared/test_analytics_connection.py` | Shared connection contract |
| `tests/integration/sql/test_analytics_snapshot_queries.py` | Execute exact saved SQL against a dbt-built DuckDB |
| `tests/dbt_runner/test_analytics_snapshot.py` | Schema, atomicity, startup, failure, and post-build behavior |
| `tests/ops/routers/test_info.py` | Full/partial/stale/empty rendering with no database calls |
| `tests/test_observability_config.py` | Compose mounts, direct scrape ownership, metric and alert contracts |

## Completion criteria

Plan 143 is complete only when:

- analytics aggregation SQL is saved and integration-tested, not embedded in
  Python;
- dashboard and `dbt_runner` share connection construction without sharing
  service-specific retry/cache/result policy;
- `dbt_runner` publishes a validated snapshot atomically after builds and
  reloads it without opening DuckDB;
- Prometheus scrapes the stable analytics gauges directly from `dbt_runner`;
- `ops` neither produces analytics gauges nor opens DuckDB/Postgres for
  `/info`;
- `last_pipeline_run_iso` is replaced by mart-derived
  `analytics_data_through_iso`;
- public requests remain immediate and useful for full, partial, stale, and
  empty snapshot states;
- the 900-second freshness alert evaluates correctly without changing existing
  dashboard metric names;
- unit, integration SQL, compose/configuration, and image-build checks pass;
- production shows at least one normal dbt publication during a 24-hour soak
  with no recurring gauge/info DuckDB lock conflict; and
- Plans 125, 136, 138, 139, and the default build order point to this plan as
  the sole owner of the serving-boundary work.
