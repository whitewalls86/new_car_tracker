# Plan 98: Bronze Data Architecture — Schema Design & Staging Pattern

**Status:** ACTIVE
**Branch:** feature/new-processors (initial migration); follow-on entities as separate PRs

---

## Problem

The prior approach (Plans 89, 93, 97) established that Postgres owns HOT operational data and MinIO owns analytics. What was missing was a concrete, repeatable pattern for how structured event data moves from Postgres to MinIO without:

- Row-by-row Parquet writes from hot paths (latency, small files, operational coupling)
- Unbounded Postgres table growth (event history accumulates forever)
- Loss of intermediate states (hot table updated in place → cleanup runs → history gone)

---

## Core Principle

> **If it needs to be queryable immediately → Postgres hot table.**
> **MinIO is for raw blobs and scheduled analytical exports.**

Anything "must be queryable now" is an operational concern → Postgres.
Anything historical/analytical is a reporting concern → MinIO Parquet, written on a schedule.

---

## Schema Layout

Three Postgres schemas with distinct ownership and permission boundaries:

| Schema | Purpose | Grows unboundedly? |
|--------|---------|-------------------|
| `public` | Users, configs, admin panel data | No — bounded by user/config count |
| `ops` | Operational hot tables — current state, 1 row per entity | No — cleaned up when entities reach terminal state |
| `staging` | Event buffer tables — flushed to MinIO, then truncated | No — truncated after each export flush |

MinIO is the only layer that grows unboundedly. That is intentional — it's cheap object storage, not a transactional database.

---

## Permission Model

| Role | `public` | `ops` | `staging` |
|------|----------|-------|-----------|
| admin_panel | read/write | read | none |
| scraper_service | none | read/write | INSERT only |
| processing_service | none | read/write | INSERT only |
| export_dag | none | none | read/write |
| dbt_runner | none | read | none (reads Parquet directly) |

The export DAG's credentials cannot see `ops` or `public`. A bug or runaway DAG cannot corrupt operational state.

---

## The Hot + Staging Pattern

For any entity with mutable current state and history worth preserving:

### Table 1: Hot table (`ops.*`)
- 1 row per entity, current state only
- Updated in place on each state change
- Cleaned up when entity reaches terminal state
- Small, fast, indexed for operational queries

### Table 2: Staging table (`staging.*_events`)
- 1 row per state transition, append-only
- Services hold INSERT privilege only — no UPDATE or DELETE
- Never read operationally; only the export DAG reads it
- Flushed to Parquet by export DAG, then TRUNCATED
- Always small — bounded by flush cadence, not by total history

### MinIO Parquet (permanent record)
- Written by export DAG after reading staging table
- Partitioned by `year/month/day/artifact_type` (or entity-appropriate axes)
- Becomes source of truth for historical data once flushed
- Read by dbt for silver/gold transformations

### Write contract for services
On every state transition:
1. UPDATE `ops.<entity>` — set new status, updated_at, etc.
2. INSERT `staging.<entity>_events` — record the transition with timestamp

These two writes are in the same transaction. Either both happen or neither.

---

## Export DAG Design

A single parameterized Airflow DAG handles all staging tables.

```
export_staging_to_minio
  params:
    - table: staging.artifacts_queue_events
    - minio_prefix: ops/artifacts_queue_events
    - partition_cols: [year, month, day, artifact_type]
```

For each configured staging table:
1. Read all rows from `staging.<table>`
2. Write as partitioned Parquet to MinIO at `ops/<table>/year=.../month=.../day=.../<uuid>.parquet`
3. TRUNCATE `staging.<table>`

Steps 2 and 3 are wrapped in a compensating pattern: if the Parquet write fails, do not truncate.

**Schedule:** Configurable per table. Initially hourly. Can be increased as volume grows.

**Credentials:** Read/write on `staging` schema only.

---

## Entities

### Immediate (this plan, V018 migration)

**`ops.artifacts_queue`** — current artifact state
- Already created by V017 in `public`; move to `ops` schema
- Columns: artifact_id, minio_path, artifact_type, listing_id, run_id, fetched_at, status, retry_count, last_error, updated_at

**`staging.artifacts_queue_events`** — artifact state transition buffer
- artifact_id, status, event_at, minio_path, artifact_type, fetched_at, listing_id, run_id

### Follow-on (separate PRs, reference Plan 89 / Plan 93)

| Hot table | Staging table | Description |
|-----------|--------------|-------------|
| `ops.listing_price_current` | `staging.listing_price_events` | Latest price per listing; full price history in MinIO |
| `ops.vin_listing_map` | `staging.vin_listing_events` | Current canonical VIN↔listing_id mapping |
| `ops.srp_observations_current` | `staging.srp_observation_events` | Latest SRP parse per listing |
| `ops.detail_observations_current` | `staging.detail_observation_events` | Latest detail parse per listing |

---

## Impact on Current PR (Plans 93 + 97)

### Remove from `shared/minio.py`
- `write_artifact_event()` — eliminated entirely
- Services never write Parquet directly; that's the export DAG's job
- Raw HTML blob writes (`write_html`, `read_html`, `make_key`) are unaffected — blobs are not events

### Scraper (`scrape_detail.py`, `scrape_results.py`)
- On `artifacts_queue` INSERT: also INSERT into `staging.artifacts_queue_events` with `status='pending'`
- Remove `write_artifact_event` call

### Processing service
- On each status transition: UPDATE `ops.artifacts_queue` + INSERT `staging.artifacts_queue_events`
- Both in same transaction

### V018 migration
- Create `staging` schema
- Create `staging.artifacts_queue_events` table
- Move `artifacts_queue` from `public` to `ops` (or alter search_path)
- Grant permissions per the permission model above

---

## What Does Not Change

- Raw HTML blobs → MinIO immediately. These are artifacts, not events. The blob IS the bronze data.
- `ops.artifacts_queue` → Postgres hot table. Operational queries (what needs processing next?) still hit Postgres.
- Cleanup logic — terminal-state rows are still removed from `ops.artifacts_queue` by the archiver.

---

## Relationship to Other Plans

| Plan | Relationship |
|------|-------------|
| Plan 89 | Established the philosophy; superseded on implementation. This plan provides the missing mechanism. |
| Plan 93 | Processing service — consumes `ops.artifacts_queue`; will INSERT into `staging.artifacts_queue_events` on transitions |
| Plan 96 | Silver layer — dbt reads MinIO Parquet output of the export DAG |
| Plan 97 | Artifacts queue — V017 migration; V018 extends it with `staging` schema and moves table to `ops` |
