# Plan 91: UUID Column Type Standardisation

**Status:** Planned
**Priority:** Medium — correctness/hygiene; no operational blocker, but causes cast noise in processing service code

---

## Overview

Several columns that store UUID values are typed as `text` in the application tables. The only correctly-typed column is `raw_artifacts.listing_id uuid`. Everything else that stores a UUID — listing IDs, run IDs in observation tables, n8n execution IDs — needs to be audited and the mismatches corrected with `ALTER COLUMN TYPE`.

The goal is a schema where Postgres enforces UUID format at the DB boundary, joins never require implicit casting, and Python code receives `uuid.UUID` objects rather than raw strings.

---

## Audit Scope

Run the verification queries below against prod before writing any migration. The goal is to confirm every value in a suspected column is a valid UUID (or NULL), and surface any surprises before we commit to `ALTER COLUMN TYPE`.

### `listing_id` columns (expected: all `uuid`)

| Table | Current type | Status |
|---|---|---|
| `raw_artifacts.listing_id` | `uuid` | ✓ correct |
| `srp_observations.listing_id` | `text NOT NULL` | needs fix |
| `detail_observations.listing_id` | `text` | needs fix |
| `detail_carousel_hints.listing_id` | `text NOT NULL` | needs fix |
| `detail_carousel_hints.source_listing_id` | `text` | needs audit — may be UUID |
| `detail_scrape_claims.listing_id` | `text NOT NULL` | needs fix |
| `blocked_cooldown.listing_id` | `text NOT NULL` | needs fix |

Audit query (run per table):
```sql
-- Should return 0 rows if all values are valid UUIDs
SELECT listing_id
FROM <table>
WHERE listing_id IS NOT NULL
  AND listing_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
```

### `run_id` columns (expected: all `uuid`)

| Table | Current type | Status |
|---|---|---|
| `runs.run_id` | `uuid NOT NULL` | ✓ correct |
| `raw_artifacts.run_id` | `uuid NOT NULL` | ✓ correct |
| `processing_runs.run_id` | `uuid NOT NULL` | ✓ correct |
| `scrape_jobs.run_id` | `uuid NOT NULL` | ✓ correct |
| `srp_observations.run_id` | `uuid` | ✓ correct |

`run_id` appears clean. Confirm no regressions by checking the analytics stg tables (`stg_srp_observations.run_id`, `stg_raw_artifacts.run_id`) — those are dbt-managed and will self-correct once source columns are fixed.

### `job_id` columns

| Table | Current type | Status |
|---|---|---|
| `scrape_jobs.job_id` | `uuid DEFAULT gen_random_uuid()` | ✓ correct |

Only one table; no issue.

### `execution_id` columns

| Table | Current type | Status |
|---|---|---|
| `n8n_executions.execution_id` | `text NOT NULL` | needs audit |
| `pipeline_errors.execution_id` | `text` | needs audit |

n8n execution IDs look like UUIDs but confirm with:
```sql
SELECT execution_id
FROM n8n_executions
WHERE execution_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
```

If n8n uses a different ID format (it has varied historically), leave these as `text`.

### `customer_id` / `seller_customer_id`

Leave as `text`. `seller_customer_id` is UUID-formatted (from SRP) but `customer_id` is a numeric string (from detail pages). Mixing formats on a column that's not a true identity key is fine as `text`.

---

## Migration Strategy

Once audit confirms all values are valid UUIDs, a single Flyway migration per affected table:

```sql
-- Example for srp_observations
ALTER TABLE public.srp_observations
    ALTER COLUMN listing_id TYPE uuid USING listing_id::uuid;
```

Each `ALTER COLUMN TYPE` takes an `ACCESS EXCLUSIVE` lock for the duration of the rewrite. On large tables (`srp_observations`, `detail_observations`) this is a brief write-block — run during a low-traffic window or use a lock-timeout guard:

```sql
SET lock_timeout = '5s';
ALTER TABLE public.srp_observations
    ALTER COLUMN listing_id TYPE uuid USING listing_id::uuid;
```

If the lock can't be acquired in 5s, the statement fails cleanly rather than queuing behind a long-running query.

Indexes on affected columns are automatically rebuilt by Postgres as part of `ALTER COLUMN TYPE` — no manual drop/recreate needed.

---

## Application Code Impact

Python code that handles `listing_id` as a plain string will start receiving `uuid.UUID` objects from psycopg2 after this change. Two options:

1. Accept `uuid.UUID` and call `str()` at the boundary (URLs, JSON responses)
2. Register a psycopg2 UUID adapter that returns strings — keeps existing code unchanged

Audit all places that read `listing_id` from the DB before running the migration:
```
grep -rn "listing_id" processing/ ops/ scraper/ --include="*.py"
```

---

## dbt Impact

dbt reads `listing_id` from source tables via `stg_*` models. After the type change, dbt will see `uuid` columns. This is handled automatically by dbt's type inference — no model changes required. The analytics tables (`stg_*`, `int_*`, `mart_*`) are materialized by dbt and will pick up the correct type on the next full build.

---

## Order of Operations

1. Run audit queries against prod — confirm zero non-UUID values in all affected columns
2. Check n8n execution_id format — decide whether to include in migration
3. Write Flyway migration (V015 or later depending on sequencing)
4. Grep Python services for listing_id string handling — fix any cast issues
5. Run migration in staging, verify CI passes
6. Run migration in prod during low-traffic window
7. Trigger a full dbt build to rebuild stg tables with correct types
