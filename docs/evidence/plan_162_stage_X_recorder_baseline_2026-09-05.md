# Plan 162 Stage X — the execution baseline, 2026-09-05

**Taken while DuckDB is still authoritative**, which is the whole reason this
measurement has a date on it. [Plan 125 Gate
D](../plans/plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration)
moves the reader, and a baseline taken after it is not a baseline — it is a
reading of the new world with nothing to compare against.

## What was measured

`tests/plugins/sql_execution_recorder.py` wraps each database client at its
entry point and records `(client, statement text, origin)` for every execution.
`origin` is the `.sql` file the statement was loaded from, carried by
`shared.query_loader.SqlText` and preserved through `.format()`.

**Recipe.** From a checkout with a Flyway-migrated Postgres reachable:

```bash
SQL_EXECUTION_RECORD=/tmp/record.json \
  pytest tests/integration -m integration --ignore=tests/integration/airflow -q
```

The plugin is registered in `pyproject.toml`'s `addopts`, so it loads in every
job; it writes an artifact only when `SQL_EXECUTION_RECORD` names one.

## The reading

| | |
|---|---|
| Executions recorded | **3,020** |
| Attributed to a `.sql` file | **2,271** |
| By client | `psycopg2` 3,019, `duckdb` 1 |
| Clients wrapped in this environment | `psycopg2`, `duckdb`, `asyncpg` |
| Clients declared | `psycopg2`, `duckdb`, `asyncpg`, `pyspark` |
| **Production `.sql` files recorded executing** | **101 of 161** |
| Test `.sql` files recorded executing | 326 |

The 749 unattributed executions are `PREPARE _fixture_probe AS …` and its
`DEALLOCATE`, which `tests/integration/sql/test_fixture_statements.py` issues to
schema-check the corpus. They are real executions and are correctly credited to
no file: the text executed is not any file's text. Filtering them was rejected —
over-filtering hides genuinely unattributed executions, which is the more
dangerous direction.

## What the number is for, and what it is not

**It is not comparable to the Layer 2 census, and the gap is the point.**
`test_every_production_sql_file_is_touched_by_a_layer_2_test` reports all 161
files covered with `LAYER_2_WAIVERS` empty — but it credits a file when a Layer
2 module *names its stem as a whole word*, which this plan has called the
weakest available reading since the day it was written. Recorded execution says
101.

The 60-file difference is mostly the environment rather than the reading:

| Tree | Not recorded here | Why |
|---|---|---|
| `archiver/` | 29 | The lake-snapshot selectors need MinIO, absent locally |
| `dashboard/` | 24 | Need the DuckDB warehouse a dbt build produces |
| `ops/` | 4 | |
| `dbt_runner/` | 2 | Serving snapshots, DuckDB |
| `processing/` | 1 | |

**And that is the argument for the aggregation, made in numbers.** A statement
may execute in any of five CI jobs, so no single job's record can be read as
coverage — 53 of the 60 above are files that execute in a job this run was not.
The per-job artifact and the gate job that combine them are what
[CAR-78](https://linear.app/cartracker/issue/CAR-78) settles the job definitions
for, and they land with or after Stage Q. Capture could not wait for that,
because of the deadline at the top of this file; aggregation can.

When it lands, replacing `_names(stem, text)` with "this file's text executed in
this run" is a one-line change to the weakest rule in the contract.

## What the recorder found on its first run

**Three loaders existed for `airflow/sql/`, and two lost the file.** The
recorder reported nine executions of `deploy_intent_gate.sql`,
`record_gate_observation.sql` and `delete_stale_emails.sql` with no origin —
statements executing against a real Postgres with nothing able to say which file
the text came from.

- `airflow/dags/dag_queries.py` has its own `load_query`, because the DAG tree
  may not import `shared` — [G12](../TESTING.md#the-gap-list)'s decided
  exemption. It returned a plain `str`; it now returns an origin-carrying one.
- `tests/integration/sql/test_airflow_dag_queries.py` had a *third* loader, a
  bare `read_text()`, because it deliberately does not import the DAG module
  (Airflow's starlette pin conflicts with the services'). It may import
  `shared` even though the DAG tree may not, so it now does.

After both repairs, **every statement loaded from a file is attributed to it**:
unattributed-but-file-backed went 9 → 0.

That is the instrument earning its keep before it has been read once in anger,
and it is the argument for keying the recorder on the client rather than on the
fixtures that hand out connections: a fixture-keyed recorder would have reported
these three as absent rather than as unattributed, and absence reads like
nothing happened.

## Limits, recorded rather than discovered later

- **Spark's DataFrame API is invisible.** The recorder records text;
  `df.selectExpr(...)` and the DataFrame API are not text. Already
  [G15](../TESTING.md#the-gap-list) for the static rule, unchanged here.
- **`pyspark` is declared and wrapped but recorded nothing**, because it is not
  installed in this environment and `tests/integration/lakehouse` is
  `DORMANT_SUITES`-declared until Plan 125 Gate C. The artifact reports which
  clients were *wrapped* separately from which executed, so "no Spark here"
  reads differently from "Spark executed nothing".
- **The cross-engine assertion is not attempted.** "This ran on the engine
  production uses for it" needs two live engines to design honestly and belongs
  to Plan 125 Gate D. Building it against one live engine and one hypothetical
  would fit the design to what exists today.
- **dbt is captured from its own `target/run/` artifacts**, not by wrapping: it
  is a subprocess, and it already writes every compiled statement it executed.
  A declared second mechanism, not a hole.
