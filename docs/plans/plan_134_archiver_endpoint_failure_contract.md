# Plan 134: The Archiver Endpoints Do Not Signal Failure

## Status

**Build order — ready to start.** Split out of [Plan 131](plan_131_packed_cold_storage.md)
Stage 5 decision D5 on 2026-08-14, which fixed the two Plan 131 endpoints and
deliberately left the rest alone.

Priority **88 (high)**. Effort **S + a 7-day observation window**. In the build
order in [`docs/PLANS.md`](../PLANS.md), which is authoritative for its position.

**Its external blocker is clear.** [Plan 141](plan_141_structured_log_ingestion_contract.md)
Stage 4 accepted 2026-08-26, so `{service="archiver", level="ERROR"}` is a
trustworthy selector and a "no failures" reading means no failures rather than
a parsing gap.

**Surveyed against the code on 2026-08-30.** That pass moved four things out of
"not yet surveyed" and corrected three claims this document previously made.
The corrections are in [What the survey changed](#what-the-survey-changed) and
they are the reason Stage 1 is larger than "a few lines per endpoint".

This is correct to fix and should not be fixed casually: every endpoint below
has been failing quietly for as long as it has existed, so the change converts
long-standing quiet into DAG failures and pages.

---

## The defect

Every archiver processor returns a **summary dict** rather than raising.
Partial results are still results, which is the right shape for a job you run
by hand and read. The CLI then translates that summary into an exit code:

```python
# delete_packed_source_html.py — the face that got this right
return 1 if result["error"] or result["objects_refused"] else 0
```

**The HTTP side never got the same translation.** `archiver/app.py` returns
whatever the processor returned, with a 200. So `resp.raise_for_status()` — the
entire check a DAG performs — passes on a run that did nothing, failed, or
never started.

| endpoint | summary shape | the failure it returns 200 for | schedule |
|---|---|---|---|
| `POST /flush/silver/run` | `{"flushed": int, "error": str\|None}` | `error` set: DB down, MinIO down, or the write/delete transaction threw | hourly, via `hourly_analytics_refresh` |
| `POST /flush/staging/run` | `{"total_flushed": int, "tables": [...], "error": str\|None}` | `error` set: any one of the per-table flushes failed | hourly, same DAG |
| `POST /compact/silver/run` | `{"scanned", "compacted", "incremental", "skipped", "failed", "error", "partitions": [...]}` | **`failed > 0` with `error: None`** — see below | daily, 04:10 UTC |
| `POST /cleanup/queue/run` | `{"total", "deleted", "failed", "results", "error"?}` | `error` set, or `failed > 0` | hourly |
| `POST /cleanup/parquet/run` | `{"total", "deleted", "failed", "results"}` | **none — it is a structural no-op**, see below | daily 03:00 |

No DAG in `airflow/dags/` inspects an `error` key.

### The compact predicate is not `error`

`compact_silver` catches per-partition exceptions, increments a `failed`
counter, appends `{"ok": False, "error": str(e)}` to `partitions`, and then
returns the run summary with **`"error": None`**
([`compact_silver.py:325-334`](../../archiver/processors/compact_silver.py)).
Its top-level `error` is set only when MinIO itself is unreachable. A run where
every partition failed is `{"failed": 7, "error": None}` and a 200.

This matters beyond the status code. `_compact_one` writes a `.parquet.tmp`,
verifies the row count, deletes the originals, then renames. A rename failure
raises **after** the originals are gone, leaving an unpublished `.tmp` that the
`*.parquet` glob does not match — the log line says so:
`"rename failed, .tmp preserved for manual recovery"`. That is a partition
whose data is present but invisible to every reader until a human moves it.
`failed > 0` is not a soft signal here; it is the one condition that most needs
a person.

**So the predicate is `error or failed`, and the 500 body must carry the
failing entries from `partitions` — not just the count.**

### `flushed: 0` is normal, for all three

`flush_silver_observations` returns `{"flushed": 0, "error": None}` when there
is nothing staged, which is the ordinary state of a quiet hour. The same trap
exists for `total_flushed` and for `deleted`. **The predicate is never a zero
count.** Each of these was read before being wired, and the reading is the
table above.

## The fix, which already exists in three places

`dbt_runner/app.py:214` raises `HTTPException(status_code=500, detail=result)`
on a failed build, and `sensors.post_json` was built for exactly that —
`JsonPostError` carries the parsed body so a notify task can quote the stderr.

Plan 131 Stage 5 applied that pattern to `/pack/bronze/run`,
`/pack/bronze/prune` and `/pack/bronze/verify` as `_pack_failure_reason` /
`_prune_failure_reason` / `_verify_failure_reason` in `archiver/app.py`.
**Those three functions are the template**: a predicate per job, mirroring that
job's own CLI exit code, unit-tested directly against summary dicts, raising a
500 whose `detail` is the summary plus a `failure_reason`. Their docstrings
also record *why* each carried condition is carried, which is the part worth
copying — a predicate without that reasoning is how a monthly job gets failed
over one unreadable object.

## What the survey changed

Four items left as "not yet surveyed" on 2026-08-14 have now been read.

1. **`/cleanup/parquet/run` needs no predicate, because it cannot do
   anything.** `archiver/sql/get_expired_parquet_months.sql` is a stub —
   `SELECT 0::int AS year, 0::int AS month WHERE FALSE` — carrying the comment
   *"raw_artifacts was dropped in V036. The old HTML→Parquet export pipeline no
   longer exists... Return empty so the cleanup job succeeds as a no-op."*
   `run_cleanup_parquet` therefore returns at its `if not months` early exit on
   every single call. It never reaches `cleanup_parquet()`, so `failed` is
   structurally `0`, and it never reaches `MARK_PARQUET_DELETED` — which is
   itself dead SQL against `raw_artifacts`, dropped by
   `db/migrations/V036__drop_raw_artifact_tables.sql`, and would raise
   `relation "raw_artifacts" does not exist` if it were ever reached.

   Production confirms it: **133 successful runs, still firing daily, most
   recently 2026-08-30 03:00:01.** Every one of them did nothing, having first
   burned a deploy-intent sensor and an archiver health check.

   A failure contract for this endpoint would be a predicate over a code path
   that cannot execute. **The disposition is deletion, not enforcement** — see
   Stage 3.

2. **`/cleanup/queue/run` is the same defect and is observable.**
   `run_cleanup_queue` catches the candidate fetch and returns
   `{"error": str(e)}` with a 200, and `cleanup_queue` logs
   `"cleanup_queue: DELETE failed"` at ERROR per row. Both conditions are in
   Loki today.

3. **`cleanup_artifacts` has never run, not once.** Its file declares
   `schedule="0 * * * *"` and POSTs to `/cleanup/parquet/run` — not
   `/cleanup/queue/run`, despite its name and despite `README.md:65` claiming
   it *"sweeps completed and expired rows from `artifacts_queue`"*. It is moot
   either way: `docker-compose.yml:14` sets
   `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`, nothing in the
   repository ever unpauses it, and **`airflow.dag_run` holds zero rows for
   this dag_id.** It has been paused since it was written.

   So there is no duplicate trigger to resolve, and no hourly exposure. There
   is a dead DAG whose stated purpose is served by `cleanup_queue`.

4. **The failures are quiet, not invisible.** `ct-log-error-spike` in
   `grafana/provisioning/alerting/rules.yml` already alerts on
   `{service=~"archiver|...", level="ERROR"} > 5 in 5m`. A single flush failure
   emits one or two ERROR records and stays under that threshold; a burst
   already pages today. So what this plan converts is the **low-rate** silence.
   The corollary matters more: those ERROR records are already in Loki, which
   is what makes Stage 0 possible.

The earlier claim that "each of these runs hourly" was also wrong. Only
`hourly_analytics_refresh` — which owns both flushes — and `cleanup_queue` are
hourly. A wrong compact predicate pages once a day, not once an hour.

### What is actually scheduled, read from production 2026-08-30

`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`, so a `schedule=` in a DAG
file proves nothing. Pause state and `airflow.dag_run` counts:

| DAG | Paused | Runs | Last run | What it does |
|---|---|---:|---|---|
| `hourly_analytics_refresh` | no | — | — | owns both flushes; the only caller that matters |
| `flush_silver_observations` | **yes** | — | — | manual-only by design, as the file says |
| `flush_staging_events` | **yes** | — | — | manual-only by design |
| `compact_silver` | no | 91 success, **0 failed ever** | 2026-08-30 04:10 | real work |
| `cleanup_queue` | no | 3017 success, 5 failed (all 2026-07-08) | 2026-08-30 22:00 | real work |
| `cleanup_parquet` | no | 133 success, 2 failed (April) | 2026-08-30 03:00 | **nothing — structural no-op** |
| `cleanup_artifacts` | **yes** | **0 — never run** | — | nothing, ever |

**`compact_silver`'s zero failures in 91 runs is the number this plan exists
for.** It is not evidence that compaction never fails; it is what a `failed > 0`
run that returns 200 looks like from Airflow. Stage 0's job is to find out which
of those 91 green runs were green.

The two `cleanup_parquet` failures both predate the V036 stub and say nothing
about the current code path.

## What a 500 actually does to the hourly DAG

This is the blast radius, and it is the reason Stage 1 is not only predicates.

`hourly_analytics_refresh` wires
`ready >> archiver_up >> flush_silver >> flush_staging >> dbt_runner_up >> build >> reconcile_cooldowns`,
with `[ready, flush_silver, flush_staging, build, reconcile_cooldowns] >> notify`
under `trigger_rule="one_failed"`.

- A 500 on `flush_silver` **skips** `flush_staging`, the dbt build and the
  cooldown reconcile for that hour. Today the build runs on stale data instead.
  Skipping is the correct new behavior — Plan 134's premise is that building
  dbt on a failed flush is worse — but it is a behavior change to state, not a
  side effect to discover.
- Both flush tasks carry `retries=1, retry_delay=30s`. A transient DB or MinIO
  blip self-heals and never pages. Only a condition that survives a 30-second
  retry becomes a failure. **The Stage 0 count is therefore an upper bound on
  the pager rate, not the pager rate.**
- `_notify` pulls its detail from `xcom_pull(task_ids="dbt_build")`. On a flush
  failure that XCom is absent, so the Telegram message is
  `"hourly analytics refresh FAILED"` with a run id, a date, and nothing else —
  naming the DAG rather than the flush that broke, and quoting none of the
  `failure_reason` the 500 just carried. **This is the same defect Plan 140
  Stage 4 fixed for the health sensors** (see the comment already in that DAG).
  Fixing it is in Stage 1, before any endpoint is allowed to fail: a pager that
  cannot say which task failed makes the enforcement stages unreadable.

## Stages

### Stage 0 — Measure what already happened (read-only, no deploy)

Loki's `retention_period` is **90d** (`loki/loki.yml:34`), archiver ships as
`{service="archiver", source="application_file"}` with `level` and `logger`
promoted to labels (`promtail/promtail.yml:223-246`), and every failure path in
these processors already calls `logger.error`. **The observation window's
headline number is already recorded.** Measure it before writing a line of
code; the answer decides whether Stage 2 is a one-line flip per endpoint or a
week of fixing real breakage, and it may make the Stage 1 window a confirmation
rather than a discovery.

Run each of these over `[30d]` and `[90d]` in Grafana Explore:

```logql
# 1. Total archiver ERROR volume, by day — the denominator.
sum(count_over_time({service="archiver", level="ERROR"}[1d]))

# 2. Per-job failure counts. These strings are the actual log messages.
sum(count_over_time({service="archiver", level="ERROR"} |~ `flush_silver:`[1d]))
sum(count_over_time({service="archiver", level="ERROR"} |~ `flush_staging:`[1d]))
sum(count_over_time({service="archiver", level="ERROR"} |~ `compact_silver:`[1d]))
sum(count_over_time({service="archiver", level="ERROR"} |~ `cleanup_queue:`[1d]))

# 3. The conditions that would have become a 500, separated by cause.
{service="archiver", level="ERROR"} |~ `(DB|MinIO) connection failed`
{service="archiver", level="ERROR"} |~ `compact_silver: partition failed`
{service="archiver", level="ERROR"} |~ `compact_silver: rename failed`

# 4. The recovery backlog nobody has counted: every .tmp left behind.
{service="archiver"} |~ `\.tmp preserved for manual recovery`
```

Then, outside Loki:

5. **Reconcile against Airflow.** For each ERROR timestamp from (2), find the
   `hourly_analytics_refresh` / `compact_silver` / `cleanup_queue` run covering
   it in the Airflow metadata DB and record its state. Every ERROR whose run is
   `success` is one silently-swallowed failure — that count, not the raw ERROR
   count, is the number this plan exists for. Check the Airflow DB's own
   retention first; if it is shorter than 90 days, the reconciliation window is
   whatever it holds.
6. **Count the orphaned `.tmp` objects directly**, since a rename failure older
   than retention leaves no log:
   `mc find <alias>/bronze/silver_normalized/observations --name "*.parquet.tmp"`.
   A non-zero count is unpublished data and needs its own fix before compaction
   is allowed to fail loudly, or the first enforced failure arrives on top of a
   backlog.
7. **Query the WARNING level separately.** `compact_silver`'s per-file delete
   failures log at WARNING, not ERROR. They abort the rename, so they are the
   upstream cause of any `.tmp` counted in (4):

   ```logql
   {service="archiver", level="WARNING"} |~ `compact_silver: failed to delete path=`
   ```

   `/cleanup/parquet/run` needs no reading at all. Its failure rate is
   structurally zero, not unknown. Record it as such and move on.

Write the readings into this document as *Evidence — Stage 0*, one table:
job × condition × count × the Airflow state that covered it. Stage 1 does not
start until that table exists, because it is the baseline every later stage is
compared against.

**Stage 0 is read-only in production and touches no code.**

### Stage 1 — Warning-only predicates, and the two repairs the survey found

One deploy of `archiver`, plus one of the Airflow DAG.

1. Add `_flush_silver_failure_reason`, `_flush_staging_failure_reason` and
   `_compact_failure_reason` to `archiver/app.py`, each a pure function on the
   summary dict, each with a docstring naming what it deliberately does *not*
   fail on (`flushed == 0`, `skipped`, `incremental`), in the shape of
   `_pack_failure_reason`.
2. Wire each endpoint to `logger.warning("<job>: would fail — %s", reason)` and
   **still return 200.** No `raise` yet. This is the whole point of the stage:
   the predicate runs against production traffic where an oversight costs a log
   line rather than a skipped dbt build.
3. Emit the warning under a distinct, greppable prefix so the window is one
   query and not a text-matching exercise:
   `{service="archiver", level="WARNING"} |~ "would fail"`.
4. **Fix `_notify` in `hourly_analytics_refresh`** to name the failed task and
   quote its `failure_reason`, rather than pulling only `dbt_build`'s XCom.
   `JsonPostError.result` already carries the 500 body; the notify task needs
   to read the failed task instance instead of a fixed `task_ids`.

Then **observe for seven days.** The gate is: the warning rate matches Stage 0's
measured rate, and every warning that fired names a condition the predicate
intended to catch. A warning firing on a condition Stage 0 did not predict
means the predicate is wrong, and the window restarts after it is corrected.

**Safe stopping point.** Stage 1 alone is an improvement — the predicates are
tested and the pager names the right task — and nothing downstream behaves
differently. If the observation shows a failure rate high
enough that enforcement would be a pager storm, the plan stops here and the
underlying breakage becomes its own plan.

### Stage 2 — Enforcement, one endpoint per deploy

Only if Stage 1's window is clean. Flip `logger.warning` + `return` to
`logger.error` + `raise HTTPException(500, detail=dict(result, failure_reason=reason))`,
**one endpoint per deploy, at least 48 hours apart**, so an unexpected pager
storm names its own cause. Order by blast radius, ascending:

1. `/compact/silver/run` — daily, no downstream task depends on it.
2. `/flush/staging/run` — hourly, but the dbt build does not read staging
   events.
3. `/flush/silver/run` — hourly, and its failure now skips the dbt build. Last,
   deliberately.

The DAGs need no change: `compact_silver` calls `raise_for_status()`, the two
flushes go through `post_json`, and both surface a 500. Each deploy is
`bash scripts/redeploy.sh archiver`.

Each deploy's gate is 48 hours with no unexpected DAG failure. A failure that
Stage 0 predicted is not unexpected — it is the plan working.

### Stage 3 — `/cleanup/queue/run`, and deleting the dead parquet path

After Stage 2 has held for a week. Two separate pieces of work that happen to
live next to each other.

**The predicate.** `/cleanup/queue/run` is the only cleanup endpoint doing real
work — 3017 runs — and it has the same defect: a candidate-fetch failure
returns `{"error": str(e)}` with a 200, and per-row delete failures accumulate
in `failed` with a 200. It gets `_cleanup_queue_failure_reason` on
`error or failed`, sized from what Stage 0 read out of its ERROR records
(`cleanup_queue: DELETE failed`, `run_cleanup_queue: failed to fetch
candidates`). Note the five 2026-07-08 DAG failures: those are the ones that
already went red, so understand what made them red before adding a predicate
that would have caught more.

**The deletion.** `/cleanup/parquet/run`, both its DAGs, its two SQL files and
its processor are dead and should go, not gain a failure contract:

- `archiver/sql/get_expired_parquet_months.sql` — a stub returning no rows.
- `archiver/sql/mark_parquet_deleted.sql` — unreachable, and references a table
  V036 dropped.
- `archiver/processors/cleanup_parquet.py` — `cleanup_parquet()` is still
  called by the caller-supplied `POST /cleanup/parquet`, so check that endpoint
  has a real caller before removing the module rather than the run path.
- `airflow/dags/cleanup_parquet.py` — 133 green runs of nothing, still firing.
- `airflow/dags/cleanup_artifacts.py` — never ran once.
- `tests/integration/airflow/test_dag_integrity.py` and
  `tests/archiver/processors/test_cleanup_parquet.py` follow whatever is
  removed.

**This is a deletion of shipped behavior, so it is the user's call, not a
side effect of a failure-contract plan.** If the answer is "delete it", it is
plausibly its own small plan rather than Stage 3 of this one — say so and split
it. What this plan owes is the finding, which is above.

`README.md:65-66` describes both DAGs as doing work they do not do. That is a
truth-pass item and belongs to
[Plan 138](plan_138_public_surface_refresh.md), not here.

## Files

| File | Change |
|---|---|
| `archiver/app.py` | `_failure_reason` per job on `/flush/silver/run`, `/flush/staging/run`, `/compact/silver/run`; warning-only in Stage 1, 500 in Stage 2 |
| `archiver/processors/cleanup_queue.py` | Stage 3 only — predicate over `error or failed` |
| `airflow/dags/hourly_analytics_refresh.py` | `_notify` names the failed task and quotes `failure_reason` instead of reading only `dbt_build`'s XCom |
| `tests/archiver/test_app.py` | Each predicate unit-tested against summary dicts, including the passing cases it must *not* fail on |
| `tests/airflow/` | `_notify` renders a flush failure without a `dbt_build` XCom |

## Tests

Mirroring `test_app.py:330-410`, per predicate:

- The clean run returns `None` — including `flushed: 0`, `total_flushed: 0`,
  and a compact run that is entirely `skipped`.
- The `error` case returns a reason quoting the error.
- **`{"failed": 3, "error": None}` on compact returns a reason** — the case
  this plan's earlier draft would have missed.
- The endpoint test asserts the 500's `detail` carries both the original
  summary and `failure_reason`, and for compact that it carries the failing
  `partitions` entries.

## Out of scope

- **The three Plan 131 endpoints.** Already done — see D5 in
  [`docs/prompts/claude_prompt_plan_131_stage_5.md`](../prompts/claude_prompt_plan_131_stage_5.md).
- **A Prometheus counter for archiver job outcomes.** Archiver has no
  `/metrics` route and no scrape job in `prometheus/prometheus.yml`, unlike
  `ops`, `dbt_runner`, `processing` and `scraper`. A counter would be a better
  long-term signal than a log line, and it is a bigger change than this plan —
  it belongs with [Plan 135](plan_135_storage_observability.md) or
  [Plan 155](plan_155_log_dashboards.md). Loki's 90-day retention is sufficient
  for both windows here.
- **Recovering orphaned `.tmp` partitions.** Stage 0 counts them. If the count
  is non-zero, publishing them is a separate fix that must land before Stage 2
  flips compaction, but it is not this plan's work.
- **Deleting the dead `/cleanup/parquet/run` path.** The finding is this
  plan's; the deletion is a separate decision, described at the end of Stage 3.
- **Correcting `README.md`'s DAG table**, which describes `cleanup_artifacts`
  and `cleanup_parquet` as doing work neither does, and gives the wrong
  schedules for both flushes. Plan 138's truth pass.

## Success criteria

1. A run of any of the four live endpoints that fails its own predicate returns a
   500 whose body carries the summary and a `failure_reason`, and the covering
   DAG run is red.
2. A clean run — including a legitimately empty one — is still a 200, and no
   DAG failed for an empty flush during either window.
3. The Stage 0 table and the Stage 1 warning counts agree. Where they do not,
   the difference is explained in writing before Stage 2 begins.
4. A Telegram page from `hourly_analytics_refresh` names the task that failed
   and quotes its `failure_reason`.
5. `compact_silver`'s run history distinguishes a genuinely clean run from a
   `failed > 0` run recorded as success — which its first 91 runs did not.

## Intersections

### Plan 141 — structured log ingestion

The dependency this plan waited on. Both observation windows are LogQL over
`{service="archiver"}` with a trustworthy `level` label, which is exactly what
Plan 141 Stage 1 delivered. Cleared 2026-08-26.

### Plan 140 — service health contract

Stage 1's `_notify` repair is the same defect Plan 140 Stage 4 fixed for the
health sensors: a Telegram message that names the DAG rather than the component
that broke. Follow the fix already recorded in that DAG's comment.

### Plan 135 / Plan 155 — storage observability and log dashboards

The natural home for an archiver `/metrics` route and for a dashboard panel on
these failure counts. This plan produces the log contract they would consume
and deliberately stops there.
