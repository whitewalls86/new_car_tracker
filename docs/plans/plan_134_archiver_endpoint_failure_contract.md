# Plan 134: The Archiver Endpoints Do Not Signal Failure

## What this plan is for

The archiver's HTTP endpoints return a summary object instead of raising, so a
partially failed job still answers success and pages nobody. Converts each
endpoint to a real failure signal, one endpoint per deploy, in ascending order
of impact.

## The case

Split out of [Plan 131](plan_131_packed_cold_storage.md) Stage 5 decision D5 on
2026-08-14, which fixed the two Plan 131 endpoints and deliberately left the
rest alone.

**Its external blocker cleared before the work began.**
[Plan 141](plan_141_structured_log_ingestion_contract.md) Stage 4 was accepted
2026-08-26, so `{service="archiver", level="ERROR"}` is a trustworthy selector
and a "no failures" reading means no failures rather than a parsing gap.

**Surveyed against the code on 2026-08-30.** That pass moved four things out of
"not yet surveyed" and corrected three claims this document previously made.
The corrections are in [What the survey changed](#what-the-survey-changed) and
they are the reason Stage B was larger than "a few lines per endpoint".

This is correct to fix and should not be fixed casually: every endpoint below
has been failing quietly for as long as it has existed, so the change converts
long-standing quiet into DAG failures and pages.

### The defect

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
| `POST /cleanup/parquet/run` | `{"total", "deleted", "failed", "results"}` | **none — it was a structural no-op**, see below | ~~daily 03:00~~ **deleted 2026-08-30 in `056cde7`** |

No DAG calling these endpoints inspects an `error` key. (`pack_bronze_html` does
— `check_pack_result` and its siblings at
[`pack_bronze_html.py:43`](../../airflow/dags/pack_bronze_html.py), `:54` and
`:102` — but those are the Plan 131 endpoints, which already raise. An earlier
draft of this line claimed no DAG anywhere did, which was never true.)

#### The compact predicate is not `error`

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

#### `flushed: 0` is normal, for all three

`flush_silver_observations` returns `{"flushed": 0, "error": None}` when there
is nothing staged, which is the ordinary state of a quiet hour. The same trap
exists for `total_flushed` and for `deleted`. **The predicate is never a zero
count.** Each of these was read before being wired, and the reading is the
table above.

### The fix, which already exists in three places

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

### What the survey changed

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

   Production confirmed it: **133 successful runs, firing daily up to
   2026-08-30 03:00:01.** Every one of them did nothing, having first burned a
   deploy-intent sensor and an archiver health check. That was its last run —
   the chain was deleted later the same day.

   A failure contract for this endpoint would be a predicate over a code path
   that cannot execute. **The disposition is deletion, not enforcement** — see
   Stage D.

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
   is what makes Stage A possible.

The earlier claim that "each of these runs hourly" was also wrong. Only
`hourly_analytics_refresh` — which owns both flushes — and `cleanup_queue` are
hourly. A wrong compact predicate pages once a day, not once an hour.

#### What is actually scheduled, read from production 2026-08-30

`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`, so a `schedule=` in a DAG
file proves nothing. Pause state and `airflow.dag_run` counts:

| DAG | Paused | Runs | Last run | What it does |
|---|---|---:|---|---|
| `hourly_analytics_refresh` | no | — | — | owns both flushes; the only caller that matters |
| `flush_silver_observations` | **yes** | — | — | manual-only by design, as the file says |
| `flush_staging_events` | **yes** | — | — | manual-only by design |
| `compact_silver` | no | 91 success, **0 failed ever** | 2026-08-30 04:10 | real work |
| `cleanup_queue` | no | 3017 success, 5 failed (all 2026-07-08) | 2026-08-30 22:00 | real work |
| `cleanup_parquet` | no | 133 success, 2 failed (April) | 2026-08-30 03:00 | **nothing — structural no-op.** Deleted 2026-08-30 |
| `cleanup_artifacts` | **yes** | **0 — never run** | — | nothing, ever. Deleted 2026-08-30 |

The last two rows are what the survey found that morning; both DAGs were
deleted the same day in `056cde7` and no longer exist. Every other row is
current.

**`compact_silver`'s zero failures in 91 runs is the number this plan exists
for.** It is not evidence that compaction never fails; it is what a `failed > 0`
run that returns 200 looks like from Airflow. Stage A's job is to find out which
of those 91 green runs were green.

The two `cleanup_parquet` failures both predate the V036 stub and say nothing
about the current code path.

### What a 500 actually does to the hourly DAG

This is the blast radius, and it is the reason Stage B is not only predicates.

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
  retry becomes a failure. **The Stage A count is therefore an upper bound on
  the pager rate, not the pager rate.**
- `_notify` pulls its detail from `xcom_pull(task_ids="dbt_build")`. On a flush
  failure that XCom is absent, so the Telegram message is
  `"hourly analytics refresh FAILED"` with a run id, a date, and nothing else —
  naming the DAG rather than the flush that broke, and quoting none of the
  `failure_reason` the 500 just carried. **This is the same defect Plan 140
  Stage 4 fixed for the health sensors** (see the comment already in that DAG).
  Fixing it is in Stage B, before any endpoint is allowed to fail: a pager that
  cannot say which task failed makes the enforcement stages unreadable.

## Design

Each endpoint gains a pure `_failure_reason` predicate over its own summary
dict, in the shape `_pack_failure_reason` already established. The predicate is
introduced warning-only so it runs against production traffic before it can
break anything, and is flipped to a 500 one endpoint per deploy in ascending
order of blast radius. The DAGs need no change: a 500 already surfaces through
`raise_for_status()` and `post_json`.

### Files

| File | Change |
|---|---|
| `archiver/app.py` | `_failure_reason` per job on `/flush/silver/run`, `/flush/staging/run`, `/compact/silver/run`; warning-only in Stage B, 500 in Stage C |
| `archiver/processors/cleanup_queue.py` | Stage D only — predicate over `error or failed` |
| `airflow/dags/notifications.py` | **New.** The shared Telegram notifier: names the failed task, quotes its `failure_reason`, logs a rejected send, caps the body. Replaces three broken copies |
| `airflow/dags/hourly_analytics_refresh.py` | `_notify` delegates to it; the flush/reconcile callables gain a `_post_result` wrapper so a failure leaves an XCom to quote |
| `airflow/dags/dbt_build.py`, `airflow/dags/pack_bronze_html.py` | Same `_notify` defect, same fix — see [It was never only this DAG](#it-was-never-only-this-dag) |
| `tests/archiver/test_app.py` | Each predicate unit-tested against summary dicts, including the passing cases it must *not* fail on |
| `tests/airflow/test_notifications.py` | The notifier renders a flush failure with no `dbt_build` XCom, and never touches `ti.dag_run`/`ti.execution_date` — asserted with a spec'd mock, so a reintroduction fails the build. Fast suite: it needs no Airflow |
| `tests/integration/airflow/test_hourly_analytics_refresh.py` | This DAG's headline and work-task set, and the `_post_result` XCom push on both the success and `JsonPostError` paths |

### Tests

Mirroring `test_app.py:330-410`, per predicate:

- The clean run returns `None` — including `flushed: 0`, `total_flushed: 0`,
  and a compact run that is entirely `skipped`.
- The `error` case returns a reason quoting the error.
- **`{"failed": 3, "error": None}` on compact returns a reason** — the case
  this plan's earlier draft would have missed.
- **Stage B's endpoint tests assert the opposite of a 500**: a failing run
  returns 200, carries the summary unchanged, and logs a `would fail` warning
  naming the condition. That is the stage, and those assertions are what has to
  change — deliberately, one endpoint at a time — when Stage C flips each one.
- **Stage C's endpoint test** asserts the 500's `detail` carries both the
  original summary and `failure_reason`, and for compact that it carries the
  failing `partitions` entries.

### Out of scope

- **The three Plan 131 endpoints.** Already done — see D5 in
  [`docs/prompts/claude_prompt_plan_131_stage_5.md`](../prompts/claude_prompt_plan_131_stage_5.md).
- **A Prometheus counter for archiver job outcomes.** Archiver has no
  `/metrics` route and no scrape job in `prometheus/prometheus.yml`, unlike
  `ops`, `dbt_runner`, `processing` and `scraper`. A counter would be a better
  long-term signal than a log line, and it is a bigger change than this plan —
  it belongs with [Plan 135](plan_135_storage_observability.md) or
  [Plan 155](plan_155_log_dashboards.md). Loki's 90-day retention is sufficient
  for both windows here.
- **Recovering orphaned `.tmp` partitions.** Stage A counted them and found
  **zero**, so nothing is owed here and Stage C is not gated on it. Had the
  count been non-zero, publishing them would have been a separate fix landing
  before Stage C flipped compaction.
- **Deleting the dead `/cleanup/parquet/run` path.** The finding was this
  plan's and the deletion was a separate decision — taken, and **done
  2026-08-30 in `056cde7`**, ahead of Stage D's gate. Recorded at the end of
  Stage D; listed here because the disposition, not the work, is what sat
  outside this plan.
- **Correcting `README.md`'s DAG table.** The `cleanup_artifacts` and
  `cleanup_parquet` rows are already gone, removed by `056cde7` with the DAGs
  themselves. What remains is the schedules it gives for both flushes —
  `README.md:61-62` still says every 15 and every 5 minutes, when both DAGs are
  paused manual-only and `hourly_analytics_refresh` owns the work hourly. Plan
  138's truth pass.

## Stages

This plan was sequenced before [`docs/PLAN_DOCUMENT.md`](../PLAN_DOCUMENT.md)
landed, and adopted stage letters on 2026-09-07 when its remaining work was
chunked into issues. The old-to-new mapping is recorded because the contract
requires it, and because closed Linear issues still carry the legacy numbers --
CAR-31 is titled "Plan 134 Stage 1", which is Stage B:

| Legacy | Stage | | Legacy | Stage |
|:---:|:---:|---|:---:|:---:|
| 0 | **A** | | 2 | **C** |
| 1 | **B** | | 3 | **D** |

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--measure-what-already-happened-read-only-no-deploy) | Measure what already happened | `done` | -- |
| 2 | [**B**](#stage-b--warning-only-predicates-and-the-two-repairs-the-survey-found) | Warning-only predicates, the shared notifier, and the seven-day window | `done` | CAR-31 |
| 3 | [**C**](#stage-c--enforcement-one-endpoint-per-deploy) | Enforcement, one endpoint per deploy | `next` | -- |
| 4 | [**D**](#stage-d--cleanupqueuerun) | `/cleanup/queue/run`'s predicate | `--` | -- |

### Stage A — Measure what already happened (read-only, no deploy)

Loki's `retention_period` is **90d** (`loki/loki.yml:34`), archiver ships as
`{service="archiver", source="application_file"}` with `level` and `logger`
promoted to labels (`promtail/promtail.yml:223-246`), and every failure path in
these processors already calls `logger.error`. **The observation window's
headline number is already recorded.** Measure it before writing a line of
code; the answer decides whether Stage C is a one-line flip per endpoint or a
week of fixing real breakage, and it may make the Stage B window a confirmation
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
job × condition × count × the Airflow state that covered it. Stage B does not
start until that table exists, because it is the baseline every later stage is
compared against.

**Stage A is read-only in production and touches no code.**

**Exit:** the readings above are written into this document as a table of job x
condition x count x covering Airflow state, and the orphaned `.tmp` count is
known. Met 2026-08-30.

### Evidence — Stage 0, 2026-08-30

**The answer is not "how often does this fail" but "twice in the last three
weeks, and Airflow reported 128 consecutive successes across both."**

#### The surface being turned on

After the dead `/cleanup/parquet` path was removed, four endpoints across three
scheduled DAGs are in scope. `flush_silver_observations` and
`flush_staging_events` still exist as manual-only DAGs but are paused; they ran
20,544 and 6,851 times before `hourly_analytics_refresh` took ownership on
2026-07-02 and are not part of the enforcement surface.

| DAG | Schedule | Runs | DAG failures | Endpoint tasks |
|---|---|---:|---:|---|
| `hourly_analytics_refresh` | `0 * * * *` | 1,426 | 12 | `flush_silver_observations`, `flush_staging_events` |
| `cleanup_queue` | `0 * * * *` | 3,022 | 5 | `cleanup_queue` |
| `compact_silver` | `10 4 * * *` | 91 | 0 | `compact_silver` |

At **task** level the picture is emptier still. `flush_staging_events` has
**never failed** — 1,419 successes, zero failures. `compact_silver` has never
failed — 91 successes, zero failures. `flush_silver_observations` failed 4
times, all on 2026-07-08, and those are HTTP-level failures (`post_json` raising
on an unreachable archiver), not summary-level ones.

So from Airflow's side, the enforcement surface has essentially never gone red.
That is the finding, not a reassurance.

#### Incident 1 — MinIO storage full, 2026-08-08 → 2026-08-13

`{service="archiver", level="ERROR"}` holds 21 records across
**2026-08-08 13:00:14 → 2026-08-13 02:00:17**, all
`[Errno 5] ... (XMinioStorageFull) when calling the PutObject operation`:

| Job | Records | What it means |
|---|---:|---|
| `flush_silver: failed:` | 4 | observations did not land in the silver layer |
| `flush_staging: failed for` | 17 | four staging tables across `artifacts_queue_events`, `detail_scrape_claim_events`, `price_observation_events`, `vin_to_listing_events` |

Over that window `hourly_analytics_refresh` ran **112 times. Every run is
`success`, and every `flush_silver_observations` task instance is `success`.**

This is the exact consequence the plan's opening table names: a failed silver
flush returns 200, the DAG proceeds, and `dbt_build` builds on stale data. It
did so for five days.

21 records is fewer than five days of hourly failures would produce, so the
`RotatingFileHandler` (5 MB × 3) almost certainly discarded some. **The counts
below are floors, not totals.**

#### Incident 2 — code deployed ahead of its migration, 2026-08-26 → 2026-08-27

32 ERROR records across **2026-08-26 23:00:18 → 2026-08-27 14:00:18**, exactly
16 hourly cycles × 2 tables, all
`flush_staging: failed for staging.coordination_state_events` /
`staging.coordination_release_evidence` — `relation ... does not exist`.

The flyway history closes the timeline without ambiguity:

| When | What |
|---|---|
| 2026-08-25 19:07:05 | `V043 coordination state` applied |
| ≤ 2026-08-26 23:00 | archiver image carrying both tables in `_TABLE_CONFIGS` goes live |
| 2026-08-26 23:00 → 08-27 14:00 | 16 hourly flushes fail on two tables, return 200 |
| **2026-08-27 14:45:22/23** | **`V044` and `V045` applied** |
| 2026-08-27 15:00 | first clean run |

All 16 runs are `success` at both DAG and task level. An expand/contract
ordering inversion — code shipped ahead of schema — ran for sixteen hours and
produced no signal anywhere except a log line nobody was reading.

#### What enforcement would have done

Had Stage C been live, `hourly_analytics_refresh` would have failed **16
consecutive times** on 2026-08-27 and **up to 112 times** across 2026-08-08–13.
Both would have been correct. Both are also, precisely, the pager storm this
plan warns about — which is the argument for Stage B's warning-only window and
for flipping `/flush/silver/run` last, not for softening the predicate.

Note that the `retries=1, retry_delay=30s` on both flush tasks does **not**
help here: neither a full disk nor a missing table heals in thirty seconds.
Retry only absorbs transient blips, so for these two incidents the failure
count and the page count are the same number.

#### The pager has never worked

`hourly_analytics_refresh`'s `notify` task has **12 failures, 1,414 skips, and
zero successes.** Every time the DAG failed and notify fired, notify itself
failed — most recently 2026-07-21, across 2026-07-02 to 2026-07-21.

So Stage B's `_notify` repair is not a polish item. The notification path has
never once delivered, and enforcement without fixing it converts silent
endpoint failures into silent DAG failures.

**Why it failed, found during Stage B implementation.** `_notify` read
`ti.dag_run.run_id` and `ti.execution_date`. On Airflow 3 — this deployment
runs 3.2.0 — the task SDK's `RuntimeTaskInstance` has **neither attribute**,
confirmed against the running scheduler: its model fields are `dag_id`,
`run_id`, `task_id`, `try_number`, `state` and friends, with no `dag_run` and
no `execution_date`. So the task raised `AttributeError` on its second line,
before it built any message, on every one of the 12 firings.

This corrects the defect this document names below. It is **not** only the
Plan 140 Stage 4 defect of a message naming the wrong component — that would
have delivered a page that was merely unhelpful. Nothing was ever delivered.
Both are real and Stage B fixes both, but the ordering matters: had the repair
been scoped to wording alone, the pager would still have sent nothing and the
Stage C gate would have been unreadable in exactly the way this stage exists to
prevent.

#### It was never only this DAG

The two lines came from **`c92bd97`, "adding detail to airflow failure
notification", authored 2026-05-08 13:10 UTC** against `dbt_build`. That DAG's
notify task last succeeded at **12:03 UTC the same day** — an hour before the
commit — and has not succeeded since. The block was then copied into the other
two notify tasks. Every Telegram pager in this repo has been dead since that
commit:

| DAG | notify success | notify failed | Last success | Now |
|---|---:|---:|---|---|
| `dbt_build` | 27 | **268** | 2026-05-08 12:03 UTC | paused, still triggerable |
| `hourly_analytics_refresh` | **0** | 12 | never | active |
| `pack_bronze_html` | 0 | 0 | never fired (5 skips) | active, **latent** |

`pack_bronze_html` is the one worth noting: its notify task has never fired at
all, so the copy there was a page that would have failed the first time the
monthly lifecycle broke.

Fixed together, in one `airflow/dags/notifications.py`, because three copies is
how one commit silenced the fleet. The module adds the two properties none of
the copies had: a non-200 from Telegram is logged rather than passing as
delivered, and the body is capped under Telegram's 4096-character limit, which
quoting several tasks can clear. Its tests run in the fast suite — it imports
only `logging`, `os` and `requests` — so the thing that is supposed to speak up
when everything else breaks no longer needs an Airflow install to be covered.

Strictly this is wider than Plan 134's DAG. It is in scope because Stage C
cannot be gated on a pager that does not work, and leaving two known-dead
copies next to the repaired one would have re-seeded the defect.

#### compact_silver is genuinely clean

Its INFO summary line carries the count directly. Over 30 days, **30 of 30**
`compact_silver: run complete` lines report `failed=0`, and a scan of
`bronze/silver_normalized/observations` finds **zero `*.parquet.tmp` objects**.

So the 91 green runs are green, there is no unpublished-partition backlog, and
`/compact/silver/run` is safe to enforce first as Stage C orders it.

#### Correction to Stage A's own method

**The 90-day retrospective is not available at full label fidelity.** Loki's
retention is 90 days, but Plan 141 Stage 1's labels only exist from
2026-08-25. Before that, archiver records carry `service` but **no `source`
label**, and 24,864 records in the 30-day window carry neither `source` nor
`level`. Queries pinned to `source="application_file"` silently see only the
last five days.

Incident 1 was found only by dropping the `source` matcher. Any query written
for this plan must either pin `source="application_file"` and state that it
covers 2026-08-25 onward, or omit it and accept a mixed stream. Plan 141's own
intersection note anticipated exactly this and it turned out to be load-bearing.

The pre-2026-08-25 records are not a live contract violation — every record in
the last five days carries both labels.

#### What Stage B owes, revised

The window is no longer discovery. Two failure modes are already characterised,
and the predicate for each is confirmed correct against a real incident:
`flush_staging`'s roll-up `error` catches Incident 2, and `flush_silver`'s
`error` catches Incident 1. Stage B's warning-only window is now a *regression
check* — seven days confirming the predicates fire on nothing else — plus the
`_notify` repair, which the evidence promotes from cleanup to prerequisite.

### Stage B — Warning-only predicates, and the two repairs the survey found

**Shipped 2026-08-30 in [PR #295](https://github.com/whitewalls86/new_car_tracker/pull/295)**
— `585c56f` the predicates, `0306629` the pager, `a05168b` this document.
Deployed the same evening with `redeploy.sh archiver pack-worker`; both services
share the `cartracker-archiver` image, and the DAG changes rode the
`./airflow/dags` bind mount with no image rebuild. **The observation window runs
to 2026-09-06.**

Two things landed differently from the specification below, both recorded where
they were found:

- **Item 4 grew.** It was scoped as an edit to this DAG's `_notify`. The defect
  turned out to be an `AttributeError` affecting all three notify tasks in the
  repository, so the fix is a shared `airflow/dags/notifications.py` — see
  [It was never only this DAG](#it-was-never-only-this-dag).
- **The tests split across two suites.** The notifier's own behaviour is in
  `tests/airflow/test_notifications.py` in the fast suite, because
  `notifications.py` imports only `logging`, `os` and `requests`. Only what
  needs a real Airflow install stayed in `tests/integration/airflow/`.

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

   It also has to **stop raising `AttributeError` before it sends anything** —
   see [the pager has never worked](#the-pager-has-never-worked) for why that
   is the larger half, and why the fix lands as a shared
   `airflow/dags/notifications.py` covering all three notify tasks rather than
   as an edit to this one. `ti.get_task_states(dag_id=..., run_ids=[...])` is
   the Airflow 3 way to read sibling task states, and `context["dag_run"]` /
   `context["logical_date"]` replace the two attributes that do not exist. The
   state lookup is a round trip to the execution API, so it is guarded: a
   lookup failure must not cost the page, or the repair reintroduces the bug it
   is fixing. The three flush/reconcile callables also need a `_post_result`
   wrapper — mirroring `pack_bronze_html`'s — since a task that raises
   `JsonPostError` currently leaves no XCom for notify to quote at all.

Then **observe for seven days.** The gate is: the warning rate matches Stage A's
measured rate, and every warning that fired names a condition the predicate
intended to catch. A warning firing on a condition Stage A did not predict
means the predicate is wrong, and the window restarts after it is corrected.

**Exit:** seven consecutive days in which the warning rate matches Stage A's
measured rate and every warning names a condition the predicate intended to
catch. Met 2026-09-07 -- see the Record.

**Safe stopping point.** Stage B alone is an improvement — the predicates are
tested and the pager names the right task — and nothing downstream behaves
differently. If the observation shows a failure rate high
enough that enforcement would be a pager storm, the plan stops here and the
underlying breakage becomes its own plan.

### Stage C — Enforcement, one endpoint per deploy

Only if Stage B's window is clean. Flip `logger.warning` + `return` to
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
Stage A predicted is not unexpected — it is the plan working.

**The first deploy is also the notifier's first live test.** Stage B's window
skipped `notify` 184 times, so the repaired pager is verified by tests alone and
this stage's gate assumes it delivers. `/compact/silver/run` is first partly
because it is daily with nothing downstream, and partly because that makes it
the cheapest place for the pager to fail if it is still wrong. Do not read a
silent 48 hours on the first deploy as proof the pager works — it is proof
nothing failed. The notifier is proven by a real page, or by deliberately
failing one compaction run, whichever comes first.

**Exit:** all three endpoints return a 500 carrying `failure_reason` on their
own predicate, each having held 48 hours with no unexpected DAG failure, and a
production page from `hourly_analytics_refresh` has named a failed task and
quoted its reason at least once.

### Stage D — `/cleanup/queue/run`

After Stage C has held for a week. This began as two pieces of work that
happened to live next to each other; the second is already done, out of stage
order, and only the predicate is still owed.

**The predicate.** `/cleanup/queue/run` is the only cleanup endpoint doing real
work — 3017 runs — and it has the same defect: a candidate-fetch failure
returns `{"error": str(e)}` with a 200, and per-row delete failures accumulate
in `failed` with a 200. It gets `_cleanup_queue_failure_reason` on
`error or failed`, sized from what Stage A read out of its ERROR records
(`cleanup_queue: DELETE failed`, `run_cleanup_queue: failed to fetch
candidates`). Note the five 2026-07-08 DAG failures: those are the ones that
already went red, so understand what made them red before adding a predicate
that would have caught more.

**The deletion — done 2026-08-30, in commit `056cde7`.** The survey found the
`/cleanup/parquet` chain dead rather than merely unsignalled, so it was deleted
rather than given a failure contract. The call was the user's, made on the
finding: Plan 145 had already finished the legacy Parquet disposition
`cleanup_parquet` was residue of, and `cleanup_artifacts` had never run. That
is why it landed ahead of Stage C rather than waiting for this stage's gate.

What went, together, because deleting the DAGs alone would have left a
callerless endpoint behind:

- `archiver/sql/get_expired_parquet_months.sql` — a stub returning no rows
  since V036, and `archiver/sql/mark_parquet_deleted.sql`, unreachable and
  referencing a table V036 dropped.
- `archiver/processors/cleanup_parquet.py`, and **both** routes. The caller
  check this stage asked for came back empty: the caller-supplied
  `POST /cleanup/parquet` had no production caller either, so the module went
  with the run path rather than being kept alive behind it.
- `airflow/dags/cleanup_parquet.py` — 133 green runs of nothing — and
  `airflow/dags/cleanup_artifacts.py`, which never ran once.
- `tests/archiver/processors/test_cleanup_parquet.py` and the covering cases in
  `test_dag_integrity.py` and `test_app.py`.

Three registries the finding had not named also knew about this chain and now
do not: `ops/mutation_contract.py`, `coordination_contract.py`'s admission
surfaces and drain evidence, and `archiver/queries.py`. 402 deletions, and the
full non-integration suite stayed green at 3097 passed.

So nothing is owed here any more. `/cleanup/parquet/run` is out of this plan's
scope because there is no failure left to contract.

**Exit:** `/cleanup/queue/run` returns a 500 carrying `failure_reason` when its
candidate fetch fails or any row delete fails, and has held 48 hours with no
unexpected `cleanup_queue` DAG failure. The five 2026-07-08 failures are
understood and stated before the predicate lands, not after.

`README.md` described both deleted DAGs as doing work neither did, and
`056cde7` removed those two rows. What remains for
[Plan 138](plan_138_public_surface_refresh.md)'s truth pass is the wrong
schedules that table still gives for both flushes.

## Success criteria

1. A run of any of the four live endpoints that fails its own predicate returns a
   500 whose body carries the summary and a `failure_reason`, and the covering
   DAG run is red.
2. A clean run — including a legitimately empty one — is still a 200, and no
   DAG failed for an empty flush during either window.
3. The Stage A table and the Stage B warning counts agree. Where they do not,
   the difference is explained in writing before Stage C begins.
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

Stage B's `_notify` repair shares Plan 140 Stage 4's defect — a Telegram message
naming the DAG rather than the component that broke — and the shared notifier
now fixes it for all three notify tasks by naming the failed task.

It was **not** the whole defect here, and the difference is worth carrying: Plan
140's sensors delivered an unhelpful page, whereas these three delivered nothing
at all. A repair scoped to Plan 140's shape would have left the pager silent and
read as done. See [It was never only this DAG](#it-was-never-only-this-dag).

### Plan 135 / Plan 155 — storage observability and log dashboards

The natural home for an archiver `/metrics` route and for a dashboard panel on
these failure counts. This plan produces the log contract they would consume
and deliberately stops there.

---

## Record

### Stage B — Warning-only predicates, the `_notify` repair, and the seven-day window

Stage A's evidence predates this section and remains inline above at
[Evidence — Stage 0](#evidence--stage-0-2026-08-30).

**Landed** 2026-08-30 in [PR #295](https://github.com/whitewalls86/new_car_tracker/pull/295)
— `585c56f` the predicates, `0306629` the shared notifier, `a05168b` the root cause.

**The window opened 2026-08-31 02:03:49 UTC** and closed 2026-09-07 02:04 UTC.
The start was not recorded at the time; it is the VM's `git pull` of `610f77e`
(the PR #295 merge), which carries `585c56f` and which the prior `de18913` did
not. Linear moved the issue to Soaking at 02:04:10Z, 21 seconds later.

**The window is clean.** `{service="archiver", level="WARNING"} |~ "would fail"`
returns **zero entries**. Archiver holds one Loki series across the window,
`level=INFO`, so it emitted no WARNING and no ERROR record at all.

**The zero is a real zero, not a selector gap.** The same matcher returns
archiver records on 2026-07-08 (4), 08-09 (15), 08-14 (6), 08-27 (3) and 08-28
(30) — the last two days before the window opened — and archiver logged
continuously throughout, 3,228 records at ~320/day, while other services
emitted thousands of WARNING and ERROR records over the same span.

**The predicates ran.** 184 `hourly_analytics_refresh` runs, every one
successful, so `_flush_silver_failure_reason` and `_flush_staging_failure_reason`
each evaluated 184 times; 8 `compact_silver` runs, each logging
`run complete — failed=0`. **376 evaluations, 0 warnings.** No archiver summary
in the window carried a non-zero `errors=` or `failed=`, and Airflow recorded no
failed or upstream_failed task instance in any DAG.

**Against Stage A.** Stage A measured two incidents in three weeks, so the
expected count over seven days was under one. Zero is consistent with it, and no
warning fired on a condition Stage A did not predict — the gate is met.
Success criterion 3 holds: the two tables agree.

**What this window did not prove.** `notify` was **skipped 184 times**; its
`one_failed` trigger never fired because nothing failed. The repaired notifier
is therefore verified by `tests/airflow/test_notifications.py` and not by a
production page. Stage C's gate assumes the pager delivers, so the first
enforced failure is also the notifier's first live test — deliberately
`/compact/silver/run`, which is daily and has nothing downstream.

**Verified by** `pytest tests/archiver/test_app.py tests/airflow/test_notifications.py`
— 122 passed. Loki and Airflow read directly from production 2026-09-07.

**Public surfaces:** no mechanism, name or quantity either surface states was
changed by this work. (`README.md:61-62`'s stale flush schedules are
pre-existing and already assigned to Plan 138's truth pass.)

**Cost:** estimate 2 → actual 1 (−1).

### Stage C — Enforcement, one endpoint per deploy

**Deploy 1 of 3 — `/compact/silver/run`.** Landed 2026-09-07 in
[PR #380](https://github.com/whitewalls86/new_car_tracker/pull/380) — `a608595`,
merged 19:24:28 UTC. The endpoint logs at ERROR and raises
`HTTPException(500, detail=dict(result, failure_reason=reason))` on
`_compact_failure_reason` rather than warning and returning 200.

**The 48-hour gate ran 2026-09-07 19:25 UTC → 2026-09-09 19:25 UTC and is
clean.** The 19:25 start is the live time `docs/PLANS.md` recorded, one minute
after the merge; the VM's actual pull was not separately recorded, the same gap
Stage B had to reconstruct after the fact. It is not material here — the window
holds the same two `compact_silver` runs under any deploy time between the merge
and 04:10 the following morning.

**Both scheduled runs in the window succeeded.** `compact_silver` is
`10 4 * * *`, so the window holds exactly two runs:

| Run | Start | End | Duration |
|---|---|---|---|
| `scheduled__2026-09-08T04:10:00+00:00` | 04:10:00.640Z | 04:10:08.926Z | 8.3 s |
| `scheduled__2026-09-09T04:10:00+00:00` | 04:10:00.780Z | 04:10:09.971Z | 9.2 s |

The first run after the gate closed, `scheduled__2026-09-10T04:10:00+00:00`,
also succeeded (7.7 s).

**The green runs are the primary evidence, not the log query.** `compact_silver`
calls `raise_for_status()` on the endpoint, so a 500 turns the task red. Two
green runs mean the endpoint answered 200 twice, so `_compact_failure_reason`
returned `None` twice and no ERROR record was emitted. The Loki reads
corroborate that; they do not carry it.

**Archiver emitted 668 records across the window and every one was INFO.**
`sum by (level)` returns a single series, `{level="INFO"} = 668` — 334/day,
continuous, no ingestion gap. So the two zeros below are structural rather than
empty: there is no ERROR stream and no WARNING stream on archiver in this window
to have missed. Both queries confirm it directly, each returning an empty vector
with zero chunks referenced:

- `{service="archiver", level="ERROR"} |~ "compact_silver: run failed"` — **0**.
  The enforced path never fired.
- `{service="archiver", level="WARNING"} |~ "would fail"` — **0**. The
  observation window deploys 2 and 3 are still read out of is uncontaminated,
  which is what deploy 1's tests were written to guarantee when they moved the
  compact case out of the warning-only class.

**This deploy could not have paged, and that is a gap in the stage's reasoning
rather than a result.** `compact_silver`'s DAG is `ready >> archiver_up >>
compact` — no `notify` task, no failure callback. Only `hourly_analytics_refresh`,
`dbt_build` and `pack_bronze_html` import the shared notifier. A failed
compaction here goes red in Airflow and is silent on Telegram. Stage C's text
says the notifier is "proven by a real page, or by deliberately failing one
compaction run, whichever comes first" — the second clause cannot hold. The
notifier's live proof can only come from deploy 2 or deploy 3, both of which run
under `hourly_analytics_refresh`. The stage exit and success criterion 4 already
name that DAG, so the exit is unaffected; the ordering rationale was wrong.

**What this window did not prove.** Two clean daily runs are proof nothing
failed, not proof the enforcement works — no run met the predicate, so the 500
path is still verified by `tests/archiver/test_app.py` alone. With the pager gap
above, the notifier also remains verified by tests only, exactly as it was at the
end of Stage B.

**For the exit:** one of three endpoints has held its 48 hours. Deploy 2
(`/flush/staging/run`) and deploy 3 (`/flush/silver/run`) are still owed, as is a
production page from `hourly_analytics_refresh` naming a failed task and quoting
its `failure_reason`.

**Recipes.** Read from production 2026-09-10, 05:55–05:58 UTC.

```bash
docker exec cartracker-airflow-scheduler airflow dags list-runs compact_silver
```

Loki instant queries, each anchored at the window's end with `time=` so `[48h]`
covers the gate exactly. An unanchored `[48h]` ends at *query time* and silently
measures a different window — read this way first, it shifted the span 10.5 hours
past the gate and excluded the 09-08 04:10Z run:

```bash
curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum(count_over_time({service="archiver", level="ERROR"} |~ "compact_silver: run failed" [48h]))' \
  --data-urlencode 'time=2026-09-09T19:25:00Z'

curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum by (level) (count_over_time({service="archiver"}[48h]))' \
  --data-urlencode 'time=2026-09-09T19:25:00Z'

curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum(count_over_time({service="archiver", level="WARNING"} |~ "would fail" [48h]))' \
  --data-urlencode 'time=2026-09-09T19:25:00Z'
```

**Deploy 2 of 3 — `/flush/staging/run`.** Built 2026-09-10, **not yet live** —
this paragraph records readings taken while building it, not a gate. The
endpoint logs at ERROR and raises
`HTTPException(500, detail=dict(result, failure_reason=reason))` on
`_flush_staging_failure_reason` rather than warning and returning 200. Checks on
this machine against the merged tree:
`python -m pytest -q -m "not integration" -p no:randomly` → **4054 passed, 720
deselected**; `python -m ruff check .` clean;
`python scripts/generate_service_contracts.py --check` exit 0.

**A 500 here skips the hour's dbt build, and this stage's own text says it does
not.** Stage C orders the deploys with *"`/flush/staging/run` — hourly, but the
dbt build does not read staging events"* against deploy 3's *"its failure now
skips the dbt build"*. That contrast does not hold. Read from
`airflow/dags/hourly_analytics_refresh.py:155`:

```
ready >> archiver_up >> flush_silver >> flush_staging >> dbt_runner_up >> build
```

`flush_staging_events` is upstream of `dbt_build`, so a red task here skips the
build exactly as a red `flush_silver_observations` would. What survives is the
narrower claim, about data rather than about the DAG: the build deploy 2 skips
would have been building from inputs the staging flush does not feed, and the
build deploy 3 skips would have been building on stale silver. **The ordering
holds on that reading; deploy 2's blast radius does not** — it is one skipped
hourly dbt build per failing run, not zero. As with deploy 1's notifier finding,
the stage text is left alone; what changed is `trigger_flush_staging`'s
docstring, which is where a reader meets the claim.

**Unlike deploy 1, this deploy can page.** Same file, line 162:
`[ready, flush_silver, flush_staging, build, reconcile_cooldowns] >> notify`,
with `trigger_rule="one_failed"`. So the first run meeting this predicate is the
notifier's live test — the one deploy 1 structurally could not give, and one
clause of this stage's exit.

**Its 48-hour gate has not opened.** Nothing is claimed here about the endpoint
in production. The deploy time and the gate reading append to this entry when
they exist.

**Deploy 2 went live 2026-09-10 19:16:00Z.** Merged as `6bcd3ac` at 19:13:31Z
([PR #412](https://github.com/whitewalls86/new_car_tracker/pull/412)), pulled to
`/opt/cartracker`, and deployed with `bash scripts/redeploy.sh archiver` in a
tmux session; the script reported every pollable service healthy after 6 s. The
live time is the container's own, not the merge time deploy 1's row had to
stand in with:

```bash
docker inspect cartracker-archiver --format '{{.State.StartedAt}}'
# 2026-09-10T19:16:00.626134898Z
```

**The running container was asked what it loaded, rather than the pull being
taken as proof.** Both halves agree:

```bash
docker exec cartracker-archiver grep -n 'flush_staging: run failed' /app/archiver/app.py
# 632:            logger.error("flush_staging: run failed — %s", reason)

docker exec cartracker-archiver python -c "import json,urllib.request; \
d=json.load(urllib.request.urlopen('http://localhost:8001/openapi.json')); \
print(sorted(d['paths']['/flush/staging/run']['post']['responses'].keys()))"
# ['200', '500']
```

**The 48-hour gate runs 2026-09-10 19:16Z → 2026-09-12 19:16Z.** Nothing is read
from it yet. When it closes, its reading appends here using deploy 1's recipes
with `time=2026-09-12T19:16:00Z` — anchored, for the reason deploy 1's entry
records — and with `compact_silver` swapped for `hourly_analytics_refresh` in
the DAG-run read, since that is the DAG carrying this endpoint.

**Deploy 2's gate was read at 2026-09-12 17:34:57Z, 1h41m before it closes, at
the maintainer's call — "I'm fine doing this two hours early if nothing has
turned up" — and it is clean.**

- **The deploy under test ran throughout.** `cartracker-archiver` started
  2026-09-10T19:16:00.626Z with 0 restarts.
- **Every hourly run succeeded: 46 of 46**, `scheduled__2026-09-10T20:00`
  through `scheduled__2026-09-12T17:00`. `flush_staging_events` is upstream of
  `dbt_build`, so a 500 turns the run red. Forty-six green runs mean the
  endpoint answered 200 forty-six times and `_flush_staging_failure_reason`
  returned `None` each time. As with deploy 1, the green runs are the evidence
  and the Loki reads corroborate them.
- **Archiver emitted 614 records over the window, all INFO.**
  `{service="archiver", level="ERROR"} |~ "flush_staging: run failed"` returned
  **0**, and `{service="archiver", level="WARNING"} |~ "would fail"` returned
  **0**, so the observation stream deploy 3 is read from is still
  uncontaminated.
- **Not covered:** the 18:00Z and 19:00Z runs, which fall inside the gate after
  this read.

**What this window did not prove.** No run met the predicate, so the 500 path is
still verified by `tests/archiver/test_app.py` alone. `notify` has a
`one_failed` trigger and no task failed, so it did not fire. That is inferred
from the run states, not read from task instances. The notifier's live proof is
still owed, and deploy 3 is now the last deploy that can supply it.

**For the exit:** two of three endpoints have held their 48 hours. Deploy 3
(`/flush/silver/run`) is still owed, as is a production page from
`hourly_analytics_refresh` naming a failed task and quoting its
`failure_reason`.

**Recipes.** Read from production 2026-09-12. This read was taken early, so it
differs from deploy 1's: each Loki query is anchored at the read time
(`time=2026-09-12T17:34:57Z`) with a range of the seconds since the deploy
(`[166737s]`), rather than `[48h]` anchored at the gate's end.

```bash
docker inspect cartracker-archiver --format '{{.State.StartedAt}} {{.State.Status}} restarts={{.RestartCount}}'
docker exec cartracker-airflow-scheduler airflow dags list-runs hourly_analytics_refresh -s 2026-09-10T19:16:00+00:00 -o plain

curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum(count_over_time({service="archiver", level="ERROR"} |~ "flush_staging: run failed" [166737s]))' \
  --data-urlencode 'time=2026-09-12T17:34:57Z'
# same anchor for: sum by (level) (count_over_time({service="archiver"}[166737s]))
# and: sum(count_over_time({service="archiver", level="WARNING"} |~ "would fail" [166737s]))
```

**Deploy 3 of 3 — `/flush/silver/run`.** Built 2026-09-12, **not yet live** —
this paragraph records the build, not a gate. The endpoint logs
`flush_silver: run failed` at ERROR and raises a 500 whose `detail` is the
summary plus `failure_reason`, on `_flush_silver_failure_reason`, rather than
warning and returning 200. It may go live a little short of 48 hours after
deploy 2 (2026-09-12 19:16Z), at the maintainer's call, on deploy 2's gate
having read clean early. Checks on this machine against the branch:
`python -m pytest -q -m "not integration" -p no:randomly` → **4102 passed, 2
failed, 720 deselected** — the two failures are
`tests/rules/test_ci_compose_parity.py`, which fail identically on master here
because this machine's `docker compose` rejects `-p`; `python -m ruff check .`
clean; `python scripts/generate_service_contracts.py --check` exit 0.

**All three endpoints raise the envelope's member, not a retyped 500.** Deploys
1 and 2 raised `HTTPException(status_code=500, ...)`, and deploy 2's needed a
`RETYPED_STATUS_LEDGER` entry to pass
`test_no_call_site_retypes_a_status_the_envelope_declares`. This deploy raises
`shared.api_envelope.ServiceFailure` on all three, so the silver flush never
enters the ledger and the compact and staging entries leave it. The response is
the same 500 with the same body. This is [Plan 180](plan_180_seam_program.md)
Stage D's drain (CAR-121), done early for three endpoints: the spec's handler
form is "refusals are raised as members"
([`seam_ideal_state_spec.md`](../planning/seam_ideal_state_spec.md) M1), so
Stage D does not revisit these call sites. What it still owes them is the
declaration side — each decorator's hand-written `responses={500: ...}` becomes
`refusals(ServiceFailure)` once Stage A builds that helper.

**The `would fail` window is closed, not just empty.** The silver flush was the
last caller of Stage B's `_warn_would_fail`, so the helper is deleted, along
with the three tests asserting each flipped endpoint no longer emitted it.
`{service="archiver", level="WARNING"} |~ "would fail"` can no longer return
anything, so deploy 3's gate does not read it: a zero there is no longer
evidence.

**`FlushSilverResponse` does not gain `failure_reason`,** despite its old
docstring saying it would on this deploy. As with deploy 2's model, the key
only ever rides the 500, inside `ErrorResponse.detail`; a 200 is a clean run
and never carries it. The docstring now says so.

**Its gate** runs 48 hours from the container's `StartedAt` and is read with
deploy 2's recipes, anchored at the gate's end, with `flush_staging` swapped
for `flush_silver` and the `would fail` query dropped. As with deploy 2, this
deploy can page: `flush_silver_observations` fans into `notify` on
`one_failed`. The Stage C exit still owes a production page from
`hourly_analytics_refresh` naming a failed task and quoting its
`failure_reason`, and a clean 48 hours will not supply it.

**Deploy 3 went live 2026-09-12 18:35:33Z**, 47h19m after deploy 2, at the
maintainer's call. Merged as `c1b9eab` at 18:34:05Z
([PR #424](https://github.com/whitewalls86/new_car_tracker/pull/424)), pulled
to `/opt/cartracker`, and deployed with `bash scripts/redeploy.sh archiver` in
a tmux session: the drain authorized after 0 s, the archiver was healthy after
6 s, and the script exited 0 with deploy intent released. The live time is the
container's own:

```bash
docker inspect cartracker-archiver --format '{{.State.StartedAt}}'
# 2026-09-12T18:35:33.155194609Z
```

**The running container was asked what it loaded.** All three endpoints raise
the envelope member, the warning helper is gone, and each route declares its
500:

```bash
docker exec cartracker-archiver grep -n -E "run failed|raise ServiceFailure|_warn_would_fail" /app/archiver/app.py
# 253: logger.error("flush_silver: run failed — %s", reason)
# 254: raise ServiceFailure(detail=dict(result, failure_reason=reason))
# ... the same pair at 284-285 (compact_silver) and 633-634 (flush_staging); no _warn_would_fail

docker exec cartracker-archiver python -c "import json,urllib.request; \
d=json.load(urllib.request.urlopen('http://localhost:8001/openapi.json')); \
print(sorted(d['paths']['/flush/silver/run']['post']['responses'].keys()))"
# ['200', '500']  -- likewise /flush/staging/run and /compact/silver/run
```

**The 48-hour gate runs 2026-09-12 18:35:33Z → 2026-09-14 18:35:33Z.** It is
read with deploy 2's recipes anchored at `time=2026-09-14T18:35:33Z`, with
`flush_silver: run failed` in place of `flush_staging` and no `would fail`
query.
