# Claude Prompt: Plan 131 Stage 5 — Lifecycle DAG And Observability

You are working in the `cartracker-scraper` repo. Branch off `master`.

Read `docs/plan_131_packed_cold_storage.md` (especially **Stage 5** and **Stage
4 as built**) and `docs/runbook_plan_131_stage_3_4.md` first. They are the
source of truth and record every decision below with the measurements behind
it.

## Where this stands

Stages 1-4 are **built**. Stage 3 is deployed; the first Stage 4 prune ran
2026-08-14 (100 objects, 0 refused, readback clean). April is packed in full;
further packing and pruning is **running manually right now**.

Everything so far is a **one-time** reclamation driven by hand. Stage 5 is what
turns it into a property of the system: a closed month collapses from ~1.1M
objects to ~82, permanently, and inodes stop being the binding constraint at
all. The plan doc argues this at length under *"Why Stage 5 is the point of the
plan, not its tidy-up"* — that framing is the brief.

## Scope

- A lifecycle DAG that **packs a closed month and then prunes it**, on a
  schedule, unattended.
- Observability that answers the two questions the plan exists for: *are inodes
  still being freed*, and *has any verification ever failed*.

**Out of scope, do not start:** re-packing, `results_page` packing, Plan 132's
reparse, and — explicitly — a **retention/expiry policy**. The plan doc leaves
that Out of Scope and Plan 132 Stage 4 only *argues* the predicate belongs
here. Packing changes the slope of the growth curve; deciding what to throw
away is a separate decision and does not ride along with this one.

---

## The five decisions this stage has to make

Each has a recommendation. Read the rationale before overriding it — several
are load-bearing on measurements already in the plan doc.

### D1 — One DAG, packing *and* pruning. Not a pack-only DAG.

**Packing frees no inodes.** A DAG that only packs automates the half that
doesn't help and leaves the half that does as a manual chore, which is exactly
the state Stage 5 exists to end.

The tension is that `delete_packed_source_html` **requires** `year`/`month` and
deliberately has no discovery mode — "this job removes data, and the month it
removes data from is an operator's decision, not something inferred from what
happens to be packed."

That property is preserved, not violated, by taking the target from the pack
task's own report: the deleter still never discovers anything, and the month is
still named explicitly by its caller. The decision moves into the DAG, where it
is reviewable, rather than into the deleter, where it was refused.

```
check_deploy_intent → check_archiver_health → pack → prune → notify
```

### D2 — One DAG run drains one whole month. Not a bounded slice per run.

**The listing is the fixed cost and it decides how to run this** (plan doc,
measured): 701-809 s to enumerate April's 557,065 objects, paid on *every* run
regardless of the cap. Draining a month one pack at a time would be ~32
listings — roughly seven hours of pure enumeration to do about an hour of work.

So the DAG passes `max_packs: 0` to the packer and a large `max_objects` to the
deleter, and accepts a multi-hour task.

> **Fix the cap asymmetry first — it is a trap, not a quirk.** `max_packs = 0`
> means *unlimited* in both jobs, but `max_objects = 0` in the deleter sets
> `budget = 0`, immediately trips `capped = True`, and deletes **nothing**
> (`delete_packed_source_html.py:735`). Two adjacent caps on the same call,
> opposite meanings, and the failure is silent: the run reports success and a
> green DAG frees nothing.
>
> Align it — `max_objects <= 0` means no cap — and update the CLI help (which
> already says "0 for no cap" for `--max-packs`), the docstring, and the plan
> doc. Add a test that `max_objects=0` drains a whole bucket, and keep the
> existing tests that a positive cap is honoured exactly. This is a deliberate
> exception to "touch only what you must": the DAG is the first caller that has
> to pass an uncapped delete, so it is this change that walks into the trap.

### D3 — Single-flight, and a cooperative stop on deploy intent. **DECIDED.**

Two separate problems that share one mechanism — a check at a safe boundary.

#### D3a — Single-flight

There is **no lock on either endpoint today.** `shared/job_counter.active_job()`
is a counter feeding `/ready`; it refuses nothing. The only 409 in
`archiver/app.py` is Plan 120's snapshot guard.

That is fine while every run is a human typing a command. It stops being fine
the moment a DAG retries: a multi-hour HTTP call that dies on a dropped
connection leaves the job **still running**, and the retry starts a second
packer on the same bucket. Two concurrent packers both list the same prefix,
both compute the same `next_seq`, and race to write packs under the same key.

Required: a single-flight guard keyed per job (`pack_bronze`, `pack_prune`)
returning **409** while one is in flight. `sensors.post_json` already turns 409
into a graceful skip — see D5 for why the pack task must then *short-circuit
the prune* rather than proceed.

#### D3b — A deploy stops the job at its next boundary; a release resumes it

A month-scale run is hours, and a backlog run could be ten. Today a deploy that
lands mid-run either restarts the container underneath it or runs migrations
and container churn against a job that is actively reading and deleting. The
job survives — a pack without a sidecar is an orphan, reported and never
deleted from, and the deleter deletes each object at most once — but an hour of
in-flight work is thrown away and the failure looks like a crash rather than a
deploy.

**Design: cooperative cancellation at an existing boundary, plus resume by
retry.** Nothing is killed and no signal handling is involved.

1. **A new column on `deploy_intent`.** `pause_long_jobs BOOLEAN NOT NULL
   DEFAULT true` (migration `V042`). `POST /deploy/start` takes an optional
   `pause_long_jobs=false` for deploys that touch nothing this job depends on.
   Default `true`, because the safe behaviour should be the one you get by
   forgetting.
2. **The jobs poll it at a boundary they already have.** The packer, after a
   pack is written, verified and its sidecar stored. The deleter, after a pack
   is drained (and, because a single pack is thousands of objects, also on its
   existing `PROGRESS_EVERY` tick). **Never mid-pack** — the boundary is chosen
   so that stopping costs nothing beyond the current unit.
3. **They return cleanly**, `stopped_for_deploy: true` plus the usual summary,
   with everything completed so far already durable. Not an error, not a
   partial write.
4. **Resume is an Airflow retry, not new machinery.** The task raises a
   retryable exception on `stopped_for_deploy`; `retries=6,
   retry_delay=15 minutes` covers a 90-minute deploy window. The endpoint also
   refuses to *start* while intent is pending, so a retry that lands mid-deploy
   simply waits for the next one. Exhausting the retries means the deploy
   intent never cleared, which is worth paging for. `STALE_LOCK_MINUTES = 30`
   already self-heals a forgotten intent.

**What this costs to be honest about:** a stop-and-resume repays the listing —
700-800 s for April — so a deploy mid-run costs ~13 minutes of re-enumeration
on top of the current unit. That is the argument for checking at a coarse
boundary rather than a fine one, and it is still far cheaper than losing the
run.

**Grants:** the worker reads `deploy_intent` as `scraper_user`. Confirm the
SELECT grant exists before assuming it does — this table has only ever been
read by `ops` and by Airflow's connection.

### D4 — Its own worker. **DECIDED.**

Plan 120 Gate C.5 already established the precedent and the reason:
production-sized work run synchronously in the archiver API starved
flush/cleanup/compact and Airflow's health checks. A ten-hour pack run is a
larger version of exactly that, and the plan doc's "lives in the archiver,
mirroring `compact_silver`" was written before the job's real duration was
measured.

**Shape: a second long-running container on the archiver image, not a one-shot.**
`snapshot-worker` is invoked by hand with `docker compose run`, which Airflow
cannot do without a Docker socket (that is Plan 108, and it is not built). A
service that speaks HTTP needs none of that — `dbt_runner` is the in-repo
precedent.

```yaml
pack-worker:
  build: { context: ., dockerfile: archiver/Dockerfile }
  image: cartracker-archiver          # same image, no new build
  container_name: cartracker-pack-worker
  restart: unless-stopped
  volumes: [ pack_worker_logs:/usr/app/logs ]
  environment:
    # …archiver's env, plus:
    PACK_BRONZE_DICT_ID: ${HTML_COMPRESSION_DICT_ID:-}
    ARCHIVER_ALLOW_PACK_JOBS: "true"
```

Five details that will each cost a debugging pass if skipped:

- **The DAG points at `http://pack-worker:8001`.** `http_health_sensor` and
  `/health` work unchanged.
- **Guard the archiver's own copies of the pack endpoints**, exactly as
  `_ALLOW_SYNC_SNAPSHOT_COHORT` guards the snapshot one: 409 with a message
  naming the worker, unless `ARCHIVER_ALLOW_PACK_JOBS=true`. Otherwise there
  are two live entry points to a job whose whole safety story assumes one.
- **`pack_worker_logs` must be its own named volume**, and the free-space floor
  reads `/usr/app/logs` — that is deliberate (plan doc: the container's `/` is
  a 49 GB overlay on the wrong device). A named volume under the same docker
  volumes root reads the same filesystem as MinIO's data, which is the property
  the floor depends on. **Verify it with `df -i` inside the new container
  before the first run**, do not assume it inherits.
- **Promtail needs a job for it.** `promtail/promtail.yml` maps
  `/logs/archiver/app.log*` → `service: archiver`; add
  `/logs/pack-worker/app.log*` → `service: pack-worker` and the read-only mount
  in the promtail service. Without it the `REFUSED` alert in Step 4 watches a
  log stream that does not exist.
- **No ports published.** Internal network only, like the archiver.

The one thing this costs: a second idle uvicorn. Against a job that would
otherwise contend with hourly analytics for ten hours, that is not a close
call.

### D5 — The endpoints don't signal failure. Fix that at the endpoint, not in the DAG.

Both processors return a **summary dict** rather than raising — partial results
are still results, which is right for a job you run by hand and read. Both then
translate that summary into a failure **on the CLI**:

```python
# delete_packed_source_html.py:872
return 1 if result["error"] or result["objects_refused"] else 0
```

**The HTTP side never got the same translation.** `archiver/app.py` returns
whatever the processor returned, with a 200, so `resp.raise_for_status()` —
the entire check in `compact_silver`'s DAG — passes on a run that deleted
nothing, refused forty thousand objects, or never started. The CLI is the face
that got this right; HTTP is the one missing its half.

**This is not a Plan 131 quirk and not CLI residue — it is every archiver
processor.** `flush_silver_observations` returns `{"flushed": 0, "error": ...}`,
`flush_staging_events` the same, `compact_silver` the same. No DAG in
`airflow/dags/` inspects an `error` key. An hourly flush that fails returns 200,
the task goes green, and `hourly_analytics_refresh` proceeds to build dbt on
stale data.

**The house pattern already exists and it is the other service.** `dbt_runner`
raises `HTTPException(status_code=500, detail=result)` on a failed build, and
`sensors.post_json` was *built for that* — `JsonPostError` carries the parsed
body so the notify task can quote the stderr. The client-side machinery for
doing this correctly is already in the repo, being used by exactly one service.

So Stage 5 does the translation **at the endpoint**, matching `dbt_runner`, and
the DAG stays thin and uses `post_json`:

| outcome | HTTP | why |
|---|---|---|
| `error` set | **500**, summary as `detail` | the run aborted; same condition as the CLI's exit 1 |
| `objects_refused > 0` | **500**, summary as `detail` | verification disagreed — the loudest signal this job produces |
| `stopped_for_deploy` | **200** + flag | a clean stop, not a failure. The DAG raises a *retryable* exception (D3b) |
| `capped`, `orphan_packs` | **200** + fields | warn and carry — see below |
| single-flight busy | **409** | `post_json` already maps it to a graceful skip |

**Scope discipline:** do this for the two Plan 131 endpoints only. Flipping
`flush/silver`, `flush/staging` and `compact/silver` to signal failure is
correct and should happen — but it would convert long-standing *silent*
failures into sudden DAG failures and pages, which is a deliberate change with
its own blast radius, not something to smuggle in under a packing plan. **File
it as its own ticket**, referencing this section.

What follows is why the predicates are not what the field names suggest — three
of them are wrong in a way that reads as correct.

#### "Did the pack drain the month?" — **not** `objects_pending == 0`

`pending` is computed once, at planning time, before a single pack is written:

```python
# pack_bronze_html.py:866
"pending": len(remaining),      # listed objects minus those already in sidecars
```

It is never decremented as packing proceeds. So a **perfect** run that packed
all 557,065 April objects still reports `objects_pending: 557,065`. The field
means *"how much there was to do"*, not *"how much is left"*, and a DAG that
reads it the obvious way concludes every successful run failed.

The field that actually answers the question is `stopped_at_max_packs` in the
per-bucket summary — set only when the run hit its pack cap and quit early. So:

```python
drained = not summary["stopped_at_max_packs"] and not summary["error"]
```

#### "Did the prune succeed?" — `objects_deleted > 0` is not it either

`objects_refused` counts objects where one of the three per-member checks
disagreed: the pack could not be resolved to the prefix a reader would look
under, the extracted bytes did not match the sidecar's `raw_sha256`, or the
live object did not match the extracted bytes. **Nothing was lost** — that is
the safety property doing its job — but something is wrong with a pack or the
resolver, and it is the single most important signal this job produces.

The CLI already treats it as failure:

```python
# delete_packed_source_html.py:872
return 1 if result["error"] or result["objects_refused"] else 0
```

The HTTP endpoint returns **200 with the count in the body**. So the DAG has to
re-implement that line, or a run that refused every object in a pack reports
green. `objects_deleted == 0` is *not* a failure on its own — a fully drained
month legitimately deletes nothing and returns early.

#### "Was it skipped?" — a 409 arrives as success

`sensors.post_json` turns a 409 into `{"ok": True, "skipped": True}` and returns
normally, so a pack task blocked by the D3a single-flight guard looks exactly
like a pack task that worked. If the prune then runs, it deletes from a month
another process is actively packing. Not unsafe — only sidecar-verified members
are ever deleted — but it is a green DAG doing something nobody asked for, on
the one job in the system that removes data. The prune must check
`result.get("skipped")` and short-circuit.

#### The two that should warn, not fail

- **`orphan_packs` non-empty** — a previous run was interrupted. Stage 2 and
  Stage 4 both already report these and never delete from them, so carrying the
  condition is safe and failing the DAG on it costs more than it buys.
- **`capped: true`** — expected on a manual backlog run, a surprise on a
  scheduled one now that the caps are uncapped (D2). Log it; let the next run
  pick up where this one stopped.

#### Where each predicate lives

Split by who needs to know:

- **The failure predicates go in the endpoint**, as a small
  `_failure_reason(summary) -> str | None` per job in `archiver/app.py`, so a
  `curl` gets the same answer the DAG does and the CLI's contract finally has
  an HTTP counterpart. Unit-test it directly against summary dicts.
- **The interpretation stays in the DAG** — `check_pack_result` /
  `check_prune_result`, plain functions over the response dict, unit-testable
  without Airflow, exactly as `export_ci_lake_snapshot.check_snapshot_result`
  already is. They handle the warn-not-fail cases, the `skipped` short-circuit,
  the `stopped_for_deploy` retry, and reading `stopped_at_max_packs` to decide
  whether the month drained.

---

## Implementation

Build in this order. Steps 1-4 are prerequisites the DAG depends on; the DAG
lands last, when there is somewhere safe for it to point.

> **The shape, stated once, because it governs several steps below.** Anything
> the DAG calls is a **processor**: a pure function returning a summary dict, an
> HTTP endpoint on a service, and a thin CLI for manual runs — the shape
> `pack_bronze_html` and `delete_packed_source_html` already have, and the shape
> every other production job in this repo has. The DAG holds sensors, one HTTP
> call per task, and the result predicates from D5. It holds no logic and shells
> out to nothing.
>
> `scripts/` is for one-off measurement that answered a question once. Stage 5
> makes the read-path verifier recurring, so it stops being a script (Step 7b).

### Step 1 — Align the cap semantics (D2)

`max_objects <= 0` means no cap in `delete_packed_source_html`, matching
`max_packs`. CLI help, docstring and plan doc updated to say so.

**Verify:** new test — `max_objects=0` drains a whole multi-pack bucket;
existing tests that a positive cap is honoured to the object still pass.
`pytest tests/archiver/test_delete_packed_source_html.py`.

### Step 2 — Single-flight guard (D3a)

A named non-blocking lock around `_pack_bronze_html` and
`_delete_packed_source_html`; **409** with a message naming the job in flight.

Put it next to `active_job` in `shared/job_counter.py` if it generalises
cleanly; a per-job-name `threading.Lock` in `app.py` is acceptable and smaller.
Do **not** build a distributed lock — one worker process is the whole
population.

**Verify:** a second call while the first holds the lock gets 409 and does not
enter the processor.

### Step 3 — Deploy-intent pause and resume (D3b)

1. `db/migrations/V042__deploy_intent_pause_long_jobs.sql` — add
   `pause_long_jobs BOOLEAN NOT NULL DEFAULT true`; confirm/grant `SELECT` on
   `deploy_intent` to `scraper_user`.
2. `ops/routers/deploy.py` — `POST /deploy/start` accepts optional
   `pause_long_jobs` (default `true`) and writes it; `_intent_status` returns
   it.
3. A shared helper — `shared/deploy_intent.py`, one function,
   `long_jobs_paused() -> bool` — that both processors call at their
   boundaries. It must **fail open**: a DB error returns `False`, because a
   Postgres blip should not silently stop a ten-hour job. Log at WARNING when
   it does.
4. Both processors: check at the boundaries named in D3b, set
   `stopped_for_deploy: true` in the summary, break cleanly, and refuse to
   start if the flag is already set.

**Verify:** unit tests with the helper patched — the packer stops after the
current pack and its sidecar exists; the deleter stops between packs having
deleted only verified objects; both set the flag; a DB error does not stop
either. Then `pytest tests/ops/test_app.py` for the endpoint, and the Flyway
migration applies cleanly in CI.

### Step 4 — The `pack-worker` service (D4)

Compose service, the archiver-endpoint guard, the promtail job and mount, and
the named volume — all four as specified in D4. Nothing new is built here; it
is the same image with a different entry point in the network.

**Verify:** `docker compose up -d pack-worker` (locally), then `df -i
/usr/app/logs` inside it reads the same filesystem as the archiver's, and
`POST /pack/bronze/run` with `apply: false` against the worker returns a
summary while the same call against the **archiver** returns 409.

### Step 5 — `airflow/dags/pack_bronze_html.py`

Thin, like `compact_silver`: sensors plus HTTP calls, no packing logic. Model
the params/XCom handling on `export_ci_lake_snapshot.py`, which already does
the `params` + `dag_run.conf` merge and the `check_*_result` split that makes
the result checks unit-testable without Airflow.

```python
PACK_WORKER_URL = "http://pack-worker:8001"

DEFAULT_PARAMS = {
    "artifact_type": "detail_page",
    "apply": True,
    "max_buckets": 1,          # oldest eligible closed month
    "max_packs": 0,            # 0 = drain it; the listing is the fixed cost
    "prune": True,
    "prune_max_objects": 0,    # 0 = no cap, after Step 1
    "prune_max_packs": 0,
}
```

- **Schedule: `"0 6 3 * *"`** — day 3 at 06:00 UTC. `PACK_SETTLE_DAYS` is 1, so
  the month that just closed is eligible from day 2; day 3 leaves a margin and
  sits clear of the hourly analytics and the 04:10 compaction.
- `catchup=False`, `max_active_runs=1`, `tags=["maintenance"]`.
- `schedule` handles **steady state only**. The April-July backlog is four
  months and one month per run would drain it in April 2027 — the backlog stays
  manual, as the run sheet already says. Exposing the params is what lets a
  manual trigger do it.
- Use `post_json`, not `requests.post` + `raise_for_status` — the endpoints now
  signal failure properly (D5), and `JsonPostError` carries the summary into
  the notify task the way `dbt_build` already quotes dbt's stderr.
- `timeout=43200` on the pack POST (a backlog month can be ten hours),
  `retries=6`, `retry_delay=timedelta(minutes=15)` — sized for the D3b resume,
  and safe only because both jobs are resumable: the sidecars are the packer's
  checkpoint, the surviving-object listing is the deleter's.
- The prune task takes `year`/`month` from the pack task's XCom
  (`buckets[0]["year"]`, `["month"]`) — see D1.
- `notify` mirrors `hourly_analytics_refresh._notify`: Telegram on final
  failure. A `stopped_for_deploy` retry is an intermediate failure and does not
  page.

**Verify:** `pytest tests/integration/airflow/test_dag_integrity.py` with the
new `DAG_SPECS` entry, plus unit tests on `check_pack_result` /
`check_prune_result` covering every case in D5 — including the two that must
warn rather than fail.

### Step 6 — Inode alerts. This is the highest-value item in the stage.

`grafana/provisioning/alerting/rules.yml` has **Disk Space Warning (>80%)** and
**Disk Space Critical (>90%)** on `node_filesystem_avail_bytes`. There is **no
inode alert at all** — on a plan whose entire premise is that bytes improved
while the inode clock did not slow.

node-exporter is already scraped and already exports what is needed. Add two
rules mirroring the byte ones exactly:

```
(1 - node_filesystem_files_free{fstype!="tmpfs",mountpoint!~"/boot.*"}
   / node_filesystem_files{fstype!="tmpfs",mountpoint!~"/boot.*"}) * 100
```

at 80% and 90%. No new code, no new exporter, no new scrape target.

**Verify:** `pytest tests/test_observability_config.py` (it already validates
this file), and the rendered expression returns data against the live
Prometheus before the PR is called done.

### Step 7 — The two numbers the plan asks for, cheaply

The plan's Stage 5 asks for objects packed, packs written, bytes/inodes
reclaimed, extraction latency p50/p95, and verification failures. Take them
from what already exists rather than building a metrics pipeline:

- **Object count is already scraped.** `prometheus.yml` has a `minio_bucket`
  job on `/minio/v2/metrics/bucket`, which exports per-bucket object totals.
  The plan's headline — 4.5M objects collapsing to hundreds — is a **dashboard
  panel, not code**. Add it to `grafana/dashboards/infrastructure.json`
  alongside the inode panel.
- **Verification failures already log at ERROR.**
  `delete_packed_source_html.py:520` logs `REFUSED`, logs ship to Loki, and
  rules.yml already contains Loki-sourced alerts. Add one matching `REFUSED`
  on `{service="archiver"}` — "should be zero; alert on any" is then true
  without a new counter.
- **Latency p50/p95 and read-path proof** come from
  `verify_pack_read_path`, which becomes a **processor** in Step 7b below —
  not a script the DAG shells out to.

If per-run gauges are wanted graphed rather than logged, that needs the
archiver added to `prometheus.yml` and a `prometheus_client` `/metrics`
endpoint (`ops/metrics/duckdb_gauges.py` is the in-repo pattern). **Separable —
recommend deferring.** Gauges reset to 0 on restart, and for a monthly job a
zero is indistinguishable from a good run, so it would need a
`last_run_timestamp` and a staleness alert to be honest. That is a fair amount
of machinery for numbers the run logs already carry.

### Step 7b — Promote `verify_pack_read_path` to a processor

**A recurring production job is not a script.** `pack_bronze_html` and
`delete_packed_source_html` are already the right shape — pure function,
HTTP endpoint, thin CLI. `scripts/verify_pack_read_path.py` is not, because it
was written for a one-off Stage 3 proof. Stage 5 makes it recurring, so it
moves.

Move it to `archiver/processors/verify_pack_read_path.py` and give it the same
three faces the other two have:

| face | shape |
|---|---|
| function | `verify_pack_read_path(*, artifact_type, year, month, per_pack, warm_reads, seed, bucket) -> Dict[str, Any]` — the existing `run()`, renamed to match its module, keyword-only, returning a summary |
| endpoint | `POST /pack/bronze/verify` on the worker, payload keys allow-listed like `/pack/bronze/run` already does |
| CLI | keep it. `_parse_args` / `main` unchanged behaviour, including the `1 if result["failed"] or not result["verified"] else 0` exit code — the run sheet and Plan 133 both invoke it by hand |

Then add a **canary task** to the DAG after the prune, sampling a bounded
number of members per pack. That is what converts the one-off Stage 3 proof
into the recurring one the plan asks for, and it is the honest version of
"verification failures should be zero; alert on any."

Three things this move has to get right:

- **It is read-only and must stay that way.** The module docstring says so;
  keep that sentence, and keep it out of the single-flight lock — a canary that
  cannot run because a pack job is in flight is a canary that never runs when
  it matters most.
- **It already works post-delete**, checked rather than assumed: `verify_one`
  handles a missing source object (`object_present: False`,
  `verify_pack_read_path.py:129-137`) and falls back to pack-vs-sidecar. Do not
  "fix" that path while moving it.
- **Update the callers of the old path.** Checked, not guessed:
  - `tests/scripts/test_verify_pack_read_path.py` → `tests/archiver/`. Its body
    changes by exactly one line — `from scripts import verify_pack_read_path`
    becomes the processor import. Every assertion stays; these are the tests
    that prove the verifier refuses to call differing bytes identical, which is
    what authorizes Stage 4's deletions, so **nothing in them is rewritten as
    part of a move.**
  - `docs/runbook_plan_131_stage_3_4.md` lines 90 and 135 —
    `python -m scripts.verify_pack_read_path` →
    `python -m archiver.processors.verify_pack_read_path`.
  - `docs/plan_133_pack_read_path_hardening.md` references it twice, once as a
    **success-criteria gate**. A stale path there is a broken gate.

**`scripts/estimate_pack_savings.py` stays a script** — Stage 0, offline, run
once to answer a question that has been answered. The line is whether the thing
runs in production on a schedule, not whether it is useful.

**Verify:** `pytest tests/archiver/test_verify_pack_read_path.py` (moved tests
pass unchanged against the new import path), the endpoint returns a summary
against a packed month, and `python -m archiver.processors.verify_pack_read_path
--year 2026 --month 4` still behaves as the run sheet documents.

### Step 8 — Docs

- Plan doc: mark Stage 5 built, fill in the Files Changed rows, record the
  first scheduled month's measured numbers.
- Run sheet: replace the closing *"Stage 5 is not built... packing and pruning
  are manual"* paragraph with how to trigger, pause and back out the DAG.

---

## Files changed

| File | Change | Step |
|---|---|---|
| `archiver/processors/delete_packed_source_html.py` | `max_objects <= 0` = no cap; deploy-intent boundary check | 1, 3 |
| `archiver/processors/pack_bronze_html.py` | Deploy-intent boundary check | 3 |
| `shared/job_counter.py` | Named single-flight lock | 2 |
| `db/migrations/V042__deploy_intent_pause_long_jobs.sql` | New — `pause_long_jobs` column + grant | 3 |
| `ops/routers/deploy.py` | Optional `pause_long_jobs` on `/deploy/start`, surfaced in `/deploy/status` | 3 |
| `shared/deploy_intent.py` | New — `long_jobs_paused()`, fails open | 3 |
| `docker-compose.yml` | `pack-worker` service, `pack_worker_logs` volume, promtail mount | 4 |
| `promtail/promtail.yml` | `pack-worker` log job | 4 |
| `archiver/app.py` | `_failure_reason` → 500 on the two pack endpoints (D5); 409 guard unless `ARCHIVER_ALLOW_PACK_JOBS`; `POST /pack/bronze/verify` | 4, 5, 7b |
| `archiver/processors/verify_pack_read_path.py` | **Moved** from `scripts/` — function + endpoint + CLI | 7b |
| `scripts/verify_pack_read_path.py` | Deleted (moved) | 7b |
| `tests/archiver/test_verify_pack_read_path.py` | **Moved** from `tests/scripts/` | 7b |
| `docs/plan_133_pack_read_path_hardening.md` | Invocation path in its success-criteria gate | 7b |
| `airflow/dags/pack_bronze_html.py` | New — lifecycle DAG | 5 |
| `grafana/provisioning/alerting/rules.yml` | Inode warning/critical; `REFUSED` Loki alert | 6, 7 |
| `grafana/dashboards/infrastructure.json` | Inode panel; bronze object-count panel | 6, 7 |
| `tests/archiver/test_delete_packed_source_html.py` | Uncapped drain; deploy-intent stop | 1, 3 |
| `tests/archiver/test_pack_bronze_html.py` | Deploy-intent stop leaves a complete sidecar | 3 |
| `tests/archiver/test_app.py` | 409 concurrent; 409 unless allowed; verify endpoint | 2, 4, 7 |
| `tests/ops/test_app.py` | `pause_long_jobs` set and reported | 3 |
| `tests/airflow/test_pack_bronze_html_dag.py` | New — result-check predicates (D5) | 5 |
| `tests/integration/airflow/test_dag_integrity.py` | Register the DAG in `DAG_SPECS` | 5 |
| `tests/test_observability_config.py` | Covers the new rules automatically — confirm it does | 6 |
| `docs/plan_131_packed_cold_storage.md` | Stage 5 status + measured results | 8 |
| `docs/runbook_plan_131_stage_3_4.md` | Replace the "Stage 5 not built" close | 8 |

## What would make this stage wrong

- Automating the pack but not the prune (D1) — ships the half that frees
  nothing.
- Slicing a month across runs (D2) — pays the 12-minute listing 32 times.
- Adding a delete grace period back in through the DAG's scheduling. The plan
  doc killed it on argument, twice; a monthly cadence is not a place to
  reintroduce it quietly.
- Letting the prune run after a **skipped** pack (D5).
- Fixing D5 only in the DAG. The endpoint is where the CLI's error contract is
  missing; a check that lives only in Airflow leaves `curl` and every future
  caller lying.
- Fixing D5 for *all* archiver endpoints in this PR. Correct, out of scope, and
  it would turn long-standing silent failures into an unrelated pager storm.
- Making `long_jobs_paused()` fail closed. A Postgres blip must not stop a
  ten-hour job.
- Killing a job mid-pack instead of at a boundary. The stop is cooperative; the
  point is that everything completed so far is already durable.
- Leaving the read-path verifier in `scripts/` and having the DAG shell out to
  it. A recurring production job gets the same three faces as every other one.
- Putting the canary behind the single-flight lock. It is read-only, and the
  moment it most needs to run is the moment a pack job is in flight.
- Building a retention policy because Stage 5 is the only recurring job in
  reach.
