# Plan 162 Stage L — SQL execution from both directions

**Legacy:** Stage 7 · **Issue:** CAR-51 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage L. It carries the summary; the sections below are the detail.

---

#### The finding that matters most: two production defects only execution found

CI's first run failed two jobs, and both traced to one cause. **psycopg2 counts
parameter placeholders across the whole statement string, comments included**,
so a comment written to *explain* a parameter adds one.

`ops/sql/set_deploy_intent.sql` explained its `interval` construct by quoting
the placeholder. That made the statement expect four parameters where
`ops/routers/deploy.py` passes three, so `/deploy/start` raised, the router
caught it, and returned **503** — seven Layer 4 tests in
`tests/integration/ops/test_deploy_intent.py` failed on that alone, in a code
path this stage was not supposed to touch.
`ops/sql/insert_blocked_cooldown_events_batch.sql` did the same to
`execute_values`, which refuses outright any statement carrying two
placeholders.

A third was a live trap that had not sprung. `processing/sql/claim_artifacts.sql`
carried a *named* placeholder in its first comment line and worked, because a
named placeholder resolves from the same dict however many times it appears.
Rename the parameter and it raises `KeyError` from a line that is not code.

**Every static rule in `test_testing_contract.py` passed on all three files.**
The statements were correctly extracted, correctly imported, correctly named,
and byte-faithful to what they replaced — verified mechanically, on normalised
whitespace, against the literals in `HEAD`. Two of them were broken. The suite
was green locally before the push and green locally after. This is the
argument for the stage, stated by the stage: **a statement that no layer
executes is not covered by anything, however carefully it was read.** Rule 5e
now fails on a placeholder inside a `.sql` comment, canaried in both
directions.

#### Three gaps this stage opened

**G5's stated measure was blind by construction.** The gap list said
"`.execute(` with a literal first argument"; `execute_values(cur, sql, rows)`
carries its statement second, and `ops/routers/maintenance.py:152` was a
literal `INSERT` sitting exactly there, in a module G5 never named. Measured
properly it was 66 sites in 15 modules, not 10 — and two of the named ten did
not belong: `shared/db.py`'s only match is inside `db_cursor`'s own docstring,
and `shared/duckdb_s3.py`'s seven are `INSTALL`/`LOAD`/`SET` session setup,
which name no schema and so cannot drift from one.

**G15 is what closing G5 revealed.** A statement bound to a name and executed
from there is invisible to both instruments at once: Rule 5b does not fire
because it is not at a call site, and Rule 5's denominator cannot count it
because there is no `.sql` file. Six were extracted by hand and only because
someone happened to read the files; the measured cost of that blind spot was 23
more in 11 modules, six of them in `ops/routers/admin.py`, a router this stage
would never have touched because every one of its statements is assigned before
it is executed.

**G17 was found by writing the tests.** `mark_artifact_status`,
`insert_artifact_event` and `insert_blocked_cooldown_cleared_event` existed
**byte-identically** under both `ops/sql/` and `processing/sql/`. Worse, Rule 5
credits a file when Layer 2 names its *stem* — and each pair shared one, so a
test of `processing`'s copy silently credited `ops`'s. Three files would have
been reported covered by a test that never executed them: the paraphrase
defect, arriving through the checker rather than through a test. They are one
file each under `shared/sql/` now, re-exported so no call site changed, and
G17 compares every production statement to every other so the next copy is a
failure rather than a discovery.

#### A merge that would have broken deploys silently

`ops/routers/deploy.py` selects `(kind, phase, generation, requested_by)` and
reads the result **positionally** — `row[1]` is phase. `coordination.py` selects
the same four columns as `(phase, generation, kind, requested_by)`. Merging the
two, which is what a tidy-up does when it sees duplicate SQL, puts `generation`
at `row[1]` and compares a number to `'none'`: every deploy would report itself
locked and nothing would fail. They are kept apart, each file saying why, and
`test_the_two_four_column_reads_are_not_interchangeable` asserts both orders
against the live server.

#### The scan surface was the third instance of one mistake

Both new rules were written against `service_packages()`, which answers "is
this a service" and not "is this production Python". The two coincided for the
eight top-level packages and stopped coinciding exactly where it mattered:
`airflow/` and `scripts/` hold no `__init__.py`, so they held no rule — and 26
SQL sites, 22 of them in Plan 125's Iceberg and Spark tooling, which Gates C
and D productionize.

**The fix is deliberately not an `__init__.py`.** `service_packages()` drives
seven rules — the layer-home mapping, the hidden-route check, route coverage
which imports `<service>.app`, the "enough" table's rows in both directions,
and the coverage `source` list. Making `scripts` a package would demand an
"enough" row for something that is not a service and send the route rule
looking for `scripts.app`. The contract already said so in
`test_every_service_directory_is_in_the_coverage_source`. So
`production_python_files()` is a second derivation, reading Stage G's declared
bucket table, and `scripts/oneoff/` is excluded because that table declares it
spent.

The lesson generalises past this stage and is worth stating once rather than a
fourth time: **a denominator that is listed, or scoped to what exists when it
is written, will be wrong.** G14 was undercounted at 54, G5 at 10, the scan
surface at eight packages, and `executemany` was left out of the name set
because it matched nothing that day. The rules that have never been wrong are
the derived ones.

#### 18 files were never uncovered, and the ruler was the problem

Every `archiver/sql/lake_snapshot_selectors/` file is executed in CI against
real Parquet in MinIO by `tests/integration/archiver/`, whose
`test_all_selectors_run_without_error` asserts the entire registry runs clean.
The rule read only `tests/integration/sql/` and reported them absent — tests
stronger than the check's own weak reading, called nothing at all.

The suites it reads are now declared in `docs/TESTING.md` and derived here,
matching `DORMANT_SUITES` and `script_buckets()`. **Deliberately not a glob:**
measured on 2026-09-01, reading all of `tests/integration/` would have credited
35 of the then-46 files on a name match alone, several from suites that mention
a statement without running it.

`cooldown_events.sql` needed a real repair rather than the widening. Five
selectors share it through `sql_template`, so no test contained the token even
though all five execute it — and it was the file Stage F caught being credited
to an unrelated *table name* under the old substring reading. The indirection
is asserted now instead: every selector `.sql` file is some runnable selector's
template, and none names a template with no file.

#### One file left the census, under G16's rule

`processing/sql/get_active_search_configs.sql` read `params -> 'makes'` and
`params -> 'models'` out of `search_configs` jsonb "for carousel make/model
filtering", in its own words. That filtering still happens — `detail_writer.py`
has a section header that still reads *Carousel search_config filtering* — but
it reads `ops.tracked_models` joined to enabled configs, a normalised
`(search_key, make, model)` grain, cached where the old one was not. Same
question, same consumer, same `enabled = true` gate, different source. The
superseded file had been dead since Plan 93 shipped it: no constant loaded it,
and `git log -S` finds only the contract test that waived it.

This is G16's first case, and recording *which* of the two ways a file may
leave `production_sql_files()` this was is the entire point of that rule. A
denominator may shrink; it may not shrink silently, and "nothing references it"
is not on its own a reason — the reason is that something else does the work
and can be pointed at.

#### A guard the instrument itself needed

`duckdb_con` skipped when `DUCKDB_PATH` was absent, and **55 Layer 2 tests take
it** — every dashboard query and both analytics snapshots. A path that quietly
went missing, or a dbt build that produced no file, would have skipped a
quarter of the suite and left the step green. The pattern for fixing it was one
fixture away: `airflow_metadata` has failed rather than skipped under
`REQUIRE_AIRFLOW_SCHEMA` since Stage D.

`REQUIRE_DUCKDB` closes that fixture and `REQUIRE_LAYER_2_EXECUTION` closes the
class — any skip in the suite fails the run. Both were verified in both
directions before shipping, which is the only way a guard is worth having: with
the flag set, 25 skipped tests exit 1 and name their reason; without it, the
same run exits 0 and stays a local convenience.

**CI's final run is what makes the 242 meaningful.** `242 passed` with
`REQUIRE_LAYER_2_EXECUTION=1`, `REQUIRE_DUCKDB=1` and `REQUIRE_AIRFLOW_SCHEMA=1`
all set — zero skips under that guard is proof of execution rather than of
collection.

#### What was deliberately not done

- **15 G5 sites remain**, all in Plan 112 and Plan 125 audit and parity scripts.
  Extracting them creates `scripts/sql/*.sql` files that immediately owe a
  Layer 2 test, and several run against Spark and Iceberg — engines the Layer 2
  job has none of. That would grow a list this plan only lets shrink, so they
  stay waived until there is somewhere for their tests to run.
- **`cancel_coordination_state.sql` and `release_deploy_coordination.sql` are
  textually identical and stay two files.** They are two policies that agree
  today — cancel refuses anything past `draining`, the deploy facade releases
  unconditionally — and both rules live in the Python around the statements.
  Consolidating would couple two policies allowed to diverge. This is the
  weakest of the three "kept apart on purpose" decisions and is recorded as a
  waiver rather than a comment, because a waiver is checked.
- **The engine-binding check was scoped and not built.** Which engine a `.sql`
  file targets is only worth asserting once
  [Plan 125 Gate D2](../plans/plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration)
  has chosen a serving pattern: under "DuckDB as a non-authoritative Iceberg
  cache" it is a no-op for all 26 affected files. It sits in Stage S with the
  execution recorder.

#### The deploy, and the failure it was watched for

Deployed to production 2026-09-01. The risk this change carried was never the
SQL — every statement was verified byte-faithful on normalised whitespace and
242 Layer 2 tests execute them in CI — but that `load_query` reads at **import
time**. A service started against an image built before `shared/sql/` existed
does not degrade; it fails to start. `shared/compression.py` importing
`shared.queries` at module scope put `scraper` in that class too.

Five services were rebuilt and recreated in two commands, `ops` last, because
`redeploy.sh` requests and releases deploy coordination through the *running*
ops container and this stage rewrote every statement on that path. Deploying it
alongside the others would have run the release path on new code with six
services already mutated. Both commands exited 0; all five reached `healthy`
(four in 7s, `ops` in 5s), and neither printed the "kept the same container"
note that would have meant no new image was applied.

The import-time failure did not occur, and that was checked in the containers
rather than in the tree: `shared/sql/` holds 7 files inside each of the five,
`import shared.queries` succeeds in each, and a log scan for
`ModuleNotFoundError`, `FileNotFoundError`, `query_loader`, `queries.py` and
`Traceback` across all five returned nothing.

Two services the deploy runbook named were deliberately not passed to
`redeploy.sh`. `snapshot-worker` and `april-processor` are `profiles:`-gated
`docker compose run --rm` targets; naming a profile-gated service on the CLI
enables its profile, so `docker compose up -d --no-deps` would have started
them as long-lived containers. Both share an image tag with a service that was
rebuilt — `cartracker-archiver` and `cartracker-processing` — so their next
invocation loads the new files without being named.

Airflow needed no rebuild, as the stage assumed: `./airflow/sql` is a
*directory* bind mount, so `git pull` made `record_gate_observation.sql` visible
to the dag-processor at once, and `airflow dags list-import-errors` returned no
rows.

Coordination released cleanly — generation 59, `phase: none`, deploy intent
`none`. **`/deploy/start` returned no 503**, which is the negative result
Stage K predicts rather than a contradiction of it: every service in this
deploy maps to at least one surface, and `dashboard` and `pgadmin` — the only
two that do not — were not in the set.

#### Cost

Estimate 2 points, actual 1. The stage was the plan's largest by file count —
18 commits, 118 files, +3,300/−650 — and cost less than its estimate because
almost all of it was mechanical once the rules existed. **Building the checker
first is what made it cheap**, and it is the reusable lesson: the rule found
`maintenance.py:152`, `admin.py`'s six, and the byte-identical trio, none of
which a reading pass had found in three prior stages of looking at these files.
