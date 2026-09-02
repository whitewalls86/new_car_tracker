# Plan 162: The Testing Census and CI Restructure

## Status

**Stages 0–7 are complete except 6c. Stage 6c is next, then Stage 8** — which
was narrowed on 2026-09-02, before it started: G7 is now the dashboard's
assertionless Layer 2 suite, and the Streamlit Python it used to mean is G18
and Plan 150's. [Why](#stage-8-narrowed-and-g7-now-names-a-different-gap).
The census enumerated
the work; Stage 1 ran the 73 tests nothing
had ever invoked and found no production defects behind them, which
[confirms the L estimate](#evidence--stage-1-the-orphaned-suites-car-45-2026-08-31);
Stage 2 [unblinded the coverage instrument](#evidence--stage-2-unblinding-coverage-car-46-2026-08-31)
the later stages are graded by, taking the reported number from 88% to 75.95%
without a line of production code changing; Stage 3
[gave the two health-sensor censuses one declared source](#evidence--stage-3-one-declared-source-for-the-health-sensor-censuses-car-47-2026-08-31)
and found a third census, `DAG_SPECS`, already one DAG short; Stage 4
[split the 267s dbt job](#evidence--stage-4-splitting-the-267s-dbt-job-car-48-2026-09-01)
and took CI's wall clock from 292s to about 155s, most of it by answering Plan 139
Stage C's question — the 92s step was 21 Python interpreters starting, not 21
dbt builds running; and Stage 5
[swept the 34 mock conversions and the 16 layer renames](#evidence--stage-5-the-mechanical-sweeps-car-49-2026-09-01),
closing G4, G11 and G13; and Stage 6b
[mechanised the encoding-sensitive I/O guard](#evidence--stage-6b-mechanising-the-encoding-sensitive-io-guard-car-60-2026-09-01),
fixing 234 sites across three shapes and closing G13's class with a rule that
fails on the exact call ruff cannot see. **The list stood at 68 after Stage 5 and stands at 56
now**, down from 116: Stage 5 deleted exactly the 50 it was scoped to and added
2 back, having found
[the Layer 2 check crediting files by substring](#the-instrument-was-weaker-than-its-own-docstring),
and Stage 6 emptied the 12 route waivers. Neither Stage 3 nor Stage 4 closes
waivers; Stage 3 closes Plan 139's Stage H and Stage 4 its Stages B and C.
Stage 6b closes none either — it adds a rule whose list starts empty.

**Every count in this section is the number an instrument reports, not a
number this document remembers.** The 68 above survived here for a day after
Stage 6 made it 56, and was caught during Stage 6b's closeout by importing the
waiver tuples rather than reading this paragraph. That is this plan's own
subject matter happening to this plan, and it is left on the record rather than
quietly corrected.

This document was written as a deliberate stub on 2026-08-30, when
[Plan 161](plan_161_testing_contract.md) had not yet decided the standard this
plan measures against. That blocker is gone: 161's contract landed, was
asserted, and is archived.

Stages 2 through 10, including 5b, 6b and 6c, are scoped below and unblocked. Effort is
**L**, down from the XL placeholder, on the reasoning in
[The estimate](#the-estimate) and now confirmed against a measurement rather
than proposed. [`docs/PLANS.md`](../PLANS.md) owns priority and effort; this
document does not choose them.

## What the census found

[`tests/test_testing_contract.py`](../../tests/test_testing_contract.py)
implemented seven mechanical rules when the census ran, and eight since Stage 2
added the coverage rule. It passes, and **a pass means only that those rules
hold** — every violation standing on 2026-08-31 is grandfathered in a waiver
list. That list is this plan's backlog:

This is the census as taken, kept as the baseline the stages are measured
against; the live count is whatever `tests/test_testing_contract.py` holds
today. **Stage 1 has since cleared the CI-invocation row and Stage 5 the
mocker and layer-numbering rows. Stage 5 also corrected the Layer 2 row
upward, from 54 to 56 — see
[the instrument note](#the-instrument-was-weaker-than-its-own-docstring).
**The live total is 56.**

| Rule | Waivers | Gap |
|---|---|---|
| CI invocation | 4 → **0** | [G1](../TESTING.md#the-gap-list) (3), G2 (1) — both closed by Stage 1 |
| Patching is `mocker` | 34 → **0** | G4 — closed by Stage 5 |
| Route reached through `app.routes` | 12 | G6 |
| `.sql` file touched by a Layer 2 test | 54 → **56** | G14 — the census undercounted; see Stage 5 |
| Layer numbering | 16 → **0** | G11 — closed by Stage 5 |
| **Total** | **120** | as measured; 122 on the corrected reading |

The waiver list can only shrink, and three assertions enforce that: a waiver
that no longer describes a violation fails, one whose owner plan is archived
fails, and one naming a gap entry that does not exist fails. **So this plan's
progress is mechanically visible and its completion is provable** — which is
what makes it schedulable rather than open-ended.

### Measurement beat inspection, and the record should say so

The readings this document carried as its starting point were taken by eye on
2026-08-30. Every one of them was wrong, and wrong in the same direction:

| Read by eye, 2026-08-30 | Measured, 2026-08-31 |
|---|---|
| 21 files mixing two mock styles | **34** files patch with something other than `mocker` — the by-eye count never looked at `monkeypatch.setattr` |
| 3 of 35 routes unreferenced | **12 of 87** routes reached through no routing table |
| 16 modules with inline SQL | **10** modules (G5) — and a gap nobody had counted: **54 of 76** `.sql` files that no Layer 2 test executes (G14). Stage 5 later corrected this to 56 |

The direction of the error is the point. Inspection undercounted three times out
of three, and the one gap that inspection missed entirely, G14, is now the
largest single item in the plan.

### Six gaps have no mechanism at all

Six of the thirteen gaps this plan owns are checked by nothing: **G5, G7, G8,
G9, G10 and G12.** (Twelve at the census; **G13 was re-owned here on
2026-08-31** when its Plan 146 half shipped, and it is half-checked — the
`PYTHONPATH` clause is asserted and nothing else is. **G10 has since been
mechanised and closed** by Stage 2 the same day, leaving five.) They are
recorded in prose, they are not among the 120, and they can worsen without
anything noticing. That is the condition
`ARCHITECTURE.md:179` was in before Plan 161, and it is why this plan's success
criteria are written the way they are below.

Three sub-cases, because they are not alike and the differences drive the stage
order:

- **G5, G9 and G10 have a natural, cheap assertion.** G10's was estimated at
  roughly five lines — every service directory appears in
  `[tool.coverage.run] source` — and Stage 2 built it, at that size plus a
  second assertion for the half the estimate had not counted.
- **G7 could never be reached by the existing rules, and that is why it was the
  wrong gap.** `dashboard/` is Streamlit, not FastAPI. The route rule imports
  `<service>.app` and reads its OpenAPI schema; there is no schema to read. The
  "enough" floor's first clause is structurally inapplicable to the one service
  with zero test files. **Rescoped 2026-09-02** — G7 is now the dashboard's
  Layer 2 suite asserting nothing, which is reachable, cheap and this plan's;
  the Python that needed a test invented is G18 and belongs to Plan 150. See
  [Stage 8 narrowed](#stage-8-narrowed-and-g7-now-names-a-different-gap).
- **G12 may correctly never get a rule.** "No module under `airflow/dags`
  imports `shared`" is *true today* — it is the constraint, not the violation.
  Closing it changes the DAG tree's import structure, which is an architecture
  decision and not an assertion.

## The stages

| Stage | Work | Closes | Waivers |
|---|---|---|---|
| **0** | **The census. Complete — CAR-40, 2026-08-31** | — | — |
| **1** | **The orphaned suites. Complete — CAR-45, 2026-08-31** | G1, G2 | 4 |
| **2** | **Unblind coverage. Complete — CAR-46, 2026-08-31** | G10 | -- |
| **3** | **The two health-sensor censuses read one declared source. Complete — CAR-47, 2026-08-31** | Plan 139 Stage H | -- |
| **4** | **Split the 267s `dbt build + test` job. Complete — CAR-48, 2026-09-01** | Plan 139 Stages B, C | -- |
| **5** | **The mechanical sweeps. Complete — CAR-49, 2026-09-01** | G4, G11, G13 | 50 |
| **5b** | **Separate production scripts from spent ones. Complete — CAR-55, 2026-09-01** | — | -- |
| **6** | **Route coverage, and `container_health`'s test home. Complete — CAR-50, 2026-09-01** | G6, G9 | 12 |
| **6b** | **Encoding-sensitive I/O, mechanised. Complete — CAR-60, 2026-09-01** | G13's class | 0 |
| **6c** | Every service contract produces an intent row the database accepts | -- | 0 |
| **7** | **SQL execution, from both directions. Complete — CAR-51, 2026-09-01** | G14; G5 to 15 | 56 |
| **8** | `scraper`'s floor, and the Layer 2 suite that asserts nothing | G7, G8 | -- |
| **9** | `airflow/dags` and the `.sql` convention it cannot currently reach | G12 | -- |
| **10** | Suites on real Compose services, dbt against the Plan 120 snapshot, advisory CI impact selection | Plan 139 Stage E | -- |
| **11** | The dbt testing contract, and what leaves the SQL census | G16 | -- |
| **12** | Shared fixtures: what the suite duplicates now that it is 3,988 tests | -- | -- |

**4 + 50 + 12 + 56 = 122.** The stages account for the whole waiver list; no
entry is left without a stage that deletes it. Stage 7 later raised its own
column from 56 to 66 + 23 across two new rules — see [Stage 7 grew two
gaps](#stage-7-grew-two-gaps-while-closing-one), and note that a stage
discovering more than it was scoped for is the instrument working, not the
arithmetic failing.

### Why this order

Four of the placements are load-bearing. The rest is grouping.

**Stage 1 is first of the remaining stages because it is the only unknown that
changes the estimate.** 73 integration-marked tests sit in 11 files that no CI
step has ever invoked; `tests/integration/processing/` — 58 of them — has never
appeared in `ci.yml` in its history. **Whether they still pass is unknown**, and
every other stage can be sized from measurements Stage 0 already took. If those
suites have rotted, the areas they cover are unexercised and Stages 7 and 8 both
get worse. Running them is cheap, is a repair in its own right, and is what
converts this plan's estimate from a proposal into a measured number.

**Stage 2 is second because coverage is the instrument the rest of the work
reads.** `[tool.coverage.run] source` names six packages and omits
`container_health`, `dashboard`, `scripts` and `airflow/dags` — so **the two
services furthest below the floor are the two the instrument cannot see.** Every
stage behind this one measures better for it being fixed first.

**Stage 4 sits after Stage 1, not before it.** Stage 1 changes which suites
exist in CI; splitting the job afterwards means organising once with full
knowledge rather than twice. The rest of the restructure stays at Stage 10,
where its risk belongs — but the job split itself is largely mechanical, and
leaving it until last would mean running the most CI-intensive work this
repository has attempted across weeks of a 267-second critical path we had
already decided to remove.

**Stages 6 and 7 each pair two gaps because splitting them means touching the
same files twice.** G9 builds `container_health` a test directory and a Layer 4
that do not exist; G6's four `container_health` routes are uncoverable until it
does. G5 moves inline SQL into `.sql` files and G14 gets Layer 2 executing
`.sql` files — the same modules, from opposite ends.

**G13 joined Stage 5 on 2026-08-31, for the same reason.** It is the thirteenth
gap and the only one this plan did not originally own: the contract assigned it
to Plan 146 Stage 1 for the `PYTHONPATH` half, that half shipped as CAR-42, and
the remaining instance was left owned by a plan that owes no code. Stage 5 is
the right home because it is already the pass that reads every patch in the
suite — and "an unexplained mock of a filesystem, clock, platform or path
primitive is a finding" is the same question asked one step further out. The
pattern the rule holds up as correct, `21333ab`, is itself a mocking fix. Doing
the two together is one reading of the suite instead of two.

**Stage 5b sits immediately after Stage 5 because Stage 5 is what makes it
free.** The first instinct was to run it near the front, so the stages that
follow would visibly move the coverage number. The waiver list forbids it. Ten
of Stage 5's 34 mocker waivers name files under `tests/scripts/` and
`tests/integration/scripts/`, character for character, and `_assert_exactly`
asserts both directions — so moving a test file breaks its waiver twice, once
because the old subject has stopped existing and once because the new path is
an unwaived violation. Running 5b first means rewriting ten waiver subjects
that Stage 5 then deletes outright. Running it second costs nothing, because
Stage 5 has already emptied the colliding set. No other waiver tuple names a
`scripts/` path — `LAYER_2_WAIVERS` and `LAYER_NUMBER_WAIVERS` have none, and
`CI_INVOCATION_WAIVERS` is empty since Stage 1 — so Stage 5 is the only
collision in the plan.

Nothing is lost by the delay. **Stages 6, 7, 8 and 9 are all downstream of
Stage 5**, so every remaining stage that moves the number is still graded
against the cleaned denominator, which was the whole point of going early. The
CI payoff is not delayed either: Stage 4 is the dbt job split, and the impact
selector that reads the new prefix is Stage 10.

**It is numbered 5b rather than 6.** Inserting an integer renumbers five stages
and invalidates two issues already filed against the old numbers — CAR-50 names
Stage 6 and CAR-52 names Stage 8 in their titles. A letter costs nothing and
breaks nothing.

### Stage 7 grew two gaps while closing one

**Added 2026-09-01, mid-stage.** Stage 7 was scoped at 56 Layer 2 waivers and
"G5's ten modules". Both numbers were wrong, and both were wrong the same way
the census was wrong about G14: **the measure was fitted to the code in front
of it.**

G5 measured 66 sites in 15 modules, not 10. The gap list's own stated measure —
"`.execute(` with a literal first argument" — cannot see
`execute_values(cur, sql, rows)`, which carries its statement second, and
`ops/routers/maintenance.py:152` is a literal `INSERT` sitting exactly there.
Two of the named ten did not belong: `shared/db.py`'s only match is inside
`db_cursor`'s docstring, and `shared/duckdb_s3.py`'s seven are `INSTALL` /
`LOAD` / `SET` session setup, which name no schema and so cannot drift from
one. Closed the same day; `INLINE_SQL_WAIVERS` is `()`.

**G15 is what closing G5 revealed.** A statement bound to a name and executed
from there is invisible to both instruments at once: Rule 5b does not fire
because it is not at the call site, and Rule 5's denominator cannot count it
because there is no `.sql` file. Stage 7 extracted six of these by hand and
only because someone happened to read the files; the measured cost of that
blind spot was 23 more in 11 modules, six of them in `ops/routers/admin.py`, a
router the stage never touched precisely because every one of its statements is
assigned before it is executed.

**The scan surface was the third instance of the same error.** Both new rules
scan `service_packages()`, which is the right predicate for "what is a service"
and the wrong one for "what is production Python". `airflow/` and `scripts/`
hold neither an `__init__.py` nor, therefore, any rule — and they hold 26 more
sites, 22 of them in Plan 125's Iceberg and Spark scripts, which Gates C and D
productionize. The repair is a second derivation reading Stage 5b's declared
bucket table, **not** an `__init__.py`: `service_packages()` drives seven rules,
and making `scripts` a package would demand an "enough" row for something that
is not a service and send the route rule looking for `scripts.app`.

The lesson is the one this plan keeps relearning about its own instruments, and
it is worth stating as a design rule rather than a third anecdote: **a
denominator that is listed, or scoped to what exists when it is written, will
be wrong.** G14 was undercounted at 54, G5 at 10, the scan surface at eight
packages, and `executemany` was left out because it matched nothing that day.
The rules that have never been wrong are the derived ones —
`service_packages()`, `_test_directories()`, `production_sql_files()`.

### Stage 8 narrowed, and G7 now names a different gap

**Rescoped 2026-09-02, before the stage started.** Stage 8 was "the services
below the floor", G7 and G8 together, and G7 was "`dashboard/`: 7 modules, 0
test files". Reading the service settled that G7 as written is not this plan's
work, and that a better gap was sitting underneath it unnamed.

**What the dashboard's Python actually contains.** 483 lines under `pages/`,
of which roughly 430 are `st.*` and `px.*` presentation calls and roughly 35
are logic — a filter-clause builder, three division-by-zero guards and two
granularity maps. Read end to end, it holds no defect: `app.py:29`'s unguarded
`.iloc[0]` is safe because `mart_freshness.sql` is a bare aggregate that always
returns one row, and the filter builder's placeholder/parameter pairing is
correct and locally coupled. What is there is redundancy — a tautological
`elif`, two different zero-guards for one metric, one granularity map that
round-trips through an intermediate encoding to reach what the other produces
directly. None of it is worth a test, and testing the other 430 lines means
asserting that `st.metric` was called with a particular string, which fails on a
renamed header and never on a wrong number.

**The gap that was underneath it.** `tests/integration/sql/test_dashboard_queries.py`
is 25 tests with **zero assertions** — the only Layer 2 suite with none, against
116 in `test_ops_queries.py` and 65 in `test_processing_queries.py`. Every test
executes a statement and discards the result. That satisfies Layer 2's first
clause and not its second: the contract says statements execute *"and return the
columns the caller expects"*, and nothing here checks a column. Every page
indexes by name, so a renamed mart column passes green and `KeyError`s in
production. **The pattern that closes it already exists in the same directory,
against the same `duckdb_con` fixture** — `test_analytics_snapshot_queries.py`
asserts `result.description` against a declared column tuple. `dbt_runner` has
that contract; `dashboard` does not.

It pays a second dividend on the way. Writing down the columns the caller uses
surfaces the ones it does not: `data_health_block_rate.sql` selects
`block_rate_pct`, `total_block_events` and `max_attempts_seen`, and
`data_health.py` reads none of the three — it recomputes the rate in pandas
after aggregating, correctly, because percentages do not average. Three dead
columns in production SQL that nothing can currently see.

**So G7 is redefined rather than deferred**, and the Python becomes **G18, owned
by Plan 150**. Two rows, not one, because they are two gaps with two owners and
two triggers, and folding either into the other hides one of them. Dropping the
Python row entirely was the alternative and it is the Plan 84 move exactly:
`docs/TESTING.md`'s Testing Strategy section was accurate the day it was written
too. G18 states the blocker in the row — the suite cannot import
`dashboard.pages.*` at all, because `streamlit` and `plotly` are declared in
`dashboard/requirements.txt` and nowhere else and production imports are bare —
so the row records a measurement rather than an intention.

**What this plan is not taking on, stated so it is a decision.** Asserting that
the dashboard's queries return *correct values* is Layer 3's shape — known
inputs, known outputs — not Layer 4's, and Stage 8 does not attempt it. Three
reasons, in order of weight:

1. **The correctness is already asserted where the data is made.** All eight
   marts the dashboard reads carry dbt unit tests with seeded inputs and
   expected rows in `dbt/models/marts/unit_tests.yml`, plus `not_null` and
   `unique` data tests in their `.schema.yml` files.
2. **Most of the SQL adds nothing to assert.** 24 files, 218 lines, averaging
   nine. Scanned for `JOIN|CASE|COALESCE|NULLIF|GROUP BY`, 13 add none of them
   — `SELECT COUNT(*) AS cnt FROM mart_deal_scores` and its kin — 10 add a
   `GROUP BY` over a tested mart, and one carries real logic.
3. **The fixture cannot do it.** `duckdb_con` is a read-only connection to
   whatever `dbt build --target duckdb` produced from CI's seed. There is no
   seeding, so there are no known inputs. Layer 3's `seed_and_build` has that
   machinery and runs `--target ci` into **Postgres**, so borrowing it means
   executing DuckDB-dialect SQL against another engine — `now() - INTERVAL '14
   days'` and the rest — which is a dialect question this repository already has
   scar tissue about.

**The one query where that reasoning does not hold is recorded rather than
built.** `data_health_block_rate.sql` LEFT JOINs `mart_block_rate` onto an
anchor of `mart_scrape_volume` with `COALESCE(..., 0)`, so hours with scrape
activity and no blocks read as zero rather than as gaps. `mart_block_rate.schema.yml`'s
own description says *"Join to mart_scrape_volume on hour to compute block rate
against observations"* — **the mart deliberately hands that join to its
consumer**, which is the one place mart-level unit testing structurally cannot
reach, and neither end of the handoff is tested. The cheap answer is probably
not a new fixture at all but moving the join into the mart, where Layer 3
already reaches it. That is a modeling change, it is what Plan 150 Stage 0c
means by *"served by extending an existing mart"*, and it is noted there rather
than done here.

### Stage 12 exists because this plan grew the suite

**Added 2026-09-01, at the maintainer's suggestion, during Stage 7.** Plan 162
has spent nine stages adding tests -- Stage 1 put 73 orphaned ones into CI,
Stage 6 added Layer 4 for `container_health`, and Stage 7 alone took Layer 2
from 129 tests to 237. Nothing has yet looked at what that growth duplicated.

Measured on 2026-09-01, before the stage starts:

| | |
|---|---|
| Tests collected | 3,988 |
| Shared fixtures serving them | 11 |
| Ad-hoc `INSERT` statements inside test modules | 96 |
| Module-local seed helpers | 55 |
| Distinct read-back `SELECT`s written in tests | 161 |
| …of those, written more than once | **43** |
| Total retypings of those 43 | **145** |

**The duplication is by name, not merely by shape.** `_insert_artifact` is
defined separately in `tests/integration/ops/test_maintenance.py`,
`tests/integration/sql/test_ops_views.py` and
`tests/integration/sql/test_processing_queries.py`; `_insert_detail_claim` in
two files; `_seed` in three archiver modules; `_make_tar_zst` in three script
modules.

**And the assertions duplicate worse than the seeds.**
`SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid`
is written out **17 times**, across `tests/integration/processing/` and
`tests/integration/sql/test_ops_views.py`;
`SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid`
nine times; `SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid` six.

**These must not become `.sql` files.** A read-back assertion is the test's own
half of the work, not a statement production issues, and `shared/sql/` feeds
`production_sql_files()` -- filing one there would demand a Layer 2 test *for
an assertion*, which is circular, and would inflate the production census with
statements no service runs. The repair is a shared *test* helper, which is why
this is its own stage and not a continuation of Stage 7.

**Why this is the same defect as G17 and not a tidiness exercise.** A seed
`INSERT` written by hand inside a test is a statement that has to agree with
the schema, and there are 96 of them with no single definition. A column rename
means finding all 96, and nothing notices when 95 are found -- the surviving
one keeps passing against whatever it seeds. `tests/` is exempt from the Layer 2
census by design, and correctly so, because fixture seeds are not production
SQL. That exemption is what makes this invisible: the rules this plan built all
stop at the tests' own door.

**Two things the stage must not do**, stated now so they are decisions rather
than discoveries. It must not consolidate a fixture whose two callers want
different data -- a shared seed that grows parameters until it can serve
everyone is harder to read than the two helpers it replaced, and a test whose
setup lives three files away is worse at explaining its own failure. And it
must not touch `tests/scripts/oneoff/`, which Stage 5b declared spent: those
helpers duplicate each other freely and should, because the plans that own them
have archived.

**The instrument this stage needs does not exist yet**, which is why it is
scoped after Stage 11 rather than before it. "Two helpers do the same thing" is
not a textual property -- `_seed` and `_insert_queue_row` may be identical in
effect and share no token -- so unlike G5, G15 and G17 there is no cheap
derived check waiting to be written. The stage should say plainly whether it
found one or whether it leaves prose behind, per success criterion 2.

### Stage 11 answers a question Plan 161 did not ask

**Added 2026-09-01.** [Plan 161](plan_161_testing_contract.md) asked what a
*service* owes before it ships and keyed the answer to a Python package. The
dbt project is not one: `dbt/` is a Dockerfile, SQL and YAML, `dbt_runner` —
the service that invokes dbt — has the "enough" row, and the 22 models it
builds have none. `test_every_service_directory_has_a_row_in_the_enough_table`
asserts the table equals `service_packages()` in both directions, so **adding a
`dbt` row today fails as a phantom.** The obligation is not unmet; it is
inexpressible.

What that costs, measured: **17 of 22 models have a dbt unit test and five do
not**, and no rule requires one. `tests/dbt/` asserts every model carries a
cadence tag, so the only obligation this repository mechanically enforces on a
model is a *scheduling* one. A new mart with no test ships green.

**Two things make this urgent rather than tidy.** The first is that
`_SQL_EXEMPT_ROOTS` exempts `dbt/` from the Layer 2 census by design — correct,
because Layer 3 is dbt's instrument — so a `.sql` file whose logic moves into a
mart leaves a counted surface for an uncounted one, and **the count drops for
something that is not a repair.** That is the same failure as Stage 5's
substring bug: the list shrinking for free. The second is that
[Plan 125](plan_125_duckdb_to_iceberg_migration.md) absorbed Plan 118 and moves
the analytics layer onto Spark/Iceberg, so that migration is not hypothetical —
it is the plan of record.

Stage 11 therefore owns three things:

1. **What the dbt project owes**, and a mechanism that can hold it — the
   "enough" table's derivation admits a non-package surface, or a second table
   does.
2. **G16: a `.sql` file may only leave `production_sql_files()` by naming the
   dbt model that absorbed it.** The denominator may shrink; it may not shrink
   silently.
3. **The execution recorder, scoped here and built later.** Recording *what
   text executed against which engine* is the only mechanism that closes the
   remaining class at once: it kills the paraphrase, the weak name-match
   reading, and a statement whose test executes it against an engine production
   no longer uses. Two hard parts are already known — `.format()` templates
   record rendered but are stored as templates, and the suites run in separate
   CI jobs so aggregation needs an artifact and a gate job.

**The recorder splits along a seam worth respecting.** *Capture* is
engine-local, cheap, and worth doing while DuckDB is still authoritative,
because a baseline taken after [Plan 125 Gate D](plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration)
is not a baseline. The *cross-engine assertion* — "this ran on the engine
production uses for it" — needs two live engines to design honestly and belongs
at that gate. Building both against one live engine and one hypothetical would
fit the design to what exists today, which is the error recorded two sections
above.

**One exposure number here is conditional and should not be quoted flat.** 26
`.sql` files are covered only by a DuckDB-bound test — every `dashboard/sql/*`
plus both `dbt_runner/sql/*` snapshots. Whether they move engines at all is
decided by Plan 125 Gate D2: under "serving extracts from Iceberg" they do,
under "DuckDB as a non-authoritative Iceberg reader/cache" — which that plan
currently calls the lower-risk first cut — they do not, and `duckdb_con`
remains the correct fixture.

### Stage 6b was added by the failure this plan predicted

**Added 2026-09-01.** Success criterion 2 records G13 as the weakest of its
three exceptions, and says why in a sentence worth reading back: *"the next
instance of G13's class will be found the way the last two were, by someone
running the suite somewhere CI does not."* That is precisely what happened, six
days later and while Stage 6 was being started.

`tests/scripts/test_build_public_roadmap.py` writes a synthetic plan document
containing an em-dash with `write_text` and no `encoding=`. The locale decides:
UTF-8 on Linux, cp1252 on Windows, where the character becomes the byte `0x97`.
`build_public_roadmap._first_heading` reads it back as UTF-8 — correctly — and
raises. **The suite was green in CI and red on a developer machine**, which is
the benign direction of the harness rule and the same shape as `21333ab`.

**What makes it a stage rather than a second one-file repair is that four
independent guards were in a position to catch it and none could.** Measured
on 2026-09-01:

1. **No encoding rule is configured.** `[tool.ruff.lint] select` is
   `["E", "F", "I"]` — nothing that reads an encoding argument.
2. **The rule that would is preview-gated.** `--select PLW1514` alone answers
   *"Selection `PLW1514` has no effect because preview is not enabled"*, so it
   is off twice over and silently.
3. **Enabling it fully would still not have caught this.** With
   `--select PLW1514 --preview` the repository has **22 violations and not one
   of them is the line that broke master.** The rule fires only on a
   directly-constructed receiver: `Path("b.md").write_text(...)` is flagged,
   `(tmp_path / "a.md").write_text(...)` is not — with or without a `Path`
   annotation on the fixture. It is blind to the idiom nearly every
   fixture-writing test in this repository uses.
4. **CI is `ubuntu-latest` in all ten jobs**, so this failure direction is
   invisible by construction — the constraint G13's exception already named.

The near-miss is the instructive part. *The harness must not decide the
outcome* is written for exactly this class and even carries a Windows example,
but its checkable rule is about **mocks** of filesystem, clock, platform or
path primitives. A missing `encoding=` is not a mock, so Stage 5's sweep — the
pass that read every patch in the suite — went straight past it. The prose
covered this; no mechanism could.

**So the stage is not "turn on the ruff rule".** Finding 3 is the whole reason
it needs designing: the available tool cleans 22 real sites, several in
production code (`ops/routers/admin.py`, `dbt_runner/app.py`, three
`archiver/processors/` modules), and still would not have stopped the defect
that prompted it. Closing the class means a rule that reads the calls the way
the route rule reads request literals, a Windows job, or an argued case that
neither is worth it — recorded as a decision either way.

**It sits after Stage 6 and before Stage 7.** After 6 because CAR-50 is already
in flight and re-cutting it buys nothing. Before 7 because **Stages 7, 8 and 9
author more new tests than the rest of the plan combined**, and a guard that
lands first is one those stages get for free rather than one that has to sweep
what they wrote. That is the same argument that put Stage 2 ahead of the stages
it measures.

**The letter is positional, not topical.** 6b has nothing to do with route
coverage; it is numbered this way for the reason 5b was, and CAR-52 still names
Stage 8.

### Stage 6c was added by a deploy, not by the suite

**Added 2026-09-01.** Stage 6b was added by a failure this plan predicted. This
one was added by a failure it did not, found during Plan 138 Stage 2's
production deploy — and the shape is the reason it belongs here rather than in
Plan 138.

`POST /deploy/start` with `{"targets":["dashboard"]}` returns **503
`{"detail":"Database unavailable."}`**. Postgres was healthy throughout. The
database was never the problem.

`ops/coordination_contract.py` maps `dashboard` and `pgadmin` to `frozenset()`
— they are the only two services in `SERVICE_CONTRACTS` with **no surfaces**.
`_set_intent` therefore writes `phase='requested'`, `targets='["dashboard"]'`
and `scope='[]'`, against a constraint that forbids exactly that pair
(`db/migrations/V043__coordination_state.sql:27`):

```sql
CHECK (
    (phase =  'none' AND kind IS NULL     AND targets =  '[]'::jsonb AND scope =  '[]'::jsonb)
    OR
    (phase <> 'none' AND kind IS NOT NULL AND targets <> '[]'::jsonb AND scope <> '[]'::jsonb)
)
```

**So two services can never be deployed alone**, and the failure is structural
rather than intermittent. The workaround is to name a scoped service in the
same command — `bash scripts/redeploy.sh ops dashboard` — because the union is
then non-empty. That is a real property of `redeploy.sh`, which takes a service
list, and it is what unblocked the deploy.

**Three guards were in a position to catch this and none could.** Measured
2026-09-01:

1. **The contract suite never asserts the value that breaks.**
   `tests/ops/test_coordination_contract.py` exercises `expand_targets` and the
   string `scope` does not appear anywhere in the file. It asserts the mapping
   is *well-formed*, never that its output is *writable*.
2. **The constraint is in a Flyway migration, and the contract is in Python.**
   Neither half is wrong on its own; the defect exists only in their
   composition, and no layer in this repository composes them. This is the same
   division Stage 3 closed for the health-sensor censuses — two sources that
   must agree, with nothing asserting that they do.
3. **The error message actively misdirects.** `_set_intent` catches bare
   `Exception` and returns `"error"`, which `ops/routers/deploy.py:248` renders
   as 503 "Database unavailable." The constraint violation never reaches the
   response or the log, so the symptom points at the one component that was
   healthy.

**The stage is therefore two things, and the second is not optional.** An
assertion that every service in `SERVICE_CONTRACTS` yields a `(targets, scope)`
pair the constraint accepts closes the defect class. Unmasking the exception is
what stops the *next* unrelated failure in this path costing the same
diagnosis, and finding 3 is the whole reason a passing deploy script is not
sufficient evidence here.

**It sits after 6b for 6b's own reason** — Stages 8 and 9 author more new
tests than the rest of the plan combined, and this is a guard those stages get
for free rather than one that has to sweep what they wrote. **The letter is
positional, not topical**, as it was for 5b and 6b.

*Written on 2026-09-01 as "after 6b and before 7", by a deploy that did not
know Stage 7 was in flight on another branch. Stage 7 completed the same day,
so 6c gets 8 and 9 rather than 7, 8 and 9 — the argument is unchanged and the
count is not.*

#### Finding 3 was corroborated the same day, on the same endpoint

**Added 2026-09-01 while merging Stage 7.** Stage 7 broke `POST /deploy/start`
too, independently and for an unrelated reason: it moved
`set_deploy_intent.sql` into a file and wrote an explanatory comment that
quoted the statement's own placeholder, and **psycopg2 counts placeholders
across the whole string, comments included**. The statement then expected four
parameters where `deploy.py` passes three.

**The symptom was identical — 503 `Database unavailable` — and for exactly the
reason finding 3 gives.** `_set_intent` catches bare `Exception`, so a
`psycopg2` parameter error and a `CHECK` violation are indistinguishable at the
response, in the log, and to the operator. Two unrelated defects, one day
apart, wearing the same misleading face.

That is the strongest evidence this stage has for its second half, and it
arrived from outside it. **The assertion half would not have caught Stage 7's
defect** — the contract's `(targets, scope)` pair was fine — but **the
unmasking half would have named it immediately**, instead of it being found by
seven Layer 4 failures in CI and diagnosed from a log. A reader comparing the
two should not conclude they share a cause: [Stage 7's
evidence](#evidence--stage-7-sql-execution-from-both-directions-car-51-2026-09-01)
records the placeholder defect and Rule 5e, which is what stops that one
recurring; this stage owns the masking that made both of them expensive.

### Stage 5b: what the split is, and why a directory rather than a list

Three buckets, but only two moves, which is what keeps the cost near zero:

| Bucket | Where | Moves? | In the denominator? |
|---|---|---|---|
| **Production** — invoked by CI, an image, a Compose file or an ops route | `scripts/` (unchanged) | no | yes |
| **Maintenance** — human-invoked, durable, named in a live runbook | `scripts/ops/` | yes | yes |
| **One-off** — ran for a plan that has since archived | `scripts/oneoff/` | yes | **no** |

`tests/scripts/` mirrors the split and needs one new row in the contract's
*Where the newer suites sit* table, which
`test_every_test_directory_is_assigned_a_layer` will demand the moment the
directory appears.

**Production does not move, and that is the entire cost argument.** There are
20 binding references to `scripts/*` across 11 deploy surfaces — `ci.yml` (4),
`docker-compose.lakehouse.local.yml` (3), `redeploy.sh` (3),
`dbt_runner/Dockerfile` (2), `.env.example` (2), the two other lakehouse
Compose files, `.gitattributes`, `deploy-followers.txt`,
`ops/routers/snapshots.py` and `deploy.sh` — and **every one of them names a
script that stays put.** Moving only the other two buckets rewrites no deploy
surface at all.

It also gives the list the safe failure direction, the one
`maintenance-running-set.txt` already argues for in this repository: a new
script lands in production-land and is measured **by default**, and has to be
deliberately moved down to leave the instrument. Nobody drops something out of
coverage by forgetting.

**A directory rather than a manifest, because the directory is the
declaration.** A manifest would be a second mechanism to keep in step with the
first; the path is self-describing, `git log --follow` records the
reclassification, and `[tool.coverage.run]` and `ci_change_scope.py` each read
it for free. `scripts/ci_change_scope.py` is today a single-prefix classifier —
`DOCS_PREFIX = b"docs/"` and nothing else — so a changeset confined to
`scripts/oneoff/` plus its tests needs lint and its own unit tests and no
Docker build, no dbt job, no 267-second critical path. That is the first real
instance of the impact selection Stage 10 generalises, against code that
already exists.

**The classification is mechanical, and that is why this stage is small.** The
first scoping of it assumed per-file archaeology across fifteen archived plans.
It is not: **33 of the 35 Python scripts declare their owning plan in the first
three lines of the docstring**, so the bucket falls out of a join — docstring
plan number against the archived numbers in
[`completed_plans.md`](../planning/completed_plans.md), overridden by the
binding-reference grep, which wins in both directions.

**The name is never the signal, and the override is what proves it.**
`audit_adaptive_refresh_features.py` reads as forensics and is baked into
`dbt_runner/Dockerfile`; `report_dbt_run_results.py` belongs to archived Plan
123 and is in the same image. Both are production. The `audit_`, `estimate_`
and `spike_` prefixes classify nothing.

**Five scripts declare no plan and are the whole of the judgement.**
`ci_change_scope.py` is settled by its `ci.yml` reference; the remaining four —
`audit_parquet_layout.py`, `audit_normalized_parquet_layout_once.py`,
`backfill_unlisted_silver.py` and `diff_semantic_duplicate_html.py` — need
reading. That is the residual, and it is four files.

**A coupling finding that ran the other way, recorded because the first reading
of it was wrong.** Two production scripts import from scripts that look spent —
`export_volatility_features_to_iceberg.py` takes `cleanup_keys` from
`spike_iceberg_lakehouse.py`, and `train_html_dictionary.py` imports from
`estimate_dictionary_savings.py`. Scoped as "production depends on a spike" and
as this stage's hardest part. **The archive join dissolved both:** Plans 112 and
129 are not archived, so all four files stay in production-land and neither
import crosses a bucket boundary. The lesson is the one Stage 0 already
recorded — run the measurement before sizing the work it implies.

**Two constraints on `oneoff/`, stated so they are decisions rather than
drift.** Spent means *out of the ratchet's denominator* — never deleted, and
never untested: `reconcile_april_detail.py` is 84% covered **because** it
deleted 14.6 GB of production data, and its tests are why that was safe. And an
entry there should have to cite the archived plan it belongs to, so the bucket
cannot outlive its reasons the way an unchecked waiver list would.

**What it is worth, measured rather than asserted.** The `oneoff/` bucket —
archived owning plan, no binding reference — is **14 scripts, 6,338 statements
at 72%**, still over half of it `reconcile_april_detail.py` alone. Removing it
takes the denominator from 19,733 to 13,395 and the reported number from 75.91%
to **77.8%**. Stage 8's dashboard repair — 309 statements, 280 of them
currently missed — moves the total by **+1.27 points today and +1.87 after**,
so the ratchet becomes about **1.5× more responsive**. Real, and worth having
before Stages 6 through 9 are graded; nowhere near large enough to justify
paying the Stage 5 waiver collision to get it sooner. That arithmetic is why
this stage is placed on the waiver argument rather than the coverage one.

An earlier draft of this section put the bucket at 19 scripts and 7,019
statements. It was close by accident and wrong in composition: it counted
`spike_iceberg_lakehouse.py`, `run_dbt_spark.py`, `verify_dialect_datediff.py`
and both `compare_gate_*_parity.py` against Plan 125, and
`estimate_dictionary_savings.py` against Plan 129 — **six scripts belonging to
plans that are still open.** Spent is a property of the owning plan's state, not
of how finished a script looks.

### Stage 10 inherits a question Stage 5b raised and declined

**Scoped 2026-09-01, from a question asked while reviewing Stage 5b's CI
change. Recorded here rather than acted on, because it is Stage E's subject
and Stage E already has a rule for it.**

`ci_change_scope.py` classifies the **cumulative** PR diff — `base.sha` to
`head.sha` — so a documentation commit pushed onto a PR that has already gone
green re-runs the whole workflow, because the cumulative diff still contains
the production paths verified two pushes ago. Classifying the *incremental*
diff instead would skip it.

**The saving is about two minutes** — PR #325's full workflow was 127s wall
clock after Stage 4. That is the number any design here has to beat, and it is
small.

**The cost is four conditions, not one diff.** A skip is only sound if all of
them hold, and three of them are invisible when they do not:

1. **The reference commit must be verified, not merely green.** The previous
   push may itself have skipped the heavy jobs, so the reference has to be the
   most recent ancestor where they actually concluded `success` rather than
   `skipped` — an Actions API walk with `actions: read`, or a marker written
   when heavy passes and read back later.
2. **The base must not have moved.** `actions/checkout` builds
   `refs/pull/N/merge` on a `pull_request` event, so the workflow tests
   `merge(base, head)` and not `head`. If `master` advances between the
   verified run and the new push, the merged tree differs even for a
   documentation-only diff and the earlier verdict does not carry. This is the
   dangerous one: green, fast, and not verifying the tree being merged.
3. **Rebases and force-pushes must fail closed**, via an ancestry check rather
   than a SHA equality that can match a commit no longer on the branch.
4. **The selection logic needs its own tests**, in Python beside
   `ci_change_scope.py` rather than in workflow shell, because it is riskier
   than the path classification it would sit on top of.

**Stage E's rule already decides this, and the answer is not yet.** Promotion
to job skipping requires an observation window with zero unexplained misses and
a benefit larger than runner variance; a false negative costs time and a false
positive suppresses evidence. Incremental gating is a false-positive risk by
construction. It also weakens exactly what Stage E told it not to: the current
fast path's proof is strong *because* it is cumulative — "every changed path in
this PR is under `docs/`" is a claim about a tree, and going incremental turns
it into a claim about a chain of runs.

**The cheaper target, if Stage 10 wants a win here first, is
content-addressed skipping for `docker-build`** — key the build on a hash of
the Dockerfiles, requirements and service sources and reuse the layer cache
when it matches. That is a claim about content rather than about run history,
so it carries none of the four conditions above, and `docker-build` is the job
`promtail-config` waits on.

### Stage 3 carries a constraint worth knowing before it starts

The two censuses live in **different virtual environments**.
`tests/airflow/test_health_sensor_demotion.py` asserts 13 DAG files wire a
sensor and runs in the main venv, where it must never import `airflow`;
`tests/integration/airflow/test_dag_integrity.py` asserts 14 sensor tasks and
runs in the isolated Airflow venv. One DAG wires two sensors, so both numbers
are right and nothing connects them. **The single declared source they both read
therefore cannot import Airflow** — a data file, or a module with no Airflow
import.

Plan 134's deletion updated the first count and missed the second, and shipped
(`056cde7`); PR #293 then failed on a count nobody had touched. The comment
added reactively in `33b275e` is documentation, not a mechanism, and will drift
again. Plan 139 scoped this as XS and warned that an XL plan should not hold a
two-file fix hostage; giving it a numbered stage near the front settles that
permanently.

## Success criteria

**1. The waiver list is empty.** All 120 entries deleted, each by the repair it
was waiting for rather than by being removed. The contract's own assertions make
this self-verifying: a waiver that no longer describes a violation fails as
loudly as an unwaived violation does.

**2. Every gap this plan closes leaves behind something that fails if it comes
back.** A repair with no assertion behind it is Plan 84 repeated exactly — real
tests, an accurate description, false within months, invisible because nothing
could tell. This is the criterion the six unmechanised gaps exist to be measured
against, and it is why Stage 2 comes before the stages that would otherwise be
graded by the instrument it repairs.

Three exceptions, stated here so they are decisions rather than omissions:

- **G7 stopped being an exception on 2026-09-02, and the rescoping is why.**
  As written it could not be asserted by the existing rules and needed an
  approach invented. Narrowed to the assertionless Layer 2 suite, it is
  ordinarily mechanisable — a Layer 2 test that executes a statement and asserts
  nothing about the result is a rule this suite can hold — and Stage 8 owes that
  rule, not just the 25 assertions. **The part that was genuinely exceptional
  left with G18**, which is Plan 150's, so this criterion no longer carries it.
- **G12** may close without a rule at all, because the condition a rule would
  assert is the constraint being removed. If it ships without one, the plan says
  so explicitly rather than leaving a silent gap.
- **G13 closes an instance without closing its class, and cannot do better.**
  Fixing the canary test's quoting is a one-file repair; asserting that no test
  lets its environment decide the outcome is not mechanisable, and the one place
  that could observe the remaining failures — CI — runs Linux and is blind to
  every Windows-only instance by construction. The `PYTHONPATH` clause is the
  only part with a mechanism and it already has one. This is the weakest of the
  three exceptions and it should be recorded as such rather than dressed up: the
  next instance of G13's class will be found the way the last two were, by
  someone running the suite somewhere CI does not.

  **That prediction came true on 2026-09-01 and the exception is now narrower.**
  A Windows-only encoding defect broke master, found exactly as forecast — by
  someone running the suite where CI does not. It is the third instance of the
  class, which is enough of a pattern to stop treating each one as a one-file
  repair, so [Stage 6b](#stage-6b-was-added-by-the-failure-this-plan-predicted)
  now owns the class. **The exception stands only for the part 6b concludes it
  cannot mechanise**, and 6b is required to say which part that is rather than
  leaving it implied. What is already settled is that the obvious mechanism does
  not close it: `PLW1514` cannot see a `tmp_path / "name"` receiver, so the rule
  that looks like the answer would have passed this defect too.

  **Stage 6b answered this on 2026-09-01, and the exception is now one named
  behaviour rather than a whole rule.** Encoding is mechanised:
  `test_every_text_read_and_write_states_its_encoding` requires `encoding=` on
  every `read_text` and `write_text` in the repository, and fails on
  `(tmp_path / "a.md").write_text("—")` — the exact call ruff answers
  `All checks passed` on. It covers three shapes rather than one: the two
  `pathlib` methods, text-mode `subprocess`, and the logging handlers that open
  a file — the last two found by running PEP 597's `EncodingWarning` once, as a
  discovery tool, and then checked statically rather than at runtime for the
  reasons the decision record gives. **What remains unmechanisable is two
  things, not one.** The first is any encoding shape nobody has named yet: this
  rule sees what it is told to see, and the next unnamed shape will be found the
  way these were. The second is everything else the harness decides: path separators, line endings, case-insensitive filename
  collisions, and locale-dependent collation.** Those have no textual signature
  to match on — the code that breaks on them is not distinguishable, by reading,
  from code that does not — so the only instrument that sees them is an actual
  second platform, and [Stage 6b's decision
  record](#evidence--stage-6b-mechanising-the-encoding-sensitive-io-guard-car-60-2026-09-01)
  says why a Windows runner was declined rather than built. That is the residue,
  and it is now a list of four behaviours instead of an open-ended class.

**3. The `dbt build + test` job is no longer the critical path**, and what
replaced it is named for what it does. Measured in wall-clock seconds against
the 267s baseline, not asserted.

**Met by Stage 4, 2026-09-01.** Across three runs of the final configuration
the workflow went 292s to 145-165s and the job's successor 267s to 118-134s,
in four jobs named for what they run.
[The precise reading](#success-criterion-3-is-met) matters more than the
headline: the dbt job's cost fell by 55% and stopped dominating, but it is
still the longest job in the workflow on both post-change runs. The criterion
was accepted as met on that basis.

**4. ~~Every suite in `tests/integration/` is either invoked by a named CI step
or declared dormant with a reason.~~ Met by Stage 1 (CAR-45), 2026-08-31.**
This was G1's repair and the one criterion already mechanically enforced when
the plan was written. `CI_INVOCATION_WAIVERS` is `()` and
`test_every_integration_suite_is_invoked_by_a_ci_step` fails against an empty
tuple the moment a suite appears unrun, with `tests/integration/lakehouse/`
declared in `DORMANT_SUITES` rather than waived.

*Struck in Stage 4 rather than deleted. The sentence went on describing "the
four current waivers" after Stage 1 had removed all four: the criterion was
already true and only its description had aged, which is the small version of
exactly what this plan exists to stop.*

## The estimate

**L, replacing the XL placeholder**, on three grounds:

1. **The census — the largest single unknown — is done.** The plan was sized XL
   when Stage 0 was an unbounded measurement against a standard that did not
   exist yet. It is now a completed stage with an enumerated result.
2. **Roughly half the waiver count is two mechanical sweeps.** 50 of 120 are
   Stage 5: converting 34 files to `mocker` and renaming 16 layer references.
   Near-zero judgement, verified by deleting a waiver.
3. **The remainder is bounded and enumerated**, file by file, in the gap list
   and the waiver tuples.

**Stage 1 is what confirms or destroys this.** The pass state of the 73 orphaned
tests is the one input the estimate rests on that the census could not settle,
which is why it is first of the remaining stages.

## Non-goals

- **Deciding the standard.** That was Plan 161, and it is archived. If this plan
  finds itself arguing about which mock library is correct, it is relitigating a
  closed decision.
- **Rewriting the contract to match the repository.** The waiver list shrinks by
  repairing code, not by revising rules. A rule that turns out to be wrong is a
  decision to make explicitly, in `docs/TESTING.md`, with the reasoning recorded
  — not a convenience taken mid-stage.
- **Editing `docs/PLANS.md`.** Its row for this plan still reads "stub until
  Plan 161 lands" and names an archived blocker. Correcting it is a state
  transition and belongs to the `plans` skill.

## What it absorbed from Plan 139

Plan 139 was written as test-suite *maintenance* and was archived on 2026-08-31
with Stages A, B and F delivered. Its disposition, recorded when the split was
made:

| Stage | Disposition |
|---|---|
| A — make coverage visible | Shipped; `ci.yml` runs `--cov`. What it did *not* do is make the number mean anything, which is G10 and Stage 2 here |
| B — recover the CI critical path | **This plan**, Stage 4 |
| C — understand the 92s step | **This plan**, Stage 4 |
| D — intent markers and the coverage decision | Split: the gate decision was Plan 161's questions 6 and 8; the markers and the coverage-source repair are **this plan** |
| E — advisory CI impact selection | **This plan**, Stage 10. Its own premise was "before any new fast path", and the restructure is the fast path |
| F — CI's database does not model production's schemas | Shipped 2026-08-31, PR #305 (CAR-36). CI now runs `airflow db migrate` |
| G — Promtail contract checker | Moved to [Plan 160](plan_160_promtail_contract_checker_reliability.md) |
| H — one invariant, two censuses | **This plan**, Stage 3 |

Stage E carries one piece of thinking worth preserving verbatim rather than
rediscovering: Plan 142's service graph is *evidence* for a CI selector, not the
selector itself, because "production asks which live work depends on a service,
while CI asks which tests, images and integration environments can detect a
changed path."

**Two notes from Stage F (CAR-36), for this plan to pick up rather than
rediscover:**

- **CI's Postgres is greenfield; production's is populated.** Stage F made the
  `airflow` schema exist in CI, but built from empty, while production's carries
  hundreds of thousands of rows. Same root cause as bare images versus Compose
  definitions: CI's database is not shaped like production's. Worth measuring in
  Stage 10 — *which* suites depend on an empty database, and which would find
  something in a full one. The rehearsal that would close it needs a deployed
  stack, not a CI job, and is recorded in
  [Plan 121](plan_121_staging_environment.md).
- **`tests/integration/airflow/` still points at `sqlite:////tmp/airflow.db`.**
  Stage F left it deliberately: pointing the DAG tests at the same Postgres
  metadata DB the drain tests read would mix test data into it. Now that a real
  Airflow metadata schema exists in the same job, whether those suites should
  share it is Stage 10's call.

**Consequence, resolved 2026-08-30:** Plans 103 and 107 were triggered by "Plan
139 Stage D settles the coverage gate." Stage D was taken apart, so that trigger
named something that would not happen. Both were **superseded by Plans 161 and
162** — their premises were a coverage percentage and a self-scored rubric, both
last edited 2026-04-29, and both are what Plan 161's contract now decides. Parts
of each had already shipped under other plans without them.

## Intersections

### Plan 161 — the testing contract

Archived. It decided the rules and built the mechanism that measures them; this
plan closes the distance. `docs/TESTING.md`'s gap list names Plan 162 as the
owner of thirteen entries — twelve at the census, plus G13, re-owned here on
2026-08-31 — and an assertion fails if that owner is ever an archived plan — so this plan cannot be quietly abandoned without the suite
saying so.

### Plans 103 and 107 — coverage

Superseded, and **their targets are this plan's inheritance rather than their
own**: 103's per-file coverage gaps and 107's testing-rubric third are what the
gap list now measures. Read them for the gap list they assembled, not for their
numbers.

### Plan 120 — CI lake snapshot

Complete, and it produced **two** artifacts this document had been conflating.
Stage 4 checked, because a claim about what CI builds against should not rest
on a sentence:

- **The synthetic fixture**, `scripts/seed_lake_snapshot_fixture.py` — its own
  docstring calls it *"the synthetic MinIO fixture used by the Plan 120
  lake-snapshot integration tests"*. It is seeded before every `dbt build` in
  CI and **is** read by it: `sources.yml` globs
  `silver_normalized/observations/**/*.parquet` with `hive_partitioning=true`,
  which picks up the fixture's reserved `obs_year=2099` partition, and the
  real-build tests assert on rows only a build over it can produce.
- **The production-derived snapshot** — the `snapshot-worker` →
  `snapshot.tar.zst` + `archive_manifest.json` →
  `ci_snapshots/adaptive_refresh/latest.json` → `download_lake_snapshot.py`
  pipeline. `download_lake_snapshot`, `snapshot.tar.zst` and `ci_snapshots`
  appear in no workflow and no Compose file. **Nothing pulls it**, and getting
  it into CI means a token and production VIN/dealer data on a GitHub runner.

This entry previously read that the fixture was *"unused for the dbt build it
was paid for"*. That is false of the fixture and true of the snapshot, which
is Stage 10's.

### Plan 121 — staging environment

Owns the deployed-stack rehearsal that Stage 10's greenfield-versus-populated
question cannot close from inside a CI job.

## Evidence

### Evidence — Stage 0, the census (CAR-40), 2026-08-31

Both exit conditions met. Commit `dfa55ae`. Estimate 2, actual 1.

The census ran against the instrument Plan 161 built rather than by hand, which
is the whole reason it cost 1 rather than the XL this plan was sized at. What
it produced is [above](#what-the-census-found): 120 waived violations across
five mechanically checked rules, twelve gaps, and a stage per repair.

**Every by-eye reading this document had carried since 2026-08-30 was an
undercount, three times out of three**, and the gap inspection missed entirely
— G14, 56 of 76 `.sql` files executed by no Layer 2 test — is now the largest
single item in the plan. The direction of that error is the reusable finding,
not the individual numbers.

**`dashboard/` is Streamlit, not FastAPI.** G7 therefore cannot be reached by
the route rule or by the "enough" floor's first clause: the rule imports
`<service>.app` and reads its OpenAPI schema, and there is no schema to read.
This was not known when the gap list was written, and it means Stage 8 must
invent an approach rather than drain a waiver list.

*Read on 2026-09-02 as a conclusion about the wrong subject, and left standing
because it was the right conclusion about the one it had. Inventing an approach
is what the Streamlit Python needs, and that is now G18 and Plan 150's; the
dashboard gap this plan kept — a Layer 2 suite with 25 tests and no assertions
— needed no invention at all, and the census never looked for it because it was
counting test files rather than reading one. See [Stage 8
narrowed](#stage-8-narrowed-and-g7-now-names-a-different-gap).*

**Half the gap list is unenforced.** Six of the twelve gaps are checked by
nothing at all, which is what success criterion 2 exists to answer — a repair
that leaves no assertion behind is Plan 84 repeated, and Plan 84's description
was accurate on the day it was written too.

**One defect was found and spun out rather than absorbed.** `docs/TESTING.md`'s
rules table claimed a check the suite did not implement: the Layer 2 rule was
written as "every `.sql` file **and module-level statement**", and only the
first half existed. Fixed as CAR-43 (PR #311) under Plan 161, which owned the
defect, and merged before this scoping was committed — the contract is the
standard this plan measures against, and scoping against a document that
overstated itself would have been building on sand. The repair added
`test_every_asserted_rule_names_a_real_test`, so the contract can no longer
claim a mechanism it does not have.

**What the census could not settle:** whether the 73 orphaned tests still pass.
That is Stage 1, and it is the one input the L estimate rests on that remains
unmeasured.

### Evidence — Stage 1, the orphaned suites (CAR-45), 2026-08-31

All five exit conditions met. Estimate 2.

**The 73 tests were run.** Against a cold `postgres:16` with all 49 Flyway
migrations applied and a MinIO container, matching the `dbt` job's services
step for step — not against a warm local stack, which would have had the state
the suites are missing:

| Suite | Files | Tests | Result |
|---|---|---|---|
| `tests/integration/processing/` | 6 | 58 | **51 passed, 7 failed** |
| `tests/integration/scraper/` | 1 | 4 | 4 passed |
| `tests/integration/shared/` | 1 | 4 | 3 passed, 1 declared skip |
| `tests/integration/lakehouse/` | 3 | 7 | dormant — not run |

**66 of 73 passed. The 7 failures were all defects in the tests, none in
production code** — which is the answer the estimate needed, and the better of
the two available answers. The areas these suites cover are not unexercised
because the code rotted; they were unexercised because nothing ran the tests.

Two distinct defects, both of the kind only running can find:

- **Six cleanups named `staging.artifact_events`, a table no migration has ever
  created.** V017 created `staging.artifacts_queue_events`; the test file has
  said `artifact_events` since `e95e426`, the commit whose message claims to
  "close processing service test gap". Every one of those six tests had already
  passed its assertions and then failed on teardown — the suite was born broken
  and merged anyway, because merging did not involve running it.
- **`test_vin_relisting_replaces_old_row` asserted a remap that
  `upsert_vin_to_listing.sql` correctly refuses.** The SQL has a recency guard —
  it only remaps on a strictly newer `mapped_at`, and production passes the new
  artifact's `fetched_at`. The fixture defaulted the prior mapping's timestamp
  to `now()`, so the value the test then supplied was never newer and the
  remap was always a no-op. The test was wrong; the guard is the feature.

**A third failure was found that the first two do not explain, and it is worth
recording as unexplained.** In the first two runs
`test_respects_batch_size_limit` reported five of its own rows claimed against
`batch_size=2`. It has not reproduced since the queue was drained, and a direct
harness confirms `_claim_batch(2)` returns exactly two rows. What *is*
reproducible is the class it belongs to: `_claim_batch` reads the whole of
`ops.artifacts_queue` lowest-`artifact_id`-first, so every "my row was claimed"
assertion in that file is really asserting the row landed inside the first
`LIMIT`. Seed twenty pending rows and two of those tests fail deterministically.

That mattered enough to fix rather than note. The suite passes today in CI's
step order — measured, not assumed: after `sql`, `ops` and `scripts` run,
`ops.artifacts_queue` is empty — but "passes because the four suites ahead of
it happened to leave no rows" is not wired in, it is booby-trapped. A
function-scoped `_quiet_queue` fixture now parks any other claimable row for
the duration of each test and restores it afterwards. Verified both ways: 58
pass against a queue holding 20 foreign pending rows, and those 20 are still
`pending` afterwards.

**Dormancy could not be a waiver, and finding out why was the stage's one
design change.** Stage 1 was scoped to declare `tests/integration/lakehouse/`
dormant through the waiver list, on the reasoning that a waiver already carries
a reason, an owner and a date. It cannot: `test_no_waiver_outlives_the_plan_that_owns_it`
fails any waiver whose owner plan has archived, so the lakehouse entry would
have failed the day *this plan* archived, and the only way to quiet it would
have been to delete the record of why the suite is not running — losing exactly
what G2 asked to be written down. Dormancy is a decision with no repair pending
and no owner to outlive, so it now lives in `DORMANT_SUITES`: same file, same
shape, no owner, no expiry. `test_no_dormant_suite_is_quietly_running` closes
the other direction, failing a declared suite that acquires a CI step.

**`CI_INVOCATION_WAIVERS` is `()`.** Both new assertions were verified by
breaking them: removing the processing step fails the invocation rule against
an empty waiver tuple, and pointing a step at the dormant suite fails the
dormancy guard. 120 waivers → **116**.

**The L estimate is confirmed.** Stage 1 was the one input it rested on that
the census could not settle, and it resolved the favourable way: no production
defects, no rot in the covered areas, and Stages 7 and 8 do not get worse. The
two test defects cost minutes, not the days a genuine failure would have. What
Stage 1 adds to the estimate is not effort but a warning about its shape — both
defects, and the queue fragility, were invisible to review and obvious to
execution, so the remaining stages should be sized on the assumption that
anything this plan has only *read* is still unmeasured.

**Found in passing, and it turned out to belong to nobody — so this plan took
it.** `tests/scripts/test_verify_recovery_live_state.py::test_a_failing_canary_command_fails_the_check`
fails on Windows ("The filename, directory name, or volume label syntax is
incorrect") and passes in CI; it quotes `sys.executable` with `shlex.quote`,
which `cmd.exe` does not honour. Reproduced on an untouched checkout, so it
predates this stage.

Chasing its owner is what made it Plan 162's. It is G13's class, and G13 was
the one gap in `docs/TESTING.md` this plan did not own — assigned to **Plan 146
Stage 1 (CAR-42)**, scoped to the `PYTHONPATH` half. That half shipped; the
`Documentation tests` step sets `PYTHONPATH` today, and `21333ab` had already
repaired the other named instance. So the G13 row described finished work while
naming an owner in closeout that owes no code, and the live instance — found and
deliberately left by Plan 161 — had no owner at all.

**G13 is therefore re-owned to Plan 162, Stage 5**, and the row rewritten to
describe what is actually left. Stage 5 rather than Stage 3: Stage 3 is a
specific two-venv census fix and "also small" is not a category, while Stage 5
is already the pass that reads every patch in the suite. What that costs is
honest and recorded above — [a third exception](#success-criteria) to success
criterion 2, and the weakest of the three.

### Evidence — Stage 2, unblinding coverage (CAR-46), 2026-08-31

All three exit conditions met. Commits `8c10d95` and `2b12294`. Estimate 1,
actual 1.

`[tool.coverage.run] source` now names all ten production directories, the unit
job gates on `--cov-fail-under=74` and uploads `coverage.xml`, and two
assertions carry the repair — `test_every_service_directory_is_measured_by_coverage`
and `test_the_coverage_number_the_unit_job_produces_is_consumed`. Both were
verified by breaking them; the mutation set is 19 and all 19 are caught.

**Unblinding moved the reported number from 88% to 75.95% with no code
changing** — the figure CI reads, and the one the gate acts on. The four
directories added hold 11,709 of 19,733 measurable statements — more than the
six that were being measured:

| Added to `source` | Statements | Covered |
|---|---|---|
| `scripts/` | 10,488 | 70% |
| `airflow/dags/` | 754 | 45% |
| `dashboard/` | 309 | **9%** |
| `container_health/` | 158 | **93%** |
| *(the six already measured)* | 8,024 | 88% |

**The two services the "enough" table calls below the floor are not alike, and
only the instrument could show it.** `container_health` is among the
best-covered directories in the repository at 93% and is still below the floor,
because the floor is routes reached through the app and a Layer 4 that exists —
neither of which a percentage measures. That is the contract's *"not a coverage
percentage"* clause holding under its first real test, and it re-scopes Stage 6:
G9 is a test-home and routing problem, not a coverage one. `dashboard/` at 9%
is the genuine gap, and it is Stage 8's.

**The threshold is a ratchet, not a target, and `scripts/` is the caveat on
it.** At 10,488 statements it is over half the denominator and largely spent
one-off code, so it damps the movement the service stages produce. That is
[Stage 5b](#the-stages), scoped from this measurement.

**Two of this stage's own claims were wrong and were corrected by measuring.**
The gap list said unblinding would expose "the two services below the floor"; it
exposed one badly covered service, one well-covered one, and a `scripts/`
denominator nobody had counted. The first sizing of Stage 5b then repeated the
error in miniature — see its section for what the archive join corrected.

**Landed alongside Stage 1 and reconciled to it.** Stage 1 established the
convention for repaired gap entries — row deleted, preamble names what closed
it, history here, letters never reused — while this branch was open; G10's
closure was rewritten to follow it. Stage 1's own deletions had left three
mutations in `scripts/verify_testing_contract_mutations.py` anchored on the
removed G1 and G2 rows, so the script aborted rather than ran; its staleness
guard is what said so. Re-anchored, and Stage 1's new dormancy rule was given
the mutation it shipped without.

**Confirmed in CI, which is the only place the gate can actually fire.**
[Run 33442835886](https://github.com/whitewalls86/new_car_tracker/actions/runs/33442835886)
on PR #318, all jobs green:

- `Required test coverage of 74% reached. Total coverage: 75.95%` — the
  threshold ran and passed, with **1.95 points of headroom**.
- `Artifact coverage-xml has been successfully uploaded` — 49,164 bytes,
  artifact 9776948765. The report outlives the log, which was the point.

**Linux reads 75.95% where Windows reads 75.92%** — same 19,733 statements,
4,745 missed against 4,752, so **seven statements are platform-dependent**.
That is the number the ratchet's headroom has to absorb, and it is why 74 was
set two points below the local measurement rather than one. A future stage
raising the floor should keep at least that much slack.

**The canary test that fails locally passes here**, which is G13 restated as
evidence rather than assertion: `test_a_failing_canary_command_fails_the_check`
is among the 3,195 that pass on Linux and is the one failure on Windows. CI
cannot see the instance Stage 5 owns, exactly as its row says. The single
Linux skip is unrelated — `test_every_sha_a_recap_names_is_a_real_commit`,
which skips on a shallow clone.

### Evidence — Stage 3, one declared source for the health-sensor censuses (CAR-47), 2026-08-31

All three exit conditions met. Commits `17d4fab`, `a5fda6c` and `92ef62b`,
[PR #319](https://github.com/whitewalls86/new_car_tracker/pull/319). Estimate 1,
actual 1.

`tests/health_sensor_census.py` declares the mapping both counts derive from:
DAG file → the service names it passes to `http_health_sensor`. Thirteen keys,
fourteen services — the two numbers that were hardcoded separately, now one
declaration. **Declaring the structure rather than the two integers is what
makes the stage's own constraint disappear**: "one DAG wires two sensors" is a
fact in the data instead of the comment that was documenting it, and neither
count can be updated without the other following.

Both assertions got stronger as a side effect of having something to compare
against:

| Test | Before | After |
|---|---|---|
| `test_the_gate_survives_the_demotion` (main venv) | `len(wired) == 13` | the whole file→services mapping, ast-extracted from the real call sites, so drift names the DAG |
| `test_health_sensors_skip_rather_than_fail_on_the_real_operators` (Airflow venv) | `health_sensors == 14` | sorted DagBag task ids against the ids the census implies, so a missing one names itself |

One assertion is new. `test_the_task_id_the_census_predicts_is_the_one_the_factory_builds`
pins `sensors.py`'s `f"check_{service_name}_health"` — the only link joining the
census's service names to real task ids, and previously unchecked from either
side.

**The exit asked that the declared source not import Airflow; it imports nothing
at all**, and that turned out to be load-bearing for a reason the exit did not
anticipate.

**The first attempt failed in exactly the way this stage exists to catch.**
`from tests.health_sensor_census import ...` resolves in the main venv and not
in the isolated `apache-airflow==3.2.0` one, where pytest leaves the repo root
off `sys.path`.
[Run 33444675959](https://github.com/whitewalls86/new_car_tracker/actions/runs/33444675959)
failed collection with `ModuleNotFoundError: No module named 'tests'` on an
import that had passed locally and could not be made to fail locally. The fix
was to remove the environment from the question — both readers now load the
census by path with `importlib.util`, which depends on nothing — rather than to
add the repo root to the CI step's `PYTHONPATH`, which would have made the
suite's outcome depend on its environment and manufactured a fresh instance of
G13's class while closing this one. **The pre-push verification had proved only
that the main venv could reach the file, and inferred the rest.** That is worth
recording as the stage's real cost: the two-venv constraint was understood,
written down in the issue, and still evaded on the one axis nobody checked.

**Mutation-checked in three directions**, each failing with the intended
message: dropping a census entry, adding a sensor to a DAG, and renaming the
`task_id` format. The path loader was then re-verified against the condition CI
actually had — with the repo root stripped from `sys.path`, the bare import
raises the same `ModuleNotFoundError` while both test modules import cleanly and
read 13 files / 14 tasks.

**Confirmed in CI, which is the only place the DagBag half can run.**
[Run 33445223553](https://github.com/whitewalls86/new_car_tracker/actions/runs/33445223553),
all jobs green:
`test_health_sensors_skip_rather_than_fail_on_the_real_operators PASSED` in the
isolated venv, with `dbt build + test` at 4m43s.

**What is not verified:** no mutation was run against the DagBag census in CI,
which would mean pushing a deliberately broken commit. That half follows by
construction — it reads the same declaration through the same loader — and it
has passed, but it has not been broken on purpose the way the main-venv half
has.

**A third census was found one layer up, and was already one short.**
`DAG_SPECS` in `tests/integration/airflow/test_dag_integrity.py` omitted
`disk_usage`, so neither `test_dag_imports_without_error` nor
`test_dag_id_and_tasks` ever reached that DAG — while
`airflow/dags/disk_usage.py`'s own `except ImportError` comment claimed *"the
Airflow integration suite imports the real DAG and asserts it exists"*. A false
claim, checked into source, in the same shape as the defect this stage removed:
a list kept honest by whoever remembers it. Fixed in `92ef62b` — the entry
added, and a new assertion comparing the DagBag's dag_ids against the ones
`DAG_SPECS` names, because parametrising over a list can only ever check the
things someone thought to list. It is not owned by any stage: G12 is about
`shared` imports, not this.

### Evidence — Stage 4, splitting the 267s dbt job (CAR-48), 2026-09-01

All four exit conditions met. Commits `5dd6bb7`, `8c54915`, `70d2411`,
`9f21f87`, [PR #321](https://github.com/whitewalls86/new_car_tracker/pull/321).
Estimate 2.

**The whole result, measured across nine CI runs rather than asserted:**

| | Workflow wall clock | The long job | `tests/integration/dbt/` |
|---|---:|---:|---:|
| Baseline, three consecutive runs | 277 / 297 / 303s | `dbt build + test` 254 / 272 / 282s | 88 / 89 / 61s |
| Split into four jobs | 232s | `dbt model tests (real build)` 210s | 89s |
| — plus per-job dependency trim | 201s | 177s | 86s |
| — plus in-process dbt, three runs | **145 / 156 / 165s** | **118 / 134 / 128s** | **25 / 37 / 36s** |

**292s to about 155s is a cut of a little under half, and the 269s job's
successor runs around 127s.** Three changes did it, and only the first was the
stage as scoped.

The final configuration is quoted as three runs rather than a number because
the spread is real and worth knowing before a later stage optimises against
it: **the wall clock moved 20 seconds across three consecutive runs of
identical code**, and the equivalence step moved 25s to 37s. Job durations are
steadier than wall clock, which also carries queueing — run 3's `lint` waited
14s for a runner. A change worth less than about 20 seconds cannot be
demonstrated here without more runs than it is worth.

#### What the job was, and why the cut is by prerequisite

Eight suites ran in series on one runner behind one Flyway migration, one
`pip install`, one `dbt build` and one Airflow venv build. The suites shared
a runner and nothing else, so the split is by the only thing they *did*
share:

| Job | The prerequisite it pays for | Suites |
|---|---|---|
| `dbt model tests (real build)` | a real `dbt build --target duckdb` | `dbt/` |
| `SQL + Airflow metadata contracts` | that build **and** the `airflow` schema | `sql/`, `airflow/` |
| `Service integration tests (Postgres)` | a migrated Postgres, nothing else | `ops/`, `scripts/`, `processing/`, `scraper/`, `dbt_runner/` |
| `Lake integration tests (MinIO)` | the Plan 120 fixture in object storage | `shared/`, `archiver/` |

`tests/integration/sql/` is the only suite needing two prerequisites — 21
tests read dbt's marts out of DuckDB, 92 read Postgres, one file reads
Airflow's own `task_instance`/`dag_run`. So `schema-contracts` runs its own
`dbt build` rather than waiting on `dbt-models`. Passing the DuckDB file
between jobs as an artifact was costed and rejected: it puts the two longest
jobs in series, which is the thing the split exists to stop. The second build
costs ~31s in a job that is not on the critical path.

The Airflow venv now lives in the one job whose suites import Airflow. It ran
ahead of all eight before, six of which never import it.

#### Plan 139 Stage C's question, answered

**The 92s step was never running tests. It was starting Python.**

`--durations=20` in CI: 16 tests, 93.99s. Seven real-build tests were 93.25s
of it — **99.2%** — and the nine selector-equivalence tests were 0.44s
together. Those seven drive 21 `dbt build --select` subprocesses at a mean
**4.44s**, while dbt's own report for one such build reads *"1 incremental
model, 1 project hook, 12 data tests, 4 unit tests in 0.61 seconds"*.

The decomposition, measured in the job rather than reasoned from adapter
internals:

| | |
|---|---:|
| `import dbt.cli.main` | 1.37s, once per process |
| `dbtRunner().invoke(build)` | 1.19 – 1.24s |
| `dbtRunner(manifest=...).invoke(build)` | 1.04 – 1.14s |
| the same build as a subprocess | ~4.2s |

So roughly 3s of every 4.4s was Python starting and importing dbt, paid 21
times: **~80s of the 93s, against ~13s of actual dbt work.**

**Plan 139's hypothesis was half right, and the wrong half is the useful
one.** It guessed "each test drives its own real dbt invocation and they could
share one build". The first clause holds. The second cannot: each test's
subject *is* a sequence of incremental builds with fixture data seeded
between them, so there is no single build to share. Only the startup was
shareable. Sharing it took the suite to **24.26s** — the seven from 93.25s to
23.83s, every test between 2.33s and 4.92s where the cheapest had been
12.31s. Both of those are pytest's own timer on one run each, which is the
only way to get a per-test breakdown; the CI *step* around them reads 86-95s
before and 25-37s after, across three runs each.

Manifest reuse was measured and **not** taken: ~0.15s per invocation, about
three seconds total, in exchange for running a configuration the numbers
above were not measured against.

#### A property nearly lost, then re-established somewhere else

All three readers in `tests/integration/dbt/` opened DuckDB with
`read_only=True`, so no assertion could mutate the warehouse it inspected.
dbt-duckdb caches its environment across invocations — precisely why an
invoke is 1.2s rather than 4.4s — so it holds the file open read-write for
the life of the pytest process, and a read-only *connection* is no longer
available. Two ways of keeping one were tried against duckdb 1.5.5, the
version CI runs, and both are closed:

```
connect(path, read_only=True)   ConnectionException: Can't open a connection to
                                same database file with a different
                                configuration than existing connections
:memory: + ATTACH (READ_ONLY)   BinderException: Unique file handle conflict
```

A third, reading a copy of the file, works and was rejected on meaning rather
than mechanism: every assertion would then describe a snapshot instead of the
warehouse the build actually wrote.

**So the guard moved from the connection to the statement**, which is a
different mechanism for the same property and on one axis a stricter one. It
was always the *statements* that were the risk, and a read-only connection
never had an opinion about `COPY ... TO`, which reads the warehouse and writes
the filesystem. `ReadOnlyConnection` classifies every statement with
`duckdb.extract_statements` — the engine's own parser, so no regex over SQL
text and no special case for `WITH ... SELECT` — and allows only `SELECT` and
`EXPLAIN`. **Deny by default**: a statement type a future DuckDB adds is
refused until someone reads it and adds it.

This is success criterion 2 applied to the stage's own work, so it has an
assertion behind it rather than a paragraph.
`test_analytics_connection_guard.py` is 16 tests and runs unmarked in the unit
job, because it needs no MinIO, no Postgres and no dbt. Verified by breaking
the guard four ways:

| Mutation | Result |
|---|---|
| `INSERT`/`DELETE` added to the allowlist | 5 fail |
| only the first statement of a multi-statement string classified | 1 fails |
| the check moved after execution instead of before | 1 fails |
| the check removed entirely | 11 fail |

**The first run of that mutation set was wrong, and how it was wrong is worth
more than the table.** Restoring each mutation with `cp` set the source
mtime inside the same second as the `.pyc` written moments earlier, so Python
kept the *mutated bytecode* and ran it against restored source. The visible
symptom was a test failing while `inspect.getsource` showed correct code —
`refuse_writes` provably first in the file, and a `DELETE` reaching the
database anyway. It was diagnosed by wrapping the connection in a spy that
logged every statement reaching it, which showed the execute happening
*before* the refusal.

Nothing was wrong with the guard. **The verification tooling was lying, in a
way that read exactly like a defect in the thing under test** — which is this
plan's own recurring finding pointed at itself, and the reason the mutation
script now clears `__pycache__` around every edit rather than trusting a
timestamp.

**What it does not cover, stated so the limit is known:** a caller that
reaches past the wrapper for a raw connection. Nothing in the directory does
today.

The change reaches further than the two files that build: all three share one
pytest process, so the selector suite's reads moved too, though it invokes dbt
never.

#### Two findings that were not scoped and cost nothing

**The `loki` service was dead weight.** The old job started three service
containers; nothing under `tests/integration/` has ever opened port 3100. It
arrived with `cf216c8`'s log-aggregation work and outlived it. Removing it
took **"Initialize containers" from 44–56s to 12–16s** — far more than
expected, because the Loki image pull was most of it.

**Every job installed every service's dependencies, and none of them needed
to.** ~220MB of wheels per job, because the single job it replaced genuinely
did need all seven files. Measured by walking each suite's imports including
its first-party ones:

- **`dashboard/` is ~50MB** of streamlit, pydeck, pandas, plotly and pillow
  that no integration suite reaches. `tests/integration/sql/` imports
  `dashboard.queries`, and that module imports `pathlib` and
  `shared.query_loader`.
- **`scraper/` brings curl_cffi (13.5MB) and asyncpg**, and the scraper suite
  reaches neither: it imports `processing.queries` and `scraper.queries`, both
  pure `load_query` modules.

`Install dependencies` went **44s to 17–20s** in the three jobs that dropped
`dashboard/`, and 46s to 41s in `service-integration`, which still needs four
files. The failure direction is the safe one — a distribution that turns out
to be needed is an ImportError at collection, named and immediate.

#### Success criterion 3 is met

The criterion is *"the `dbt build + test` job is no longer the critical path,
and what replaced it is named for what it does"*. The job is gone; the four
that replaced it are named for what they run; the workflow went 292s to
145-165s and the job's successor 267s to 118-134s across three runs.

The third clause is the one worth stating precisely rather than rounding, and
**the first attempt to state it here was wrong on one reading — which is this
plan's own recurring finding, committed by this plan.** On the first run after
the in-process change, `dbt model tests (real build)` was 118s against `Unit
tests (pytest)` at 112s, and this section said the two were "within runner
variance of each other". The next run said 134s against 97s:

| | run 33465499674 | run 33466934324 | run 33467252321 |
|---|---:|---:|---:|
| workflow wall clock | 145s | 156s | 165s |
| `dbt model tests (real build)` | 118s | 134s | 128s |
| `Unit tests (pytest)` | 112s | 97s | 97s |
| gap | 6s | **37s** | **31s** |

**Three readings, and the dbt job is the longest in all three.** It is no
longer a 267-second critical path; it is still the critical path. The honest
form of the claim is that the job's cost fell by about half and stopped
dominating, not that something else overtook it — and one run was never enough
to say which, on a runner whose variance moves a 25s step to 37s.

**The third run is the one that settles it, and it settles it the other way
from the guess.** It was taken after the temporary measurement step was
deleted, which the previous draft of this section expected to be worth 7-11s;
the job came back at 128s and the workflow at its slowest of the three. The
rig was never the variable. Runner variance was, and it is larger than the
thing being subtracted — which is the argument for quoting three runs
everywhere above rather than the best one.

**The criterion was accepted as met by the maintainer on 2026-09-01 with this
reading on the record**, which is a decision about what "no longer the critical
path" was worth in practice, not a measurement that says something else is
slower. A stage that wants the dbt job genuinely off the top has a sized
target waiting: its remaining `Install dependencies` is 18s and its `dbt build`
16s, against 25-37s of tests.

#### What was deliberately not done

- **No test file moved and no step was renamed.** Two `ci.yml` step names are
  subjects in `LAYER_NUMBER_WAIVERS`, and `tests/integration/sql/`'s files
  appear there too; renaming or moving either would have meant rewriting
  waivers that Stage 5's G11 sweep deletes outright — the same collision
  [Stage 5b's placement argument](#stage-5b-what-the-split-is-and-why-a-directory-rather-than-a-list)
  is built on. The waiver list is untouched at 116.
- **`docs/PLANS.md` was moved through the `plans` skill**, not edited here.
  Build-order row 1 keeps its position; only its slice pointer advanced to
  Stage 5, since Stages 5 through 10 remain. This plan's non-goal is authoring
  that file mid-stage, and the skill's boundary is what kept the two apart:
  both authored cells were proposed with sources and approved before the row
  was touched.
- **The measurement rig is gone.** `.github/scripts/measure_dbt_invocation_cost.py`
  and its `ci.yml` step existed to answer Stage C and were deleted once the
  answer was recorded above. The numbers it produced are in this section; the
  script is in the history at `e3b4c82` if a later stage wants to re-run it.

#### Cost, and one regression worth recording

The stage cost one red CI run, and it was self-inflicted in a way the suite
was already built to catch: the split commit wrote `apache-airflow==3.2.0`
into two YAML *comments*, and
`test_ci_pins_the_airflow_version_production_runs` greps the whole workflow
for that pattern and requires exactly one distinct match. It found three —
`3.2.0`, ``3.2.0` `` and ``3.2.0`.`` — having swept up markdown backticks.
The test was right and the comments were wrong; a version in prose is a second
declaration that drifts. **It was invisible locally because the assertion is
integration-marked and runs inside the isolated Airflow venv, which a
`-m "not integration"` run never reaches** — the same shape as Stage 3's
finding, and the second time in two stages that pre-push verification proved
only what the main venv could see.

One harmless consequence of in-process dbt, recorded so it is not mistaken
for a defect later: dbt-core's own Click deprecation warnings now surface in
this suite's output, because dbt is imported into the pytest process instead
of hidden inside a subprocess.

### Evidence — Stage 5, the mechanical sweeps (CAR-49), 2026-09-01

All four exit conditions met. **The waiver list went 116 to 68** — the 50 the
stage was scoped to delete, and 2 added back because moving two test files
[exposed a defect in the Layer 2 checker](#the-instrument-was-weaker-than-its-own-docstring).
Both tuples the stage owns are now `()`. 51 test files, three workflow lines
and one contract document changed; the production tree was not touched.

| | Before | After |
|---|---:|---:|
| `MOCKER_WAIVERS` | 34 | **0** |
| `LAYER_NUMBER_WAIVERS` | 16 | **0** |
| `ROUTE_WAIVERS` (Stage 6) | 12 | 12 |
| `LAYER_2_WAIVERS` (Stage 7) | 54 | **56** |
| **Total** | **116** | **68** |

G4, G11 and G13 are deleted from `docs/TESTING.md`'s gap list, which is now
seven rows: G5, G6, G7, G8, G9, G12 and G14.

#### The venv fix, which really was one argument

`ci.yml`'s isolated Airflow venv now installs `pytest-mock` alongside `pytest`,
and that was the whole of it — one word on one `pip install` line. Both
`tests/integration/airflow/` files converted on the strength of it. The gap
row had already argued the point and the argument held: `pytest-mock`
depends only on `pytest`, so none of the starlette/fastapi conflict that
forced the venv's existence applies to it.

**What that fix cannot be verified against locally, and is not.** Neither
Airflow file runs in the main venv — `test_hourly_analytics_refresh.py`
imports `airflow.exceptions` through its fixture and errors out without a real
Airflow install. `test_scrape_listings.py` guards its import and does run: all
7 pass converted. The other file's conversion is `patch.object(module, ...)`
to `mocker.patch.object(module, ...)` eleven times over, with no scope change,
and CI is where it is proved.

#### Two of the 16 were a move, not a rename

Fourteen were: `tests/integration/sql/` and `tests/integration/ops/` moved 1→2
and 3→4 respectively, and the two `ci.yml` step names went with them —
`Run SQL smoke tests (Layer 2)` and `Run API integration tests (Layer 4)`.

The other two were not, and the waiver list said so before the sweep started:
`test_flush_silver_observations.py` and `test_flush_staging_events.py` were
waived as *"1, not 4"*, not "1, not 2". They are SQL smoke tests by content —
"validates the SELECT / DELETE SQL patterns ... against a real DB with Flyway
migrations applied" — and they were sitting in `tests/integration/archiver/`,
which the contract places at **Layer 4**. Claiming Layer 2 there fails the
rule; claiming Layer 4 makes the headline argue with the file.

**The docstrings were right and the directory was wrong, so the files moved.**
Both are now `tests/integration/sql/`, keeping their original
`Layer 2 — SQL smoke tests for <processor>` headline. Nothing else changed:
each uses only the `cur` fixture from `tests/integration/conftest.py`, imports
nothing from `archiver`, and needs only `TEST_DATABASE_URL` and Flyway — all of
which the `Run SQL smoke tests (Layer 2)` step supplies. The archiver step
keeps its six MinIO-dependent files.

The rule caught the mismatch because the waiver recorded the *actual* layer
rather than the shifted one. A +1 sweep applied blindly would have written
"Layer 2" into a Layer 4 directory and failed, which is the assertion doing its
job.

#### Four `Layer N` cross-references, which the rule cannot see

The asserting test reads only a *leading* `Layer N` on a module docstring's
first line. Four other mentions carried Plan 84's numbering in prose, invisible
to it, and were swept by hand:

- Two class docstrings in `tests/integration/sql/test_ops_queries.py` saying
  "Layer 1 smoke tests" in a Layer 2 file. Straight +1.
- `tests/ops/routers/test_scrape.py`: "SQL correctness is covered by Layer 3
  integration tests" → Layer 4, and `tests/integration/ops/test_scrape.py` is
  the file it means. Straight +1.
- `tests/integration/ops/test_maintenance_api.py` pointed at "the Layer 1 tests
  in test_maintenance.py". **This one lost its number rather than gaining
  one.** The file it names is `tests/integration/ops/test_maintenance.py`,
  beside it, which the contract places at Layer 4 — so neither "Layer 1" nor
  the +1 shift's "Layer 2" is true, and "Layer 4" would say the opposite of
  what the sentence means. It now names the file and no layer.

That last one is worth keeping because the contract test's own comment cited it
as the example of *"prose that is correct"* — the reason cross-references are
excluded from the rule. It was not correct. The comment now cites
`test_scrape.py` instead, which is.

#### G13: an interpreter path, replaced by a shell builtin

`tests/scripts/test_verify_recovery_live_state.py::test_a_failing_canary_command_fails_the_check`
built its canary command as `f"{shlex.quote(sys.executable)} -c ..."`. The
verifier runs `--canary-cmd` through `subprocess.run(..., shell=True)`, and
`shlex.quote` is POSIX quoting that `cmd.exe` does not honour, so the test
failed on Windows and passed in CI on the same code.

The command is now `exit 3`. What the test owns is that a non-zero canary fails
the check and lands its returncode in the report; naming an interpreter was
never part of that, and it dragged a filesystem path through the shell's
quoting rules to get there. `exit 3` needs no quoting and means the same thing
to `/bin/sh` and `cmd.exe`.

The original comment defending `sys.executable` — *"a `python` command is not
guaranteed to exist even where Python does"* — was answering the right question
and reaching for the wrong tool. Avoiding the interpreter entirely answers it
better.

#### One conversion that is not mechanical, and says why in the file

`tests/scraper/processors/test_scrape_detail.py::test_a_dummy_scrape_is_not_counted`
took both its readings **outside** a `with patch("builtins.open", ...)` block
on purpose: `prometheus_client`'s `ProcessCollector` opens `/proc/<pid>/stat`
in binary mode during `REGISTRY.collect()`, and under `mock_open` that read
returns a `str` and raises. `mocker.patch` lasts until teardown, so a
straight conversion would have moved the second reading inside the patch and
broken it — **on Linux only**, which is to say in CI and not on the machine
doing the conversion.

It converts with two `mocker.stop()` calls and a comment saying why the patch
is stopped by hand. This is the only place in the 34 where the `with` block's
*scope* was load-bearing rather than incidental, and it is the same rule G13 is
about, one layer down: the file already knew the answer because someone had
been bitten by it, and the comment is what carried that across.

#### How it was done, and what that cost

The sweep is 345 new `mocker.patch` call sites across 34 files. It was done
with four throwaway AST rewriters — flatten `with patch(...)` blocks (including
the parenthesised multi-manager form) into `mocker.patch` calls and dedent the
body; rename `monkeypatch.setattr` to `mocker.patch.object`, or to
`mocker.patch` where the first argument is a dotted string; add the `mocker`
parameter to every function that gained a use of it; drop `monkeypatch` from
every signature that no longer references it. Each file was then run.

**The rewriters got the tests right and the plumbing wrong, repeatedly.** Every
failure they produced was the same shape: a *helper* — `_run_apply`,
`_patch_dedupe`, `_canary_ready`, `_mock_zstd`, some thirty of them — gained
`mocker` as a trailing parameter while its call sites went on passing
positionally, so the fixture landed in the wrong slot. `test_reconcile_april_detail.py`
alone took five rounds of this, 152 failures down to zero. The tell was always
a `TypeError` or `AttributeError: 'MonkeyPatch' object has no attribute
'patch'` at call time, never a wrong assertion, which is the good failure mode
to have: nothing silently passed.

Where a `@contextmanager` helper existed only to hold patches — `_fake_db` in
two files, `_mock_zstd` — it stopped being a context manager. `mocker`'s
teardown is the fixture's, so the yield had nothing left to do.

#### Three process-state patches stayed monkeypatch, correctly

The contract's carve-out is not a formality and three sites landed in it:
`patch.dict(os.environ, ...)` in `tests/airflow/test_notifications.py` became
`monkeypatch.setenv`, and `patch.dict(sys.modules, {"sensors": ...})` in
`test_pack_bronze_html_dag.py` became `monkeypatch.setitem`. `mocker` is the
wrong tool for all three and the rule already says so —
`_MONKEYPATCH_ALLOWED` lists `setenv`, `setitem` and the rest, so the checker
agrees.

#### A unit test filed as an integration test, and two wrong answers before the right one

`tests/integration/airflow/test_scrape_listings.py` carried no `integration`
marker, so `pytest tests/integration/airflow/ -m integration` collected it and
deselected all 7 of its tests. Its sibling `test_hourly_analytics_refresh.py`
sets `pytestmark` and runs.

**The first reading of that was wrong, and worth recording because it was wrong
in a specific way.** It was read as "the suite is credited as invoked and runs
nothing" — G1 one level down. The tests were in fact running the whole time, in
the **unit** job, because the unit selector is `pytest tests/ -m "not
integration"` and it walks every directory under `tests/`. The layer section of
[`docs/TESTING.md`](../TESTING.md) states exactly this, and it had already been
read once during this stage:

> it collects **every** directory under `tests/` and runs whatever is not
> marked `integration`, so a file's location does not decide whether it runs
> here. The marker does.

So the finding was an inference from one job's log rather than a measurement,
and the measurement was one grep away.

**The second answer was also wrong.** Adding the marker made the file match its
directory — and the directory was the thing that was wrong.
`airflow/dags/scrape_listings.py` imports only `logging`, `time` and `requests`
at module level and guards its DAG construction behind `except ImportError`, so
the 7 tests mock `requests` and `time.sleep` and need no Airflow, no database
and no MinIO. That is a Layer 1 unit test by this contract's own definition, and
`tests/airflow/` is the row that describes it: *"Unit tests of DAG modules.
Runs in the main venv, so it must not import `airflow`."*

**The file is now in `tests/airflow/`**, beside `test_notifications.py` and
`test_coordination_admission.py`, which avoid importing Airflow by the same
discipline. No marker, and `parents[3]` becomes `parents[2]`. It runs where it
already ran, now for a stated reason rather than by accident, and
`tests/integration/airflow/` collects 59 with zero deselections.

**What caught it was the coverage number, and only the coverage number.** The
marker moved the 7 tests out of the one job that runs `--cov`:

| | master `b80ae88` | with the marker |
|---|---:|---:|
| `airflow/dags/scrape_listings.py` | 76% | **23%** |
| total | 75.82% | 75.64% |

Nothing failed. The tests passed in the Airflow venv, the step went green, and
0.18 percentage points was the entire signal that a dependency-free test had
been moved into the heaviest interpreter in CI. Stage 2 unblinded that
instrument five stages ago; this is the first time it has caught something on
its own.

**The root cause of both wrong answers is one bad default: the directory was
treated as ground truth and the file adjusted to match it.** The two flush files
above came out right for a reason that does not generalise — their waiver said
`"1, not 4"`, so the mechanism supplied the answer. `scrape_listings.py` makes
no `Layer N` claim, so no waiver named it, so nothing prompted the same
question, and unprompted judgement picked the wrong invariant twice in a row.
That is this plan's own thesis about itself: where a mechanism exists the answer
is right, and where one does not the answer is whatever someone assumed.

**The mechanism that would have caught it is not written.**
`test_every_integration_suite_is_invoked_by_a_ci_step` asks whether a
*directory* appears in a step's arguments. A directory is not a suite, and no
assertion here can currently distinguish "this file runs in CI" from "the
directory containing this file is named in a `run:` line" — nor "this file is in
`tests/integration/` and needs nothing that makes it one". Both need collection
run against each step's real arguments and marker expression. Stage 10 already
owns CI selection and is the place for it.

#### The instrument was weaker than its own docstring

Moving the two flush files into `tests/integration/sql/` made
`archiver/sql/lake_snapshot_selectors/cooldown_events.sql`'s waiver go stale —
the rule now considered that file covered. It is not covered. The stem
`cooldown_events` had matched inside the *table name*
`staging.blocked_cooldown_events`, because the check asked
`Path(relative).stem not in layer_2` — a bare substring test over the
concatenated source of every Layer 2 module.

The move only surfaced the bug; two more files were already being credited the
same way and would have gone on being credited if nothing had moved:

| `.sql` file | Credited by | What that is | Since |
|---|---|---|---|
| `lake_snapshot_selectors/price_drop.sql` | `test_price_drops_no_filter` | a test method name | before the move |
| `lake_snapshot_selectors/stale_listing.sql` | `test_price_stale_listing_is_also_held_by_the_backoff` | a test method name | before the move |
| `lake_snapshot_selectors/cooldown_events.sql` | `staging.blocked_cooldown_events` | a table name | the move |

None of the three is executed by anything. The match is now on a word boundary,
and **G14 is 56 of 76, not 54** — the census undercounted, and Stage 7's scope
grows by two files.

This is worth more than its size. The plan's own claim is that *"the waiver
list can only shrink, and three assertions enforce that"*, and progress being
**mechanically visible** is what makes the plan schedulable. A checker that
credits a `.sql` file for a substring inside an unrelated identifier lets the
list shrink for free — the same failure as the paraphrased SQL its own docstring
calls out, one level up, in the thing doing the measuring. Deleting the
`cooldown_events` waiver on that reading would have recorded a repair that
never happened.

**Correcting a waiver count upward is allowed and this is the case for it.** The
rule the plan states is that a waiver may not be *deleted* without a repair;
nothing forbids the instrument getting more accurate and finding more. What
would be forbidden is quietly keeping 54.

#### Coverage is a unit-test instrument, and the integration answer is already built

The `scrape_listings` refile raised a question worth settling once, because
Stage 6 will ask it again: the 7 tests lost their coverage measurement by moving
into an integration job, so should the integration jobs run `--cov` and
`coverage combine` into one number?

**No.** Combining answers *"was this line executed by any test"*, which fuses
two claims of very different strength — a line pinned by a fast isolated test,
and a line that happened to execute while a request flowed past it. A logging
call or an unasserted error branch touched incidentally by a `TestClient`
request would read identically to a line with a dedicated unit test behind it.
The number would go up and the average evidence behind it would go down, which
is the same defect as the substring match [two sections up](#the-instrument-was-weaker-than-its-own-docstring)
and the same one this contract already names about paraphrased SQL: an
instrument that gets easier to satisfy as it gets less meaningful.

The contract settled this before the question was asked:

> **Not a coverage percentage.** The floor is: every route reached through the
> app, every production statement executed against a real engine, and every
> failure branch that another service's behaviour depends on. Coverage
> percentage is an instrument for finding gaps, not the definition of one.

**So "fully covered" for integration is not a percentage of lines — it is a
complete enumeration of the surfaces that must be exercised, and this repository
has already built three of them.** They are this plan's own waiver tuples:

| Instrument | Denominator | Standing after Stage 5 |
|---|---|---:|
| `CI_INVOCATION_WAIVERS` | every integration suite | **0** — closed by Stage 1 |
| `ROUTE_WAIVERS` | every route in each app's schema | 12 unreached |
| `LAYER_2_WAIVERS` | every production `.sql` file | 56 unexecuted |

Each has a real denominator derived from the repository, cannot be satisfied by
touching a line, and is *complete* when its tuple is empty. That is the
integration coverage number, and Stages 6 and 7 are what move it. Line coverage
stays what it is good at — unit tests, where the question genuinely is whether
the line was exercised — measured on the one job that runs them.

Which inverts the reading of the `scrape_listings` incident above. The 0.18
points were not an instrument gap to be engineered away. **The instrument was
working**: it complained because a unit test had been moved out of the job that
measures unit tests, and it was right to.

### Evidence — Stage 5b, separating production scripts from spent ones (CAR-55), 2026-09-01

**Fourteen scripts and seven test files moved, as renames.** `scripts/oneoff/`
holds the work whose owning plan has archived; `tests/scripts/oneoff/` mirrors
it. Everything else stayed at `scripts/`, and `git log --follow` carries the
history rather than a manifest recording it. Shipped in `e954306`, PR #325.

| | before | after |
|---|---|---|
| Statements in the denominator | 19,733 | **13,588** |
| Reported coverage | 75.91% | **77.66%** |
| `--cov-fail-under` | 74 | **75** |

[Run 33474748500](https://github.com/whitewalls86/new_car_tracker/actions/runs/33474748500),
all ten jobs green in 127s: `Required test coverage of 75% reached. Total
coverage: 77.66%`, with `Documentation tests` the only skip — which is the
correct classification for a branch that touches production paths, and the
first exercise of the new `heavy` gate.

**Linux and this checkout read the same number**, 13,588 statements and 3,035
missed on both. Stage 2 found seven statements that differed between Linux and
Windows and set the floor two points low to absorb them; that spread did not
appear here, and the two points of headroom carried forward on the same
reasoning rather than on a new measurement.

#### The classification needed a third step the design did not name

The stage's design section above says the bucket falls out of a join —
docstring plan number against `completed_plans.md`, overridden by the
binding-reference grep. That is two of the three steps actually required.

**Nine Python scripts declare no plan, not five.** Four beyond the design's
list — `diff_log_analysis.py`, `estimate_recompression_savings.py`,
`recompress_bronze_html.py` and `rewrite_parquet_layout.py` — so the residual
the design called "four files to read" was really nine.

**What closed the gap was a reverse join: the script's own name, grepped back
through the archive and every plan document.** It settled six of the nine with
no judgement at all. `estimate_recompression_savings.py` is the clearest case —
Plan 116's archive row names the script outright, so the archive already
contained the answer, read from the other end. Only three files
(`backfill_unlisted_silver.py`, `diff_semantic_duplicate_html.py`,
`audit_normalized_parquet_layout_once.py`) were genuinely read by hand.

**The reverse join belongs in the method, not only in this section.** A script
that declares its plan is the easy case; a script that does not is exactly the
one whose classification a future reader will have to reconstruct, and the
archive is where the answer already lives.

#### Three claims in the design section above were wrong

Recorded rather than quietly fixed, because the stage's own thesis is that an
unasserted claim goes false without anyone noticing.

**"No deploy surface is edited" is wrong by one.** `docker-compose.yml`
defines a profile-gated `april-processor` service whose documented invocation
is `python -m scripts.reconcile_april_detail`. One comment line was edited. The
20-reference count across 11 surfaces also missed four:
`docker-compose.lakehouse.a3.yml`, `maintenance-running-set.txt`,
`healthcheck-exemptions.txt`, and `.claude/settings.json`, which is what binds
`public_surface_gate.py` through a `PreToolUse` hook.

**There are 39 Python scripts, not 35.**

**`scripts/ops/` found no members and was not created.** The maintenance
bucket's rule was "human-invoked, durable, named in a live runbook", and
`host_maintenance.py` is the only script that satisfies it — but
`ops/routers/coordination.py:14` does `from scripts.host_maintenance import
HOST_VALIDATION_GATES` at module import, so the binding-reference override
keeps it in production. Shipped as a two-bucket split. The third bucket is not
deferred; it was dissolved by the same kind of override that dissolved the
coupling finding, and a future script that is durable, human-invoked and
imported by nothing can revive it.

#### The coupling finding ran the other way a second time

The design section records a coupling worry that the archive join dissolved:
two production scripts importing scripts that *looked* spent, where the owning
plans turned out to be open. The inverse case is the one it did not anticipate.

`lake_snapshot_common.py` and `seed_lake_snapshot.py` both belong to Plan 120,
which **is** archived, and neither is invoked by a workflow step. The stated
rule — archived owning plan, no binding reference — puts both in `oneoff/`.
They are imported by `download_lake_snapshot.py`, which an ops route documents,
and by `preflight_local_lakehouse_snapshot.py`, which a Compose file names. So
**a full import walk, not the reference grep alone, is what keeps them in
production**; the rule as written would have moved them and broken two
production imports. Nothing in the shipped split crosses a bucket boundary,
verified in both directions across all 39 scripts.

#### Three repairs found on the way

**`verify_testing_contract_mutations.py` could not run at all.** Stage 5 closed
G4 and deleted its row, and reworded G14 from "54 of 76" to "56 of 76", leaving
three mutations anchored on text that no longer existed. The script aborted on
its own staleness guard before reaching them — **the identical failure this
plan already records against Stage 1**, where Stage 1's deletions stranded
mutations on the removed G1 and G2 rows. Re-anchored to G6 and G14 as written.
The guard worked twice; what has not been established is a habit of running the
verifier after deleting a gap row.

**Two moved test files anchored their paths by counting parents.**
`test_audit_sectioned_html_storage.py` computed a fixture directory as
`__file__.parent.parent` and broke outright, twelve failures.
`test_estimate_recompression_savings.py` was the worse of the two: its
subprocess cases pass `cwd=Path(__file__).parents[2]` and assert only
`returncode != 0`, so "the script is not where I looked" and "argparse
rejected the flags" are the same result — a wrong `cwd` would have left both
cases **passing for the wrong reason**, invisibly. Both now resolve `tests/` by
name.

**One markdown link, and the line it drew.** Historical documents were
deliberately not rewritten: a prose mention of `scripts/x.py` inside an
archived plan records where the file sat when that plan ran, and editing it
manufactures history. A markdown *link* is a different object — it is a promise
that the target resolves, and `test_no_markdown_link_in_docs_is_dangling`
asserts it. One link in `plan_147_scrape_state_ownership.md` was repointed and
the prose around it left alone. Code, config and the two Plan 145 runbooks were
rewritten because they have to execute.

#### What the contract gained

Two assertions, both proven able to fail by new entries in the mutation
verifier, which now catches 23 of 23:

- `test_every_script_directory_is_classified` — both directions: a script
  directory the contract places nowhere, and a bucket the contract describes
  that does not exist.
- `test_every_unmeasured_script_bucket_is_omitted_from_coverage` — the prose
  and `[tool.coverage.run] omit` are one statement. The dangerous direction is
  the second: a bucket coverage omits while the contract calls it measured is
  code that silently stopped being graded.

`docs/TESTING.md` gained a *Where scripts sit* section and one row in *Where
the newer suites sit*. One row rather than two, because one new test directory
exists. `test_every_test_directory_is_assigned_a_layer` would in fact have been
satisfied without it — `_layer_of` inherits from the nearest declared ancestor,
so `tests/scripts/oneoff/` reads as Layer 1 through `tests/scripts/` — and the
row was added anyway, because a bucket that exists to be classified should say
its own layer rather than inherit one.

#### The CI zones compose, which was a second pass

`ci_change_scope.py` first shipped here with two mutually exclusive scopes,
`docs_only` and `oneoff_only`, and a changeset spanning both fell through to
the full workflow — a spent script edited alongside the note explaining why,
which is an ordinary shape of change in this repository and the one the
classifier helped with least.

It now maps a changeset to the job groups it needs, and the zones compose:

| changeset | `docs_tests` | `unit` | `heavy` |
|---|---|---|---|
| `docs/` only | yes | -- | -- |
| `scripts/oneoff/` only | -- | yes | -- |
| both | -- | yes | -- |
| anything unclassified | -- | yes | yes |
| empty, malformed, or no base sha | -- | yes | yes |

`docs_tests` fires only when the unit suite is not running, because
`pytest tests/` already contains `test_planning_docs.py` — the documentation
job is a substitute for the unit run, never an addition to it. Moving the
decision into the classifier also took the double negative out of six job
conditions, which now read `needs.changes.outputs.heavy == 'true'`.

**Fail-open is stated once.** The workflow's shell defaults are `unit=true
heavy=true`, and every path that fails to classify leaves them standing.
`test_paths_in_neither_zone_can_never_narrow_the_run` asserts that as a
property over every mixture of zones, comparing against the whole decision row
rather than the `heavy` flag alone.

The question this raised and declined — classifying the incremental diff rather
than the cumulative one — is scoped for Stage 10
[above](#stage-10-inherits-a-question-stage-5b-raised-and-declined).

#### What was deliberately not done

- **`tests/integration/scripts/` was not split**, though all three of its files
  are Plan 145's. `test_every_integration_suite_is_invoked_by_a_ci_step`
  derives the invoked set from the literal path in each `ci.yml` step, so a new
  subdirectory would have needed its own step or a `DORMANT_SUITES` entry —
  cost for no coverage benefit, since the integration jobs are not what the
  ratchet measures.
- **`verify_testing_contract_mutations.py` stayed at `scripts/`** although
  Plan 161 has archived. Plan 162 edits it, and this stage edited it twice. The
  rule "the owning plan archived" is beaten by "an open plan maintains it",
  which is the one place the archive join needed a human answer rather than a
  better query.
- **`docs/PLANS.md` was moved through the `plans` skill**, not edited here.
  Row 1 keeps its build-order position; only its slice pointer advanced.

### Evidence — Stage 6, route coverage and `container_health`'s test home (CAR-50), 2026-09-01

`ROUTE_WAIVERS` is `()`. G6 and G9 are deleted from the gap list. All twelve
routes are reached through their app's routing table by a test that asserts a
status code, `container_health` has both a `tests/container_health/` and a
Layer 4 suite, and the two misfiled unit tests are in the former.

#### Five of the twelve were never uncovered

This is the finding, and it is the second instance of one Stage 5 already
recorded under [the instrument was weaker than its own
docstring](#the-instrument-was-weaker-than-its-own-docstring).

The three `/admin/snapshots/adaptive-refresh/` reads and the two safe-lifecycle
coordination routes had tests going through `TestClient` and asserting status
codes the whole time — 200, 409 and 503 among them, including four exemplary
parametrized cases in `tests/ops/routers/test_coordination.py`. **The rule
could not see them.** `_requested_routes` matched only an `ast.Constant` first
argument, so both of the repository's ordinary ways of writing a request
vanished:

| Written as | Seen before | Where |
|---|---|---|
| `mock_client.get(f"{BASE}/latest")` | nothing | `test_snapshots.py`, 3 routes |
| `mock_client.post(path)` under `parametrize` | nothing | `test_coordination.py`, 2 routes |

So G6's census — "twelve routes reached by no test through any routing table" —
was wrong about five of them, and wrong in the direction that costs work: it
would have had someone rewrite five sound tests to satisfy a reader, leaving
the next f-string just as invisible.

**The repair widened how the argument is read, not what counts as a request.**
`_resolve_path` now resolves a module-level string constant, `+` concatenation,
an f-string whose parts resolve, and a `parametrize`-injected argument. It
still requires an HTTP-verb call, and it still yields nothing for an expression
it cannot resolve rather than guessing — because "named somewhere in `tests/`"
is the weak reading `docs/TESTING.md` rejects by name, and a reader that
degraded to it would pass 83 of 87 routes on the strength of a mention. `ops`
went from 54 to 61 request literals against 54 routes with no test added.

The three that were real gaps stayed failing until they got tests:
`GET /coordination/status` and the two `/maintenance` routes were exercised
only by calling their helpers. `/coordination/status` is the one
`scripts/host_maintenance.py` polls before it will proceed, so a rename would
have stranded the host maintenance workflow while this suite stayed green.

#### `container_health` had nowhere to put a `TestClient`, which is why G6 and G9 were one stage

Four routes were a genuine gap and could not have been closed separately. A
test is attributed to a service by its directory, so
`tests/test_container_health_app.py` could not have counted for
`container_health` even after growing a `TestClient` while it sat at the top
level. Moving it was not filing tidiness; it was the precondition.

Both files moved to `tests/container_health/` and pass unchanged (39 tests).

#### The Layer 4 suite has no database, and the substitute is a recording

`container_health`'s dependency is the Docker API over real HTTP. Standing up
the real `docker-socket-proxy` in CI was considered and rejected on a specific
fact: `collector.health_values` raises `NoContainersFound` on an empty fleet by
design, so a real proxy against a CI daemon returns 500 rather than an answer.
The suite would have needed real containers labelled
`com.docker.compose.project=cartracker` before it could assert one status code.

`tests/integration/container_health/` therefore serves a corpus recorded from a
real proxy through a strict fake on loopback. **Nothing is mocked** — the path
runs `TestClient` → router → handler → `DockerApi` → `urllib` → HTTP → parsing,
so the `v1.44` prefix, the `filters` JSON encoding and the two-step inspect are
exercised rather than assumed. The fake 404s anything not recorded, and the
session asserts both directions: an unrecorded request fails, and a recorded
exchange nothing asked for fails too.

The corpus was recorded against a daemon that also had an unrelated
`de-podcast` project running, which is why the project-label filter has
something real to exclude rather than a fixture built to agree with it.

**The import-time hazard was handled deliberately.** `container_health.app`
reads `DOCKER_API_URL` at module scope and builds two `DockerApi` instances
from it, so an import that happened first would point the suite at
`docker-socket-proxy:2375` and fail for a reason unrelated to the code. The
fake binds its port before the app import, in conftest module scope — the same
ordering `tests/integration/dbt_runner/conftest.py` keeps, and the
harness-decides-the-outcome rule applied to ourselves.

#### What the recording cannot see, and who owns that

A fake is a recording, so nothing in the Layer 4 suite can notice the day
Docker or the proxy changes a response shape. That is stated rather than
implied, and it has an owner:
`scripts/verify_container_health_docker_contract.py` stands up the real proxy
against a throwaway labelled fleet and asserts the live responses still carry
every field `collector.py` reads. It runs in its own
`container_health Docker contract (real proxy)` job.

This is the split Plan 141 already uses for Promtail — one corpus, two
consumers, neither importing the other, so what runs where is a CI-wiring
question rather than a code change. The script's `--record` mode is what
refreshes the corpus, so the fixtures stay re-derivable instead of hand-edited.

Both failure directions were exercised rather than assumed: a bogus required
field makes the shape check fail, and the request-set comparison fails when the
client asks for something the corpus does not hold.

#### What was deliberately not done

- **No Windows runner, and no ruff rule.** Both belong to Stage 6b, which this
  stage filed rather than absorbed.
- **The `de-podcast` containers on the recording machine were not cleaned up or
  hidden.** They are somebody else's project and their presence is the point.
- **Two plan documents still name `tests/test_container_health_app.py`** at its
  old path — Plan 136 §3a and Plan 161. They are dated records of what was true
  when written, and the gap list's own convention is that history lives in the
  plan documents.
- **The `enough` table's `container_health` row was updated, not its
  neighbours.** The other counts are a dated measurement and re-deriving them
  was not this stage's work.

#### What CI said, and what only CI could have said

Merged from run `33521767976` on `4b88d4b`, all eleven jobs green
(`Documentation tests` skipped by design on a changeset that is not docs-only).

| | |
|---|---|
| Unit suite | 3355 passed, 1 skipped, 479 deselected, 48.7s |
| Coverage | **78%** against a floor of 75 |
| `container_health` Layer 4 | **8 passed in 0.09s** |
| `container_health` Docker contract (real proxy) | **green in 16s**, verify step ~4s |

The Layer 4 suite passed in CI on its first attempt and needed no change. The
loopback fake, the background thread and the conftest import ordering behave the
same on `ubuntu-latest` as on Windows, which was the part with no prior evidence
either way.

**The real-proxy job earned its place on its first run by failing.** It died in
nine seconds on `ModuleNotFoundError: No module named 'prometheus_client'`: the
job ran `setup-python` and installed nothing. The cause is a consequence of a
decision worth keeping — the verifier imports the production label constants
from `container_health.collector` rather than restating them, because a copy of
`com.docker.compose.project` in a checker is the paraphrase failure this contract
names for SQL — and that import chain reaches `prometheus_client`. Repaired by
installing `container_health/requirements.txt`, so the pin has one source.

The repair was verified against a **cold venv**, not the development environment
that already had the package, which is the only reason the fix was known to work
before the second run rather than guessed at.

**The corpus proved portable, which was the open risk.** It was recorded on a
Windows machine against Docker 29.1.3 (api 1.52) and verified against the
runner's own daemon — a different machine, a different daemon, the same seven
exchanges. That is the property the whole two-part design rests on, and until
this run it was an assumption.

#### Three times the same mistake: citing a precedent and copying half of it

Worth recording because the shape repeated inside one stage, and none of the
three was caught by reading:

1. The contract job was modelled on `promtail-config` and copied without its
   `pip install` step. CI caught it.
2. The verifier was modelled on `verify_promtail_contract.py` and shipped
   without the test file that sits beside it. 171 uncovered statements, caught
   by reading the coverage report rather than by any rule.
3. The first pass at those tests stopped at 45% on the reasoning that the rest
   "needs a daemon". Most of it did not: `main`'s exit codes, `_capture`'s
   one-stats-read-per-capped-container rule and `_start_fleet`'s argument
   construction are all decision logic over data, and every one of their failure
   modes is silent. A dropped `--memory` does not fail anything; it removes
   `memory_capped`'s only input and the corpus quietly stops carrying a stats
   exchange for ever.

Coverage after the third correction: the verifier 45% → **99%**, the two
remaining lines being a one-line `subprocess` wrapper and the `__main__` guard.

**The sister script was cleaned up in the same pass**, unscoped and deliberately
so: `verify_promtail_contract.py` sat at 48% two files away, and the argument
that the coverage was cheap applies identically. 48% → **69%**. What it gained is
not more verdict testing but the replay *setup* — the image read from compose
(so the checker cannot agree with a version production stopped running), the
`docker: {}` envelope strip, the `service` label `_parse_entries` filters on,
and `main`'s exit codes. One test written for it asserted the wrong thing and
the code was right: absence on every attempt is a real drop, and inconclusive
means lost and then recovered.

Both scripts stop at the same line. `_run`'s `Popen` and threading needs a
daemon, and faking it would assert the shape of the mocks rather than the
behaviour of Promtail or Docker — rule 3 of what must never be mocked. That half
is CI's in both cases, which is the whole argument of this stage stated twice.

### Evidence — Stage 6b, mechanising the encoding-sensitive I/O guard (CAR-60), 2026-09-01

The stage was filed to close G13's *class* rather than repair another instance
of it, and it was allowed to conclude that no mechanism was worth building. It
did not conclude that. A mechanism exists, it fails on the exact call that broke
master, and the residue it cannot reach is now four named behaviours rather than
an open-ended exception.

#### The measurement that decided the design

`PLW1514` was the obvious answer and the stage began by sizing it. Measured on
this branch at `144db69`:

| | Sites |
|---|---|
| `PLW1514` (`--preview`, explicit selection) | **28** |
| `read_text`/`write_text` with no `encoding=` | **213** |
| Ruff's share of the class | **~13%** |

**The stage's brief recorded 22 and the number is 28.** The difference is not
drift in the repository — it is that the 22 was measured before Stage 6 merged.
The count is stated here as re-measured rather than carried forward, because a
figure quoted from a stale branch is exactly the kind of unchecked claim this
plan exists to stop.

Every one of the 28 is a directly-constructed receiver or a builtin `open`. The
shapes ruff never reports: **92 built with `/` from a fixture path** — the
idiom the defect used and nearly every fixture-writing test here uses — and
roughly 110 more on a plain name. Finding 3 of the stage's brief was correct
and, if anything, understated it.

#### The class was dormant, not live, and that changed the cost argument

The development machine is Windows with `cp1252` and UTF-8 mode off, which is
precisely the environment that exposes this. The suite on that machine, before
any change: **3401 passed in 36s.** All 213 sites were already there and not
one of them was failing.

That is the finding that ruled out the Windows runner. **A Windows job added
today would have gone green and caught nothing** — it only earns its cost when
a future commit puts a non-ASCII character through one of these calls. It bills
at twice the minutes of a Linux runner, it cannot run the Docker, dbt or
Postgres legs, so it would be a unit-only eleventh job, and
[PEP 686](https://peps.python.org/pep-0686/) is Final for **Python 3.15**,
where UTF-8 mode becomes the default and the class stops existing. The
repository is on 3.13 in all ten jobs. Paying a permanent recurring cost to
guard a class with a known expiry, against a job that catches nothing on the
day it lands, is the trade that was declined.

**This is a decision, not an omission**, and the thing it gives up is named in
success criterion 2: path separators, line endings, case-insensitive filename
collisions and locale-dependent collation stay invisible to CI.

#### Why the rule is a test and not a ruff setting

Ruff resolves a receiver by type. `Path("b.md").write_text(...)` is flagged;
`(tmp_path / "a.md").write_text(...)` is not, with or without a `Path`
annotation on the fixture. Ruff has no plugin interface, so a check that reads
these calls has to be Python, and it lives beside the route and mocker rules
because it is the same kind of rule.

**The two instruments were given the halves each reads correctly.** `PLW1514`
owns `open` and `tempfile.NamedTemporaryFile`, where type inference is the
right approach and a name-only rule would be wrong — `tarfile.open` and
`os.open` take no encoding and would be false positives. The new rule owns
`read_text` and `write_text`, which only `pathlib` defines, so the method name
is proof on its own and no inference is needed. No gap between them across
those two shapes, and no call reported twice.

**That last sentence was first written as "no gap between them" without
qualification, and it was wrong.** The two static instruments between them
cover `open`, `NamedTemporaryFile`, `read_text` and `write_text` — the shapes
somebody thought to name. They do not cover the encoding class, and the way
that was found is worth recording: this stage had already been committed when
PEP 597 was checked, and turning its `EncodingWarning` on found **21 more
sites in two shapes neither instrument could see at the time** —
`subprocess.run(text=True)` without an encoding, which decodes a child
process's output through the locale, and `logging.RotatingFileHandler`. Ten of
the 21 are production or scripts, including three in `dbt_runner/app.py`
capturing dbt's output and one in `archiver/processors/disk_usage.py`. Both
shapes are named by the static rule now, so the sentence is true again — but it
was bought rather than reasoned to, and the record says which.

The `RotatingFileHandler` instance mattered more than its count. It writes the
ops log that `ops/routers/admin.py` reads, and this stage had just pinned that
reader to an explicit UTF-8 — so the sweep had made the pair *inconsistent*
where it had previously been merely undefined. Fixing only what a static rule
can see is how that happens.

#### The runtime check that found them, and why it is not in CI

[PEP 597](https://peps.python.org/pep-0597/) is Final in Python 3.10 and adds
`EncodingWarning`, raised from inside CPython whenever a text operation falls
back to the locale encoding. Turning it on — `PYTHONWARNDEFAULTENCODING=1`,
with the warning as an error — is how the 21 sites above were found, after this
stage had already been committed. **It earned its place as a discovery tool and
was then deliberately not kept**, which is a distinction worth stating clearly
because the first instinct was to wire it into CI, and doing so failed twice in
a way that taught the actual lesson.

**It is an interpreter-wide flag, so it has no notion of whose code it is
judging.** Enabled in CI it measured dbt's and Airflow's own file handling
against this repository's policy. dbt is invoked in-process by
`tests/integration/dbt/real_build.py`, so `dbt.tracking`, `dbt.compilation` and
`dbt.parser.manifest` raised inside our pytest process; Airflow's config loader
did the same in the isolated venv job. Neither is our read passed downward —
both are third-party code doing its own I/O on its own files, which this plan
has no standing to fail a build over.

**The escape hatch made it worse rather than better.** Silencing a module by
name is the only lever the warnings machinery offers, and each ignore revealed
the next frame down the same call chain: ignoring `airflow.configuration`
surfaced stdlib `configparser`, one layer beneath it. Two CI rounds, each
~2.5 minutes, with no way to know how many remained — and no way to find out
locally, because that suite only exists inside a CI-only venv.

**And the attribution those ignores depend on is not reliable.** The same
`configparser.read()` with no encoding was blamed on **the calling file**
locally and on **`configparser.py:739`** in CI. So a module-scoped ignore added
for a library's sake can silence the identical defect in our own code, without
a trace. An exception list that cannot be trusted to mean what it says is worse
than no exception list, because it reads as coverage.

**The scope test settles it.** This stage exists because a test written on one
operating system behaves differently on another — *our* tests, *our* fixtures,
*our* subprocess calls. A guard that also arbitrates dbt's internals is
answering a question nobody asked, at the cost of an unreliable exception list
that fails open. So the two shapes it found are checked the same way everything
else here is: statically, over this repository's files, where ownership is not
in question and no ignore is needed.

**What that gives up, stated rather than glossed.** The static rule only sees
shapes someone has named, so the *next* unnamed shape will not be caught by
anything. That is a real loss and it is the price of not measuring other
people's code. `EncodingWarning` remains available as a developer tool for
exactly the job it did here — run it by hand when hunting for what no rule
names yet:

```
PYTHONWARNDEFAULTENCODING=1 python -m pytest -m "not integration" -W error::EncodingWarning
```

#### The exit criterion, demonstrated rather than asserted

The stage's second criterion asks for a mechanism that fails on
`(tmp_path / "a.md").write_text("—")` with no `encoding=`. Both tools were run
against that exact line:

| Tool | Result |
|---|---|
| `ruff --select PLW1514 --preview` | `All checks passed!` |
| `test_every_text_read_and_write_states_its_encoding` | **fails** |

That comparison is kept as an assertion, not a note.
`test_the_encoding_rule_sees_the_shape_ruff_cannot` pins all three receiver
shapes the repository writes and pins the correct calls as clean, so if this
rule ever narrows back to what ruff already sees, it fails instead of going
quiet. The detection was split into `_encoding_free_text_io` for no other
reason than to make that test possible: a structural check nothing exercises
reports a clean repository whether or not it still works.

#### What was swept, and why the sweep is safe rather than merely large

All **213** sites were fixed; none were waived. The waiver list stays at 56.
Waiving instead would have taken it to 269 and broken the one property the
plan's three waiver assertions exist to protect — that the list only shrinks.

**The sweep cannot change behaviour, and that is provable rather than hoped
for.** Every one of these calls already runs in Linux CI, where the default
encoding is UTF-8; writing `encoding="utf-8"` explicitly makes them do what
they were already doing there. It was verified from both ends: green on Linux
in CI, and green on the `cp1252` machine before (3401) and after (**3403**, the
two new tests) — the platform where a wrong encoding would have shown up
immediately.

The edit was applied by AST position rather than by regex, in bytes rather than
text. Both mattered: `col_offset` is a UTF-8 **byte** offset and this
repository's docstrings are full of em-dashes, so a character-indexed insert
would have landed in the wrong column on exactly the files this stage is about;
and the working tree is CRLF, so a `read_text`/`write_text` round-trip would
have rewritten every line ending in all 50 files. The diff was checked for
mixed endings afterwards and has none.

Twenty-four lines went over the 100-character limit once the keyword was added
and were wrapped — fifteen sharing one shape, nine individually.

The 21 `subprocess` and logging sites were swept the same way afterwards. With
those fixed, the rule that now covers all three shapes passes on an **empty
waiver list**, which is the check that the sweep and the rule agree.

#### What CI said, and what only CI could have said

Green on `c7d1d33`, run
[`33539915522`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33539915522),
all ten jobs (`Documentation tests` skipped by design on a changeset that is not
docs-only). PR
[#332](https://github.com/whitewalls86/new_car_tracker/pull/332).

**It took three runs, and the two red ones are the evidence for the design.**
The local suite could not have produced either: both failures were third-party
code running inside jobs that exist only in CI.

| Run | Head | Result |
|---|---|---|
| [`33537879926`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33537879926) | `f9a702b` | 9/11 — dbt and Airflow jobs red |
| [`33538571583`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33538571583) | `27288e6` | 10/11 — Airflow job red |
| [`33539915522`](https://github.com/whitewalls86/new_car_tracker/actions/runs/33539915522) | `c7d1d33` | **green** |

The first red run failed on dbt's own `dbt.tracking`, `dbt.compilation`,
`dbt.parser.manifest` and `dbt.utils.utils`, plus `airflow.configuration` — 32
occurrences of the latter. The second, after those were silenced by name,
failed on stdlib `configparser.py:739`: the layer beneath the module that had
just been ignored, reached through the same call chain. **The escape hatch was
uncovering offenders one frame at a time, with no way to see how many were
left**, because that suite runs in a venv built only by CI.

That is the run that ended the approach rather than the one that fixed it. Two
rounds of ~2.5 minutes each bought one fact worth more than a green build: a
guard that has to be told, module by module, whose code it is allowed to judge
is not measuring what this stage set out to measure.

**The third run is green because the question changed**, not because the last
module was found. The shapes are checked statically over this repository's
files, and CI never had to arbitrate dbt's file handling at all. Nothing in the
`ci.yml` diff survives; the only workflow change in the merged branch is none.

**The waiver list is unchanged at 56.** `ENCODING_WAIVERS` is empty and joins
`ALL_WAIVERS`, so the three assertions that keep the list honest now cover this
rule too: a waiver here that stopped describing a violation would fail, as would
one naming a missing gap entry or an archived owner.

#### What was deliberately not done

- **No Windows runner**, for the reasons recorded above. This is the stage's
  substantive decision and success criterion 2 now names what it costs.
- **`.open()` on a non-`pathlib` receiver is not checked by the new rule.**
  `tarfile.open`, `os.open` and `pyarrow`'s filesystem `open` share the name
  and take no encoding, so a name-only rule would report them and be wrong.
  Ruff's type inference covers the `open` family instead, which is the whole
  point of splitting the two.
- **`PYTHONUTF8` was not set anywhere.** It would make the class disappear on
  every machine that had it, but it is an interpreter start-up flag: a developer
  running `pytest` without it still diverges, so it moves the harness dependency
  rather than removing it. Explicit `encoding=` needs no environment to be
  correct. `PYTHONWARNDEFAULTENCODING` is a different proposition — it changes
  no behaviour, it only makes the fallback audible — and it was used once, by
  hand, rather than wired into CI.
- **Bytes-mode `subprocess` calls were not touched.** Only text mode qualifies,
  because a bytes-mode call has no encoding to state. A call that gains
  `text=True` later is caught by the rule the moment it does, without needing
  to be executed.
- **The 3.15 upgrade was not scheduled here.** PEP 686 will retire this class,
  but that is a version bump with its own consequences and it is not Plan 162's
  to make.


### Evidence — Stage 7, SQL execution from both directions (CAR-51), 2026-09-01

**G14 is closed and `LAYER_2_WAIVERS` is `()`.** With it, the whole of the
original waiver list: the plan's own arithmetic was 4 + 50 + 12 + 56 = 122, and
G14's 56 was the last column standing. Every waiver that remains is one this
stage found.

| Ledger | Start | End |
|---|---|---|
| `LAYER_2_WAIVERS` (G14) | 56 | **0** |
| `INLINE_SQL_WAIVERS` (G5) | rule did not exist | 15 |
| `SQL_LITERAL_WAIVERS` (G15) | gap did not exist | 19 |
| `DUPLICATE_SQL_WAIVERS` (G17) | gap did not exist | 1, waived with a reason |
| production `.sql` files | 76 | 141 |
| production `.py` scanned for SQL | ~100, across 8 packages | 156 |
| Layer 2 tests | 129 | **242, all executed in CI** |

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

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
`production_python_files()` is a second derivation, reading Stage 5b's declared
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
though all five execute it — and it was the file Stage 5 caught being credited
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
`REQUIRE_AIRFLOW_SCHEMA` since Stage 3.

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
  [Plan 125 Gate D2](plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration)
  has chosen a serving pattern: under "DuckDB as a non-authoritative Iceberg
  cache" it is a no-op for all 26 affected files. It sits in Stage 11 with the
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
Stage 6c predicts rather than a contradiction of it: every service in this
deploy maps to at least one surface, and `dashboard` and `pgadmin` — the only
two that do not — were not in the set.

#### Cost

Estimate 2 points, actual 1. The stage was the plan's largest by file count —
18 commits, 118 files, +3,300/−650 — and cost less than its estimate because
almost all of it was mechanical once the rules existed. **Building the checker
first is what made it cheap**, and it is the reusable lesson: the rule found
`maintenance.py:152`, `admin.py`'s six, and the byte-identical trio, none of
which a reading pass had found in three prior stages of looking at these files.

### Evidence — Stage 6c, a service that pauses no surface can be deployed alone (CAR-66), 2026-09-02

**The defect is closed in production.** `V050` applied 2026-09-02, and `bash
scripts/redeploy.sh dashboard` — the exact command that returned 503
`{"detail":"Database unavailable."}` on 2026-09-01 — now completes end to end.

| Deploy | Drain | Healthy | Exit |
|---|---|---|---|
| `ops` | 5s | 6s | 0 |
| `dashboard` alone | **0s** | 6s | 0 |
| `archiver pack-worker processing scraper dbt_runner` | 1s | 8s | 0 |

**"Drain confirmed after 0s" is this stage's own prediction, observed.** V050's
comment argued that an empty scope is a true statement rather than a missing
one, because `required_drain_sources(frozenset())` is empty and every source
reports not-applicable. The `dashboard` deploy drained in zero seconds where
every scoped deploy above it took one to five. The readers already agreed; only
the constraint did not.

**The constraint keeps the invariant worth keeping.** Verified against
production `pg_constraint` after the migration: the `scope <> '[]'` clause is
gone and `targets <> '[]'` remains, so an active record must still name what it
coordinates. Coordination advanced generation 59 → 65 across the three deploys
and returned to `phase='none'` after each.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

#### The rollout found a gap in the deploy service list, of this stage's own kind

`shared/db.py` changed, and every service that bakes it needed rebuilding. The
recorded list was archiver, pack-worker, snapshot-worker, processing,
april-processor, scraper and ops. Measured against the tree, it was wrong in
both directions:

- **`dbt_runner` bakes `shared/` and was absent from the list** —
  `dbt_runner/Dockerfile:19` copies it to `/usr/app/shared/`, and the service
  is `restart: unless-stopped`. It would have kept the old module indefinitely.
- **`snapshot-worker` and `april-processor` are `profiles:`-gated** and were
  not running, so they are not deploy targets at all; they load new files on
  their next invocation, as Stage 7 recorded for `docker compose run --rm`.

`container_health` copies only its own package and `lakehouse` is not a Compose
service, so neither is affected. **This is Stage 6c's defect one layer out** —
a contract (which images bake `shared/`) and its consumer (the list an operator
types) with nothing composing them, and the same failure mode: the list looked
right and was never asserted against the tree.

#### Cost

Estimate 1 point, actual 1. The stage was sized before the diagnosis was
written down and still landed on its estimate, which is worth recording as
plainly as an overrun would be: the expensive half was already spent finding
the cause on 2026-09-01, and what remained — one migration, two exception
paths, one shell function and the assertion — was the cheap half. **The
unmasking cost almost nothing and is the part that pays later**, since the next
unrelated failure on this path will name itself.
