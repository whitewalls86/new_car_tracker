# Plan 162 Stage N — the DAG tree's `.sql` convention

**Legacy:** Stage 9 · **Issue:** CAR-53 · **Closed:** 2026-09-02

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage N. It carries the summary; the sections below are the detail.

---

#### What actually remained was one statement, invisible to three instruments

`sensors.py`'s admission `SELECT`, inline at `hook.get_first(...)`. It escaped
every rule at once:

- **G5 could not see it.** `production_python_roots()` has included
  `airflow/dags` since Stage L, but `_SQL_CALL_NAMES` held DB-API, DuckDB,
  Spark, pandas and SQLAlchemy names and no Airflow ones. A `PostgresHook`
  method is not a cursor.
- **G14 could not count it.** It was in no `.sql` file.
- **The only thing asserting on it was a paraphrase.**
  `test_coordination_admission.py` asserted `"scope ? 'host'" in sensor_source`
  — a substring match against Python text obtained with `ast`. Nothing executed
  it against Postgres.

The repair: the statement moved to `airflow/sql/deploy_intent_gate.sql`, loaded
by a new `airflow/dags/dag_queries.py`; six Airflow names joined
`_SQL_CALL_NAMES`; and Layer 2 gained five tests that execute it.

#### The exemption is from the loader clause, not the file rule

G12 closes as a decision, and the decision is narrow. `shared/query_loader.py`
is two lines, and reaching it means mounting `shared/` into an image that is
`apache/airflow:3.2.0` plus four providers. That would put `minio`,
`duckdb_s3`, `iceberg_catalog` and `packfile` on the DAG tree's import path
with boto3, duckdb and pyiceberg absent — an import resolving in the main venv
and failing **at DAG-parse time in production**. The same dependency isolation
already forces a separate Airflow venv in CI. What is exempted is the shared
import; what is not exempted is inline SQL, which now fails like anywhere else.

#### The name had to be `dag_queries`, and the suite proved it

Written first as `airflow/dags/queries.py`, mirroring every service. It passed
`tests/airflow/` in isolation and failed 46 tests in the full run:

```
ImportError: cannot import name 'DEPLOY_INTENT_GATE_SQL' from 'queries'
             (.../scraper/queries.py)
```

Airflow puts `dags/` on `sys.path` directly, so a module there competes in the
**top-level** namespace — and bare `queries` is already taken by
`scraper/queries.py`, imported flat because its Dockerfile does `WORKDIR /app;
COPY scraper/ .`. That is [G18](../TESTING.md#the-gap-list)'s dual import
identity reached from a third direction, after Stage M found it in
`scraper/app.py`'s `from db import`. It would not have appeared in production,
where only `dags/` is on the path — which is exactly what makes it worth a
prefix: the collision is silent where it is wrong and loud only by luck of
suite ordering.

#### Both guards were mutation-tested rather than assumed

Goal: an inline statement or a reordered select list must fail *loudly*.

| Mutation | Result |
|---|---|
| `hook.get_first("SELECT count(*) FROM listings")` added to `orphan_checker.py` | `test_no_production_module_holds_sql_at_its_execute_call_site` fails, naming `airflow/dags/orphan_checker.py:66` |
| A new `.sql` file with no Layer 2 test | `test_every_production_sql_file_is_touched_by_a_layer_2_test` fails, naming the file — observed for real, before the test was written |
| `SELECT di.intent, cs.phase,` → `SELECT cs.phase, di.intent,` | 47 unit failures plus the Layer 2 column-order test |

The third is the one that matters. `poke()` reads `row[0]`..`row[3]` because
`PostgresHook.get_first` returns a tuple, so **column order is production
behaviour**: swapping the first two makes `row[0] != "none"` test `phase`,
which admits DAG runs during a deploy. That is a corrupted run rather than an
error, and it is now asserted from both ends — the text order in
`tests/airflow/`, the engine's order in `tests/integration/sql/`.

#### The Layer 2 tests were run against a real Postgres, and three failed first

Executed locally against `postgres:16` with all 50 Flyway migrations applied,
because the alternative was reasoning about the statement. Three of the five
new tests failed on first run:

```
psycopg2.errors.CheckViolation: new row for relation "coordination_state"
violates check constraint "coordination_state_check"
```

V043's table constraint, as amended by V050, ties the columns together: a phase
other than `none` requires a non-null `kind` and a non-empty `targets`. Setting
`phase` and `scope` alone is rejected. A fixture that seeded rows the real table
would refuse proves nothing about the real table — which is Layer 2's whole
argument, arriving as a failure in the stage that was writing Layer 2 tests.

One of the five is worth naming: **V050's own safety argument was untested
until now.** Its comment says an empty scope is safe because "in
`airflow/dags/sensors.py` `cs.scope ?| %s::text[]` is false against an empty
array, so no DAG blocks on it". That is a claim about this statement, and
nothing executed it — the migration's stated reasoning rested on a reading of a
Python string. `test_an_empty_scope_intersects_nothing` now asserts it against
the engine.

#### What Stage N did not do

No compose change and no container recreation, so nothing here needs a deploy:
`airflow/sql/` and `airflow/dags/` are directory bind mounts, and a `git pull`
makes both the new `.sql` file and `dag_queries.py` visible to the scheduler
with no image rebuild. The plan flagged this as the one slice that changes
production import structure; it changed the DAG tree's imports and deliberately
left the image alone.

`_record_observation`'s docstring still explains its module-level SQL constant
in terms that Stage L superseded. True but stale, and out of scope here.

#### Rules 5b and 5c became one rule, and that reverses a Stage L decision

**Added 2026-09-02, at the maintainer's challenge, after Stage N's first fix
was the wrong shape.** Stage N originally closed its half of the SQL rule by
adding six Airflow method names — `get_first`, `get_records`, `get_pandas_df`,
`get_df`, `get_first_or_none`, `SQLExecuteQueryOperator` — to
`_SQL_CALL_NAMES`. The maintainer's objection was one sentence: *"Lists are
inherently an issue. Lists go stale. A new tech or new way to call these things
means silent drift."*

That is [the first rule of this contract](../TESTING.md#the-first-rule) turned
on the instrument: *a test you can silence by appending to a list reproduces
the defect it was written to catch.* Stage L had already written the general
form — *"a denominator that is listed, or scoped to what exists when it is
written, will be wrong"* — and applied it to the **scan surface**,
`production_python_files()`, which is why `airflow/dags` was in scope at all.
It did not apply it to the **detector**. So the surface was derived and the
trigger was an inventory, and the fourth instance of this plan's recurring
mistake was sitting inside the rule written to catch the third.

**Three sites proved it was not theoretical**, found by asking what the literal
*is* rather than where it sits:

| Site | Shape | Why both rules missed it |
|---|---|---|
| `ops/coordination_drain.py:77` | inline arg to `_database_count(...)` | a **project-local helper** — no inventory of database libraries can contain your own function names, at any list length |
| `scripts/compare_gate_b_parity.py:510` | dict value in `TIE_QUERIES` | not a call site (5b) and not an assignment (5c) — a third shape |
| `scripts/compare_gate_b_parity.py:527` | dict value in `TIE_QUERIES` | as above |

The first is production code that gates deploys.

**So 5b and 5c collapse into 5f: no production module holds a SQL statement.**
Keyed on the statement's own grammar — a SQL verb followed by a clause keyword
— and never on its container. The set of ways to *invoke* SQL in Python is open
and grows with every library and every helper anyone writes; the set of ways to
write a string literal is closed. Only the closed one can be asserted without
going stale.

**"Not in Python" is exactly "in a `.sql` file", which is why one rule replaces
two.** There is nowhere else for a statement to live, and paired with Rule 5 —
every `.sql` file is executed by a Layer 2 test — the loop closes with no
judgement in it: a statement cannot be in Python, so it is in a file, and the
file is executed in CI.

#### The detour worth recording: an exemption that decided nothing

The first draft of 5f kept the contract's *"structurally generated statements"*
exemption and tried to make it mechanical, narrowing it to "a dynamic
identifier, which SQL cannot parameterise". The maintainer asked the question
that killed it: **what decides that programmatically?** Nothing does —
`f"SELECT * FROM {x}"` and `f"SELECT * FROM t WHERE a = {x}"` are the same AST
shape, and only meaning separates them. An exemption that needs a human to
adjudicate is a waiver list with extra steps.

The resolution is that the rule needs no exemption at all. It fires on
everything; a genuinely forced case takes a **waiver**, dated and owned, which
is the contract's existing and only sanctioned escape hatch and which it
already calls *"a decision, not a convenience."* Judgement moves to the
exception, where it is visible, and the default becomes failure rather than
silence.

Testing that reasoning against the two sites actually claiming the exemption
found they never qualified. `task_instance_query()` and
`gate_observation_query()` interpolate **placeholder counts**, never
identifiers, and both have static equivalents — verified against `postgres:16`:

| dynamic | static | result |
|---|---|---|
| `JOIN (VALUES (%s,%s), …) AS drained(dag_id, task_id)` | `JOIN unnest(%s::text[], %s::text[]) AS drained(dag_id, task_id)` | identical rows |
| `state IN (%s, %s, %s)` | `state = ANY(%s)` | same true, same false |

The rewrite also **deletes** a special case rather than adding one: both
builders return `None` on an empty scope because `(VALUES )` is a
`SyntaxError`, while `unnest(ARRAY[]::text[])` is legal and returns zero rows,
so the `if query is None` branch at every call site exists only to serve the
dynamic form. They are waived here rather than rewritten — that is drain logic
gating production deploys, and it does not belong in the same commit as a
contract change — but they are waived as **ordinary debt with a proven fix**,
not as a blessed category.

#### Not reinventing a wheel, and the check is recorded

Asked whether this exists in the wild. The **detector** does, and converged
independently: `flake8-sql` treats a string as SQL if it holds "select from",
"insert into values", "update set" or "delete from" *in order* — verb plus
clause, the same grammar this arrived at by measurement. The **rule** does not.
`flake8-sql` (Q440–Q449) and `sql_str_lint` both *style* the SQL they find,
taking "SQL lives in Python" as the premise. Ruff's `S608` is an injection
check: run against the statement Stage N moved out of `sensors.py` — correctly
parameterised, no interpolation — it passes clean, and only fires once the same
query is rewritten as an f-string. Semgrep could express 5f but ships no such
registry rule.

It stays in pytest rather than moving to Semgrep because the contract's
machinery — `Waiver`, the gap list, `_assert_exactly`, the mutation harness —
already lives there, and a second tool would need a second waiver mechanism
outside all of it. `S608` is worth enabling separately as a maintained backstop
for the interpolation half: 55 sites today, 31 outside `scripts/oneoff/`, so it
wants its own slice.

#### What the change cost, and what was verified

`_SQL_CALL_NAMES` is deleted, not lengthened. Both waiver ledgers are kept
separate — they record which gap each site came from, that attribution is
history, and the mutation harness anchors on `Waiver(subject, gap="G5",
owner=162)` as literal source text. G5 and G15 stay in the gap list as
**superseded** rows rather than being deleted, for the same anchoring reason;
Stage M found 17 of 24 mutations had silently stopped running when their anchor
gap closed, and that failure is not worth repeating three days later.

Ledger totals moved 15 + 19 = 34 to 16 + 23 = 39: three genuinely new sites,
and two more because line numbers now anchor on the literal rather than on the
call, which shifted a handful by one. One waiver is a false positive kept
honestly rather than engineered around —
`scripts/verify_container_health_docker_contract.py:335` is argparse help text
reading "refresh the committed corpus from the live API", which is `REFRESH` …
`FROM` in English. Rewording production text to satisfy a linter is worse than
a dated waiver saying why.

Verified: 3,523 unit tests pass, ruff clean, **all 24 contract mutations still
caught**, and the four shapes 5b and 5c could not see between them are caught
by 5f — including `newdb.run_statement("UPDATE t SET a = 1")`, a library that
does not exist and never needed to be listed.

#### The two builders were rewritten, not waived

**The waiver written two sections above was removed the same day.** Recording
that a statement is debt is not the same as clearing it, and this plan's job is
to bring the repository up to the standard rather than to inventory its
distance from it.

Three statements left `ops/coordination_drain.py` and became files:

| Was | Now | Shape |
|---|---|---|
| inline at `_database_count(...)` | `ops/sql/select_processing_artifacts_backlog.sql` | plain literal, never structural |
| `task_instance_query()` f-string | `ops/sql/select_airflow_task_instances.sql` | `(VALUES …)` → `unnest(…::text[], …::text[])`, `IN (…)` → `= ANY(…)` |
| `gate_observation_query()` f-string | `ops/sql/select_airflow_gate_observations.sql` | `(VALUES …)` → `unnest(…::text[])` |

Both query functions survive as `(sql, params)` returners so no call site
changed shape, but they now build **parameters only** and the SQL beside them
is a constant. `task_instance_params()` and `gate_observation_params()` are
split out so the parameter arithmetic is testable without the statement.

**Equivalence was proven, not asserted.** Old and new were executed
side-by-side against `postgres:16` with Flyway's 50 migrations applied, over
every admission scope, and the results are identical everywhere:

```
scope                  task_instances   gate_obs
airflow_control                     1          1
analytics                           9          6
archive                            13          8
database                            1          1
detail_fetch                       10          3
listing_fetch                       2          1
processing                          4          2
<ALL>                              25         15
<EMPTY>                             0          0
```

The non-zero counts matter as much as the equality: an all-zero table would
have made "equivalent" vacuous.

**The `<EMPTY>` row is the special case the rewrite deleted.** `(VALUES )` with
no rows is a syntax error, which is why both builders returned `None` on an
empty scope and both call sites translated that back into `_known(source, 0)`
by hand. `unnest` of an empty array is legal and yields no rows, so the engine
answers zero directly. Two branches in production and two more in tests are
gone, and `tests/integration/sql` now asserts the empty scope against Postgres
rather than against a Python `if`.

#### Four things the rewrite broke, each worth naming

None was a surprise, and each is a test that was doing its job.

1. **Rule 5e fired on a comment.** The new `.sql` file explained the change by
   quoting the placeholder syntax it replaced — and psycopg2 counts
   placeholders inside comments as part of the statement, so a comment naming
   one makes the caller pass too few parameters. The comment now says so
   instead of demonstrating it.
2. **A unit test asserted `"retry" not in sql`.** True of the old inline
   statement; false of the file, whose comment explains that pending and retry
   rows are backlog. Fixed by stripping comments before the substring check,
   which is what the assertion meant in the first place — a rule about the
   *statement* should not be decidable by its prose.
3. **`params[-1]` and `params[:-1]` stopped meaning what they meant.** Four
   tests indexed the flat parameter tuple positionally. Parameters are now
   `(dag_ids, generation)` and `(dag_ids, task_ids, states)`, so they unpack by
   name.
4. **`sensors.py` gained an import the by-path loader did not know about.**
   `_load_sensors()` execs `coordination_contract` and `sensors` by path with
   Airflow stubbed; `dag_queries` had to join that list.

#### The Layer 2 census demanded the new files, immediately

Adding three `.sql` files failed
`test_every_production_sql_file_is_touched_by_a_layer_2_test` before a single
test was written for them — the loop closing on itself in the same commit that
widened it. Two needed the suite to name the constants rather than call the
builders, which is the stronger reading anyway: naming
`SELECT_AIRFLOW_TASK_INSTANCES` proves the file is executed, while calling
`task_instance_query(...)[0]` proves only that *something* was.

Six of the drain's Layer 2 tests skip locally because they need the real
`airflow.task_instance` and `airflow.dag_run` from `airflow db migrate`; CI
sets `REQUIRE_AIRFLOW_SCHEMA=1`, so there a missing schema is a failure rather
than a skip. The equivalence run above stood those two tables up directly,
which is what made the comparison possible outside CI.

**Ledger effect: three waivers deleted, none added.** `INLINE_SQL_WAIVERS` is
16 → 15 and `SQL_LITERAL_WAIVERS` 23 → 21. Verified: 3,523 unit tests pass,
ruff clean, all 24 contract mutations still caught.
