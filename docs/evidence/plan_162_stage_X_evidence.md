# Plan 162 Stage X — the closing record, 2026-09-06

The stage's third evidence file and the one its `## Record` entry links. The
other two keep their own scopes and are not restated here:
[the origin](plan_162_stage_X_origin_2026-09-04.md) records how the stage was
arrived at, including the correction its prior-art table needed once the stage
existed; [the recorder baseline](plan_162_stage_X_recorder_baseline_2026-09-05.md)
records the local reading taken while DuckDB is still authoritative, and the
contract-drift findings this stage measured and deliberately did not fix.

## 1. Where test SQL went, and the provenance decision taken at the top

506 SQL literals under `tests/` → **0**. 483 were `.execute()` arguments and
moved mechanically into **381 files under `tests/sql/`**, which mirrors the test
tree down to the module — so a statement's owning module *is* its path and no
registry is needed. `tests/` stays exempt from `production_sql_files()`, so
nothing here inflated the production denominator or owed a Layer 2 test.

`shared.query_loader.load_query` returns `SqlText`, a `str` subclass carrying
**the set of files its text was composed from**, preserved through `.format()`.
That is the provenance decision the exit required be taken at the top rather
than in the middle. The alternative — matching a rendered statement back to its
template turned into a pattern — is approximate and brittle on a multi-line
placeholder. A *set* rather than one path because a statement formatted into
another belongs to both files, which was worth fourteen files (§3). Every driver
still sees a `str`.

**The judgement rule left rather than a mechanical one arriving.** Plan 161
question 3 exempted test SQL for one stated reason: fixture seeds are SQL in test
files too, and a checker that cannot tell a seed from a paraphrase fails on
correct code. That premise stops holding once no SQL literal appears under
`tests/` at all. `test_no_test_module_holds_a_sql_statement` is the **same**
`_is_sql_statement` predicate the production rule uses, pointed at a second file
set — not a variant, because two copies of a grammar are two rules that can
disagree. Pointing it at that surface is what improved it, and both fixes apply
to production too: `MERGE` now requires an adjacent `INTO` (without it git's own
vocabulary reads as SQL — "Merge origin/master into a-branch" is a commit
subject in `test_commit_msg_hook.py`), and DDL is no longer exempt when it
materialises a query.

## 2. The aggregation this exit had deferred, and what Stage Q inherits now

The exit is explicit that two things are **not** in it: the cross-engine
assertion, which needs two live engines and belongs to Plan 125 Gate D, and the
aggregation — a per-job artifact and a gate job — which "lands with or after
Stage Q and takes the replacement of the Layer 2 name-match reading with it."

The cross-engine assertion is correctly absent. **The aggregation is not: it
landed here.** `scripts/check_sql_execution_coverage.py` runs downstream of every
pytest job and merges their records, which is the half no single job can be since
a statement may execute in any of six.

Recorded because it changes a later stage's scope rather than this one's:
[Stage Q](../plans/plan_162_testing_census_and_restructure.md#stage-q-cis-services-are-productions-in-definition-and-in-contents)
inherits less than its section claims, and should be re-read before it is sized.

## 3. Three instrument defects the gate found, and two holes under the denominator

Every reading below 161 was the instrument, not the repository, and the gate
found all three rather than a reviewer.

**CI was throwing the record away.** Every pytest job wrote its slice and only
some uploaded it, so the gate measured a whole-repository number from a fraction
of the record. Fixed by uploading from each job and merging in the gate job.
`test_every_job_that_runs_pytest_has_its_record_read_by_the_gate` derives the
owing set from which jobs run pytest rather than listing it — the listed version
is what failed — and writing it found a second instance immediately: the gate did
not wait on `docs-tests`, which uploads a record.

**Two loaders lost the origin.** `read_text()` instead of `load_query`, so
statements reached a real Postgres with nothing able to say which file the text
came from. The gate's "executed but unattributable" check names a file whose
exact text reached an engine with nothing saying so; that section is now empty.

**Fourteen statements were nested, not untested.**
`archiver/processors/lake_snapshot_cohort.py` formats a selector's own statement
into `wrap_candidate_query.sql` and executes the result, so a single origin
credited the wrapper and left all fourteen selectors reading as never executed.
The reading was wrong in the expensive direction: acting on it meant writing
fourteen Layer 2 tests for statements that already run in CI. **Fixed in the type
rather than in the gate** — `SqlText.origins` is a set and `.format()` unions in
the origins of any `SqlText` argument — so nesting is a case the instrument
answers instead of a case that quietly loses a file. It is also why the gate's
failure message now names its own blind spot *before* telling anyone to write a
test.

**And two holes under the denominator itself**, since `production_sql_files()`
is what every coverage number here is a fraction of:

- **`_SQL_EXEMPT_ROOTS` was a tuple no test read.** Adding `dashboard/` drops 24
  files and the gate reports 137 of 137, green. It is now asserted both ways
  against a table in `docs/TESTING.md` stating why each root is not production
  SQL, with a stale direction for a root that no longer holds any `.sql` file.
- **An empty corpus passed.** A set difference over an empty corpus is empty, so
  a broken glob would print `0 of 0` and exit green.
  `test_there_is_something_to_check` guards the test corpus this way; the
  production side never had one, and now does.

## 4. A count the stage was scoped by was already wrong

The exit said the split moves 7 mechanical / 4 judgement to 8/3. Judgement went
**4 → 3** as intended — the test-SQL rule is struck from `docs/TESTING.md` and
from the reviewer skill. The mechanical half was never 7: the mechanically-checked
table held **12 rows before this branch and holds 19 after**.

Only the judgement half is stated where anything checks it, which is why it
stayed true while the other drifted. Recorded rather than corrected quietly,
because a plan whose subject is counts that stop being true is the last place to
quietly correct a count.

## 5. G19 drained 25 → 1

The ledger landed at 25 on the reading `"{" in text`, which is wrong twice over.

**Seven were not templates.** `'{"makes": ["test"]}'::jsonb` is a JSONB literal:
the braces sit inside a quoted SQL string, `%s` beside them is a real bind
parameter, and the statement plans exactly as written. Reading those as templates
held seven statements out of the schema check they would have passed — a coverage
hole wearing a known limit's clothes, which is worse than an open gap because the
ledger made it look decided. The detector now strips SQL string literals before
looking for a placeholder.

**Seventeen were templates whose bindings the call site states out loud**, in a
module constant or a literal the module iterates — `RECEIPT_TABLE`,
`PROTECTED_TABLES`, `POSTGRES_SNAPSHOT_TABLES`, and the `for table in (...)`
loops. `tests/sql_bindings.py` reads the call *shape* from the owning module's
AST and the *values* from importing it. Neither half alone is enough: the AST
cannot see `RECEIPT_TABLE` because it is imported rather than assigned, and the
import alone cannot say which constant reaches which placeholder. Pairing is
preserved for `parametrize` rows, so a `schema` from one row is never combined
with a `table` from another. `test_fixture_statements` now PREPAREs every
rendering rather than skipping the file — `select_h_from_table` is planned five
times, once per protected table.

**One survives and is now a real limit rather than a backlog.**
`insert_ops_price_observations` builds `{columns}` and `{values}` with
`", ".join(...)` over a per-case list, which exists only at run time.

Four more were repaired rather than waived during the stage: `{claimed_at}`,
`{created_hours_ago}` and `{proc_event_hours_ago}` interpolated a *value* where
the driver would have bound one, and are `now() - (%s || ' hours')::interval` now.

## 6. One waiver that was prose, and why the predicate was left alone

`scripts/verify_container_health_docker_contract.py:335` carried a G5 waiver. It
is argparse help text: `help="refresh the committed corpus from the live API"` —
a SQL verb followed at a distance by a clause keyword, which is all the rule asks
for. So is *"delete the old rows from the cache"*.

This is the same shape as the git commit subjects the `MERGE INTO` fix already
handles, and the decision went the other way for a stated reason. Those fixtures
must read like real git output, so the text could not move and the rule had to.
This string is ours to word. It says **"re-record"** now — which is clearer
against the `--record` flag it documents — and the waiver went with it, because
rephrasing makes a waiver stale exactly as repairing one does.

**The predicate is unchanged, by decision rather than oversight.** The
alternative was per-verb adjacency rules — `DELETE FROM`, `REFRESH MATERIALIZED
VIEW`, `UPDATE … SET` — which is a table of SQL grammar facts to maintain, the
same shape as the `_SQL_CALL_NAMES` inventory Stage N deleted rather than
lengthened. A blunt rule whose false positives cost one reworded sentence is the
safe side to be wrong on: over-detection fails loudly, under-detection is silent
and is the failure the rule exists to prevent. The constraint is now a stated
one — do not write prose that parses as SQL.

## 7. Plan 129's statements, and the obligation that forced a testability seam

`scripts/estimate_dictionary_savings.py` carried a G5 and a G15 waiver.
**Its classification was already right.** `scripts/train_html_dictionary.py`
trains the dictionary the bronze write path compresses every object against, and
imports `collect_documents` from it, so `scripts/oneoff/` does not apply — that
bucket is *"the owning plan has archived **and** nothing binding names it"*, and
something binding names this. Plan 129 archiving on 2026-09-03 changed nothing.
[Stage G](../plans/plan_162_testing_census_and_restructure.md#stage-g-what-the-split-is-and-why-a-directory-rather-than-a-list)
had already corrected an earlier draft that counted it as spent.

What was wrong is that both statements were f-strings, so the unit suite tested
the Python around them with a stub connection — which proves the call shape and
nothing about the SQL. A renamed partition column, a dropped `minio_path` or a
window function DuckDB stops accepting were all invisible.

Both now live in `scripts/sql/` and load through `shared.query_loader`, which
puts them in `production_sql_files()` and makes them owe what production SQL
owes. **That obligation is the point rather than a cost:** it is what forced the
`silver_path` and `artifact_events_path` seams, without which no test could point
the statements anywhere but production.
`tests/integration/sql/test_estimate_dictionary_savings_queries.py` runs both
against a real DuckDB over pyarrow-written fixture Parquet, authors no SQL of its
own so the recorder credits nothing staged around them, and asserts the collapse
of repeat captures, the detail-only filter, the joined object path, the per-month
cap, reproducibility, and that the text under test is the file on disk.

`scripts/sql/` gains a row in the script-bucket table answering "yes" to the
coverage question rather than `**no**`: the directory holds no Python so the
answer is vacuous, but `**no**` obliges an `omit` entry, and omitting a directory
that could later hold Python is the silent drop that table exists to prevent.

Two defects this found in its own work, both now guarded by comments in the
files: **a brace in a SQL comment is a `str.format` placeholder like any other**
(`{path}` in prose raised `KeyError`), and **the determinism test reads the whole
file, comments included** (naming the shuffling function in a comment broke it).

Production `.sql` files 161 → 163, both recorded executing.

## 8. An authoring gap seen from outside, and the skill that answers it

Adding those two statements took **six suite runs**, and nine of the thirteen
steps were obligations learned by failing rather than by reading: that
`scripts/sql` needs a row in a `docs/TESTING.md` table; that the row's second
cell is regex-parsed and must read exactly `yes` or `**no**`; that Layer 2 is
`tests/integration/sql/` and `tests/integration/archiver/` and nowhere else, so a
test in `tests/integration/scripts/` discharges nothing; that a test may no
longer author its own SQL; and the two brace defects above.

**None of them was wrong.** Every one caught something real, two of them defects
in that commit's own files before they shipped, and the "no SQL in a test" rule
pushed the fixture from `COPY … TO` to pyarrow — a better design, since the only
statements the test now executes are the production ones. What was missing is
that the obligations are stated in five places and discoverable in one: a
traceback.

`.claude/skills/add-sql/` is the route map. It is written for a reader that does
not accumulate — `testing-contract` reviews a change against `docs/TESTING.md`
and nothing told anyone how to write one that passes, and an agent starts every
session with no memory of the last, so a rule learned by failing is a rule
learned again next week. Its three verification commands were run verbatim
against `301beb1` before it landed.

## 9. A failure this stage caused, and the guard that fixes it

The first CI run after the follow-on work failed `SQL + Airflow metadata
contracts` with `RuntimeError: Form data requires "python-multipart"`.

It reads like a missing dependency and is not. `_plannable` called `renderings()`
for **every** statement under `tests/sql/`, and reading a call site means
importing the module that owns it — which runs that package's `conftest`, and
`tests/integration/ops/conftest.py` imports the FastAPI app, whose `Form` routes
need `python-multipart`. That package is absent from the schema-contracts job by
design, because until then nothing in that job imported an app. A Layer 2 suite
that plans SQL against Postgres was importing the whole test tree to answer a
question about statements with nothing to bind.

Installing the dependency would have been the wrong repair: it makes the
dependency list pay for the defect and leaves the suite importing 380 modules it
has no business touching. The guard is the one already applied at Layer 0 and
forgotten here — **a statement with no placeholder is its own rendering and needs
no call site**. Imported modules went from every test module to four, asserted
directly rather than assumed.

## Verification

CI green on `2906edf` across 14 jobs — 13 success, one skipped (`Documentation
tests`, correct by design on a full run).

```
read 10010 executions from 12 record(s)
execution routes:      dbt.target/run, duckdb.execute, psycopg2.execute, psycopg2.execute_values
production .sql files: 163
recorded executing:    163
not recorded:          0
```

Four declared execution routes, four used: no undeclared route and none stale.

**40/40 mutation cases CAUGHT** by `scripts/verify_testing_contract_mutations.py`,
including *"a test goes back to typing its statement inline"*, *"a statement
becomes a template without joining the G19 ledger"*, *"a fixture declares a
column its dbt model does not"*, *"a new engine arrives as an unclassified
import"*, *"a job runs pytest and uploads no execution record"*, *"an exemption
quietly shrinks the coverage denominator"* and *"the corpus glob stops matching
and every coverage number reads 0 of 0"*.

**Two things the green run proves that green alone would not.** The gate could
only reach 163 of 163 if the new Layer 2 suite executed both `scripts/sql/`
files, since nothing else in the repository touches them — so the suite ran
rather than being deselected. And `REQUIRE_DECLARED_SKIPS=1` fails a run on an
undeclared skip, so neither `importorskip("duckdb")` nor `importorskip("pyarrow")`
fired: the suite ran rather than skipping quietly.
