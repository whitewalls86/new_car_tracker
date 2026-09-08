# Plan 162 Stage F — the mechanical sweeps

**Legacy:** Stage 5 · **Issue:** CAR-49 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage F. It carries the summary; the sections below are the detail.

---

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
been moved into the heaviest interpreter in CI. Stage C unblinded that
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
run against each step's real arguments and marker expression. Stage P already
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
and **G14 is 56 of 76, not 54** — the census undercounted, and Stage L's scope
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
Stage H will ask it again: the 7 tests lost their coverage measurement by moving
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

| Instrument | Denominator | Standing after Stage F |
|---|---|---:|
| `CI_INVOCATION_WAIVERS` | every integration suite | **0** — closed by Stage B |
| `ROUTE_WAIVERS` | every route in each app's schema | 12 unreached |
| `LAYER_2_WAIVERS` | every production `.sql` file | 56 unexecuted |

Each has a real denominator derived from the repository, cannot be satisfied by
touching a line, and is *complete* when its tuple is empty. That is the
integration coverage number, and Stages H and L are what move it. Line coverage
stays what it is good at — unit tests, where the question genuinely is whether
the line was exercised — measured on the one job that runs them.

Which inverts the reading of the `scrape_listings` incident above. The 0.18
points were not an instrument gap to be engineered away. **The instrument was
working**: it complained because a unit test had been moved out of the job that
measures unit tests, and it was right to.
