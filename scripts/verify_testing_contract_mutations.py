"""Plan 161 / CAR-34: prove ``tests/test_testing_contract.py`` can fail.

A contract test that nobody has watched fail is a contract test nobody knows
anything about. This applies one mutation per rule, runs the single assertion
that should notice, and restores the tree — so the claim "the test fails when
the contract and the repository disagree" is demonstrated rather than asserted.

Exits non-zero if any mutation goes unnoticed, or if the tree does not come
back green. It writes to the working tree and restores from an in-memory
snapshot: it must never ``git checkout --`` anything, because the files it
mutates are exactly the ones a change in progress is editing.

**This file has two halves, and only one of them runs in CI.**

*The anchors run there.* Every ``_edit`` anchor below must still match its file
exactly once, and every ``_delete`` path must still be on disk — a string search
per entry, no mutation applied and no subprocess started, so it costs a fraction
of a second. ``test_every_mutation_anchor_still_matches_its_file`` in
``tests/test_testing_contract.py`` is that check, and it is there because an
anchor is a literal in somebody else's file: it stops matching silently, and the
only thing that ever noticed was a human choosing to run this script. Plan 162
Stage Y broke five waivers keyed on ``admin.py:135:_fetch_dbt_context`` by adding
lines above them; the waivers failed loudly, and the anchors beside them would
not have. That rule found two already-ambiguous anchors the day it was written.

*The mutations do not.* Running them costs a pytest subprocess per entry plus a
mutate-and-restore of the working tree — a minute or two of wall clock, against
a workflow Plan 162 Stage R exists to shrink. That is a **cost decision, not a
verdict on their value**: the mutations are what actually prove a rule can fail,
and everything below is written so that running them stays cheap enough to do by
hand whenever a rule is added or reworded, which is the moment a rule most often
stops checking anything.

    python scripts/verify_testing_contract_mutations.py

What CI does assert about them is the *obligation*:
``test_every_asserted_rule_is_proved_by_a_mutation`` fails if a rule named in
``docs/TESTING.md``'s ``Asserted by`` column has no entry here. So a rule cannot
land unproven and an anchor cannot go stale in silence, while the proving itself
stays a deliberate run.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST = "tests/test_testing_contract.py"

#: Every module holding contract rules this harness proves can fail. A rule in
#: its own module is still a contract rule, and the baseline has to run it --
#: a mutation measured against a suite that never collected its assertion
#: reports CAUGHT for the wrong reason. ``tests/test_env_example_wiring.py`` is
#: Plan 162 Stage V's and joined on 2026-09-08.
#:
#: The third entry is a **node, not a module**, and that is the whole of why it
#: can be here. ``tests/integration/sql/test_fixture_statements.py`` is Layer 2
#: and its other test takes a ``cur`` fixture, so naming the module would put a
#: live Postgres between this harness and its own baseline. The one test in it
#: that reads the corpus rather than planning against it takes no fixture and
#: runs anywhere, so the baseline names exactly that test. Joined 2026-09-09 by
#: Stage AF.
TESTS = (
    TEST,
    "tests/test_env_example_wiring.py",
    "tests/integration/sql/test_fixture_statements.py"
    "::test_there_is_something_to_check",
)

#: The nodes whose assertion cannot be reached without a live, Flyway-migrated
#: Postgres. **Per node and not per module**, because the module holding this
#: one also holds a test that needs no engine at all and is in ``TESTS`` above:
#: "which layer this lives at" is not a property of the file.
#:
#: One entry, and it is a Layer 2 rule on purpose. ``PREPARE`` plans a statement
#: against the live catalogue, so a fixture left behind by a renamed column is a
#: condition only a real engine can express -- the same argument
#: ``test_a_database_triggered_code_is_asserted_against_a_real_engine`` makes
#: about a mocked ``rowcount``. Its mutation is therefore run against a
#: throwaway database this script provisions and destroys; see :func:`_engine`.
ENGINE_BOUND = (
    "tests/integration/sql/test_fixture_statements.py"
    "::test_every_test_statement_plans_against_the_migrated_schema",
)

_PG_IMAGE = "postgres:16"
_FLYWAY_IMAGE = "flyway/flyway:10-alpine"
_PG_CONTAINER = "cartracker-mutation-harness-postgres"
#: Not 5432. A developer running this almost certainly has the real stack up on
#: the default port, and a throwaway database that quietly shadowed it would be
#: the worst possible failure of a script whose whole job is not to disturb the
#: tree it runs in.
_PG_PORT = 55432
_PG_DSN = f"postgresql://cartracker:cartracker@localhost:{_PG_PORT}/cartracker"
_VIEWER_DSN = f"postgresql://viewer:ci_viewer@localhost:{_PG_PORT}/cartracker"

#: The same placeholders `ci.yml`'s Flyway step passes, because the migrations
#: do not apply without them and a second set of values here would be a second
#: schema to keep in step with the first.
_FLYWAY_PLACEHOLDERS = (
    "-placeholders.viewerPassword=ci_viewer",
    "-placeholders.scraperPassword=ci_scraper",
    "-placeholders.dbtPassword=ci_dbt",
    "-placeholders.authEmailSalt=ci_salt",
    "-placeholders.adminEmail=ci@example.com",
    "-placeholders.airflowPassword=ci_airflow",
    "-placeholders.airflowAppPassword=ci_airflow_app",
    "-placeholders.metricsPassword=ci_metrics",
)


def _docker(*args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout,
    )


def _provision_postgres() -> str | None:
    """Start a throwaway Postgres, migrate it, and hand back its DSN.

    Measured at roughly eight seconds on a machine that already holds both
    images: three to a ready server and four to apply the migrations. That is
    what makes running the engine-bound mutation here reasonable rather than a
    thing to declare an exception about -- the cost of *provisioning* an engine
    turned out to be smaller than the cost of explaining why we had not.

    Returns ``None`` on any failure, and every failure is one this script must
    survive: no Docker daemon, no images, a port already taken. The caller then
    reports the entry as unproven rather than judging it, because a mutation
    measured against a suite that could not run is the CAUGHT-for-the-wrong-
    reason this file warns about everywhere else.
    """
    try:
        _docker("rm", "-f", _PG_CONTAINER, timeout=60)
        started = _docker(
            "run", "-d", "--name", _PG_CONTAINER,
            "-e", "POSTGRES_USER=cartracker",
            "-e", "POSTGRES_PASSWORD=cartracker",
            "-e", "POSTGRES_DB=cartracker",
            "-p", f"{_PG_PORT}:5432", _PG_IMAGE,
        )
        if started.returncode != 0:
            print(f"  no engine: {started.stderr.strip().splitlines()[-1:]}")
            return None

        for _ in range(60):
            if _docker(
                "exec", _PG_CONTAINER, "pg_isready", "-U", "cartracker",
                timeout=30,
            ).returncode == 0:
                break
            time.sleep(1)
        else:
            print("  no engine: postgres never became ready")
            return None

        migrated = _docker(
            "run", "--rm", "--network", f"container:{_PG_CONTAINER}",
            "-v", f"{REPO_ROOT / 'db' / 'migrations'}:/flyway/sql",
            _FLYWAY_IMAGE,
            # `--network container:` puts Flyway in the database's own network
            # namespace, so it reaches it on localhost with no user-defined
            # network to create and tear down.
            "-url=jdbc:postgresql://localhost:5432/cartracker",
            "-user=cartracker", "-password=cartracker",
            "-locations=filesystem:/flyway/sql", "-detectEncoding=true",
            *_FLYWAY_PLACEHOLDERS, "-defaultSchema=public", "migrate",
            timeout=300,
        )
        if migrated.returncode != 0:
            print(f"  no engine: flyway migrate failed\n{migrated.stdout[-600:]}")
            return None
        return _PG_DSN
    except (OSError, subprocess.SubprocessError) as error:
        print(f"  no engine: {type(error).__name__}: {error}")
        return None


@contextmanager
def _engine() -> Iterator[str | None]:
    """A DSN the engine-bound nodes can plan against, or ``None``.

    ``TEST_DATABASE_URL`` wins when it is set, because a developer who has
    pointed the integration suite somewhere has said where they want it to run
    and this script has no business starting a second database beside it.
    Otherwise one is provisioned and destroyed.

    **The DSN alone is not the check.** Whichever database is used, the caller
    runs the engine-bound node *unmutated* first and only trusts it if that
    passes: an empty Postgres would fail every ``PREPARE`` and make the
    mutation look caught when nothing had been proved at all.
    """
    supplied = os.environ.get("TEST_DATABASE_URL")
    if supplied:
        print(f"engine: TEST_DATABASE_URL ({supplied.rsplit('@', 1)[-1]})")
        yield supplied
        return
    print(f"engine: provisioning a throwaway {_PG_IMAGE} on port {_PG_PORT}")
    dsn = _provision_postgres()
    try:
        yield dsn
    finally:
        if dsn is not None:
            _docker("rm", "-f", _PG_CONTAINER, timeout=60)
            print("engine: throwaway database destroyed")


def _pytest(node: str | None = None, dsn: str | None = None) -> tuple[int, str]:
    # A node containing `::` names its own module; a bare name belongs to
    # `TEST`, which is where most of them still live. That is what keeps the
    # entries written before a second module existed working unchanged.
    if node is None:
        targets = list(TESTS)
    else:
        targets = [node if "::" in node else f"{TEST}::{node}"]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *targets, "-q", "--no-header",
         "-p", "no:cacheprovider"],
        cwd=REPO_ROOT, capture_output=True, text=True,
        # ``PYTHONIOENCODING`` because the parent decodes as UTF-8 and the child
        # would otherwise encode its stdout with the console codepage: cp1252 on
        # Windows, where the em-dash pytest echoes out of
        # ``test_the_encoding_rule_sees_the_shape_ruff_cannot``'s own source is
        # byte 0x97 and the read raises ``UnicodeDecodeError`` before any
        # mutation can be judged. Found on 2026-09-09 by Stage AF, writing that
        # rule's mutation on the Windows half of this repository's two boxes --
        # the same locale-dependent defect the rule itself exists to catch,
        # arriving in the harness that proves it.
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT),
             "PYTHONIOENCODING": "utf-8",
             # Only when an engine was obtained -- passing a DSN
             # unconditionally would point the integration conftest at a port
             # with nothing behind it. The viewer DSN is set only for the
             # database this script provisioned, whose viewer password it
             # knows; a supplied TEST_DATABASE_URL brings its own.
             **({"TEST_DATABASE_URL": dsn} if dsn else {}),
             **({"TEST_VIEWER_DATABASE_URL": _VIEWER_DSN}
                if dsn == _PG_DSN else {})},
        encoding="utf-8",
        errors="replace",
    )
    return result.returncode, result.stdout


def _edit(relative: str, old: str, new: str) -> None:
    path = REPO_ROOT / relative
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(
            f"anchor not found in {relative}: {old!r}. The mutation has gone "
            f"stale, which means it has stopped testing anything."
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def _write(relative: str, text: str) -> None:
    path = REPO_ROOT / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _delete(relative: str) -> None:
    """Remove a file the snapshot list will restore afterwards.

    The rule this serves is about the corpus *shrinking*, which no edit can
    express -- the file has to actually leave the tree. Restoration works
    because the harness snapshots content and writes it back, recreating the
    file; a deleted path must therefore always appear in its mutation's
    snapshot list.
    """
    path = REPO_ROOT / relative
    if not path.is_file():
        raise AssertionError(
            f"cannot delete {relative}: it is not on disk. The mutation has "
            f"gone stale, which means it has stopped testing anything."
        )
    path.unlink()


# (assertion that should notice, what changed, how to change it, files to
# snapshot before changing, files that only exist during the mutation)
_TEST_SQL = REPO_ROOT / "tests" / "sql" / "integration" / "sql" / "test_ops_views"


def _statement(name: str) -> str:
    """A real statement, read from the file that owns it.

    **This harness may not type SQL either**, and the rule it exists to verify
    is what said so: ``scripts/`` is production Python, so a mutation payload
    written as a SQL literal here fails
    ``test_no_production_module_holds_a_sql_statement``. Reading the statement
    out of the file it already lives in is both the fix and the better
    mutation -- what gets inlined is the *real* text, not a plausible-looking
    stand-in for it.
    """
    return (_TEST_SQL / f"{name}.sql").read_text(encoding="utf-8")


MUTATIONS = [
    (
        "test_no_route_declares_a_422_no_request_can_trigger",
        "a route's only parameter loosens, and its declared 422 becomes a phantom",
        lambda: _edit(
            "ops/routers/users.py",
            "def revoke_user(request: Request, user_id: int):",
            "def revoke_user(request: Request, user_id: str):",
        ),
        ["ops/routers/users.py"],
        [],
    ),
    (
        "test_every_route_declares_the_statuses_it_can_return",
        "a route stops declaring a code it still answers with",
        lambda: _edit(
            "ops/routers/users.py",
            '        404: {"description": '
            '"No user with that id; nobody was revoked."},\n',
            "",
        ),
        ["ops/routers/users.py"],
        [],
    ),
    (
        "test_every_status_code_a_route_can_produce_is_asserted",
        "a route starts answering with a code no test asserts",
        lambda: _edit(
            "ops/routers/admin.py",
            '        "message": message,\n    }, status_code=404)',
            '        "message": message,\n    }, status_code=418)',
        ),
        ["ops/routers/admin.py"],
        [],
    ),
    (
        "test_the_route_code_corpus_is_not_empty",
        "the path matcher stops matching, and the coverage rule goes quiet",
        lambda: _edit(
            "tests/test_testing_contract.py",
            "    return all(a.startswith(\"{\") or a == b "
            "for a, b in zip(decorator, tail))",
            "    return False",
        ),
        ["tests/test_testing_contract.py"],
        [],
    ),
    (
        "test_no_caller_discards_an_outcome_it_asked_for",
        "the deploy API stops reading the five-valued result it asked for",
        lambda: _edit(
            "ops/routers/deploy.py",
            '    result = _set_intent("Deploy Declared", pause_long_jobs, targets)',
            '    _set_intent("Deploy Declared", pause_long_jobs, targets)',
        ),
        ["ops/routers/deploy.py"],
        [],
    ),
    (
        "test_every_response_we_ask_for_has_its_status_read",
        "the release gate stops reading the status of a service it polls",
        lambda: _edit(
            "ops/coordination_release.py",
            "            # The status, before the body. Plan 162 Stage Y, G27.\n"
            "            response.raise_for_status()\n"
            "            payload = response.json()\n"
            '            if payload.get("known") is not True',
            "            payload = response.json()\n"
            '            if payload.get("known") is not True',
        ),
        ["ops/coordination_release.py"],
        [],
    ),
    (
        "test_no_route_swallows_a_failed_write_and_reports_success",
        "a route's failed write is logged and then answered as a success",
        lambda: _edit(
            "ops/routers/users.py",
            'logger.exception("Failed to revoke user")\n'
            "        return _db_error_response(request=request)",
            'logger.exception("Failed to revoke user")',
        ),
        ["ops/routers/users.py"],
        [],
    ),
    (
        "test_every_mutation_observes_whether_it_changed_anything",
        "a route stops reading the rowcount of the UPDATE it performs",
        lambda: _edit(
            "ops/routers/admin.py",
            'error_context="Toggle-Search") as cur:\n'
            "            cur.execute(sql, params)\n"
            "            matched = cur.rowcount",
            'error_context="Toggle-Search") as cur:\n'
            "            cur.execute(sql, params)",
        ),
        ["ops/routers/admin.py"],
        [],
    ),
    (
        "test_every_integration_suite_is_invoked_by_a_ci_step",
        "a CI step stops invoking tests/integration/archiver/",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "run: pytest tests/integration/archiver/ -v -m integration",
            "run: echo skipping the archiver suite",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_patching_is_mocker_everywhere",
        "a file that used mocker starts importing unittest.mock.patch",
        lambda: _edit(
            "tests/shared/test_db.py",
            "import pytest",
            "from unittest.mock import patch\n\nimport pytest",
        ),
        ["tests/shared/test_db.py"],
        [],
    ),
    (
        "test_every_route_is_reached_through_the_apps_routing_table",
        "a new route is added to ops with no test requesting it",
        lambda: _edit(
            "ops/app.py",
            '@app.get("/health")',
            '@app.get("/widgets")\ndef list_widgets():\n    return []\n\n\n'
            '@app.get("/health")',
        ),
        ["ops/app.py"],
        [],
    ),
    (
        "test_every_service_directory_has_a_row_in_the_enough_table",
        "a new service package appears with no row in the enough table",
        lambda: _write("notifier/__init__.py", ""),
        [],
        ["notifier/__init__.py"],
    ),
    (
        "test_every_production_sql_file_is_touched_by_a_layer_2_test",
        "a new production .sql file appears that no Layer 2 test names",
        lambda: _write("ops/sql/select_orphaned_widgets.sql", "SELECT 1;\n"),
        [],
        ["ops/sql/select_orphaned_widgets.sql"],
    ),
    (
        "test_every_test_directory_is_assigned_a_layer",
        "a new test directory appears that the contract places nowhere",
        lambda: _write(
            "tests/notifier/test_smoke.py",
            "def test_smoke():\n    assert True\n",
        ),
        [],
        ["tests/notifier/test_smoke.py"],
    ),
    (
        "test_every_layer_number_in_the_code_matches_the_contract",
        "a module claims a layer its directory does not have",
        lambda: _write(
            "tests/shared/test_widget_layer.py",
            '"""Layer 4 — widgets."""\n\n\ndef test_widget():\n    assert True\n',
        ),
        [],
        ["tests/shared/test_widget_layer.py"],
    ),
    (
        "test_every_pytest_invocation_in_ci_sets_pythonpath",
        "a pytest step loses its PYTHONPATH",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "        run: pytest tests/integration/sql/ -v -m integration\n"
            "        env:\n"
            "          PYTHONPATH: ${{ github.workspace }}\n",
            "        run: pytest tests/integration/sql/ -v -m integration\n"
            "        env:\n",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_no_layer_2_test_executes_a_statement_without_asserting_on_the_result",
        "a Layer 2 test executes a statement and discards the result",
        lambda: _write(
            "tests/integration/sql/test_widget_queries.py",
            '"""Layer 2 — widgets."""\n\n\n'
            "def test_widget_query_runs(cur):\n"
            "    cur.execute(WIDGET_QUERY)\n"
            "    cur.fetchall()\n",
        ),
        [],
        ["tests/integration/sql/test_widget_queries.py"],
    ),
    (
        "test_every_waiver_names_a_gap_entry_that_exists",
        "a gap entry a waiver depends on is renamed in the contract",
        # Re-anchored from G6 to G5 by Plan 162 Stage M. G6 was deleted by
        # Stage 6 when route coverage closed, and this anchor went with it --
        # so the harness aborted here and every mutation after it stopped
        # running, unnoticed, for two stages. That is the same failure Stage 1
        # hit on G1 and G2, and the staleness guard is what said so both times.
        # G5 is one of the three gaps a live waiver still names.
        lambda: _edit(
            "docs/TESTING.md",
            "| G5 | **Inline SQL at a SQL-taking call site.",
            "| G5x | **Inline SQL at a SQL-taking call site.",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_integration_suite_is_invoked_by_a_ci_step",
        "a waiver survives the repair it was waiting for",
        lambda: _edit(
            TEST,
            "CI_INVOCATION_WAIVERS = ()",
            'CI_INVOCATION_WAIVERS = (\n'
            '    Waiver("tests/integration/ops", gap="G6", owner=162),\n'
            ')',
        ),
        [TEST],
        [],
    ),
    (
        "test_no_dormant_suite_is_quietly_running",
        "a suite declared dormant acquires a CI step and keeps the declaration",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "run: pytest tests/integration/archiver/ -v -m integration",
            "run: pytest tests/integration/lakehouse/ -v -m integration",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_no_gap_entry_outlives_the_plan_that_owns_it",
        "a gap entry's owner plan is archived and the entry stays behind",
        # Re-anchored twice by Plan 162 Stage M. Stage 7 struck G14's text
        # through when it closed the gap, so the old anchor stopped matching;
        # and the row it renamed G14 to, G15, is a real entry Stage 7 added, so
        # the replacement would now write a duplicate. G99 is used precisely
        # because no gap will ever have that letter.
        lambda: _edit(
            "docs/TESTING.md",
            "| G14 | ~~**56 of 76 production `.sql` files",
            "| G14 | **PLACEHOLDER** | -- | Plan 84 |\n"
            "| G99 | ~~**56 of 76 production `.sql` files",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_no_waiver_outlives_the_plan_that_owns_it",
        "a waiver's owner plan is archived and the waiver stays behind",
        # Re-anchored from G6 to G5 by Plan 162 Stage M, for the reason above:
        # ROUTE_WAIVERS is `()` since Stage 6, so no waiver names G6 any more.
        # Anchored on the constructor *and* the comprehension line beneath it.
        # `test_no_waiver_outlives_the_plan_that_owns_it` quotes the constructor
        # in its own docstring as literal source text, so the shorter anchor had
        # come to match twice and `_edit` was choosing the code site by position
        # rather than by intent. Found by Stage AF's anchor rule.
        lambda: _edit(
            TEST,
            'Waiver(subject, gap="G5", owner=162)\n    for subject in (',
            'Waiver(subject, gap="G5", owner=84)\n    for subject in (',
        ),
        [TEST],
        [],
    ),
    (
        "test_every_service_directory_is_measured_by_coverage",
        "a new service package appears that coverage is not pointed at",
        lambda: _write("notifier/__init__.py", ""),
        [],
        ["notifier/__init__.py"],
    ),
    (
        "test_every_service_directory_is_measured_by_coverage",
        "a coverage source is renamed and nothing measures it any more",
        lambda: _edit("pyproject.toml", '    "dashboard",', '    "dashboards",'),
        ["pyproject.toml"],
        [],
    ),
    (
        "test_the_coverage_number_the_unit_job_produces_is_consumed",
        "the unit job goes back to measuring coverage and discarding it",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "\n          --cov-fail-under=75",
            "",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_the_coverage_number_the_unit_job_produces_is_consumed",
        "a coverage threshold is left behind with nothing measuring coverage",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "          --cov --cov-report=term-missing --cov-report=xml",
            "          --cov-report=term-missing --cov-report=xml",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_asserted_rule_names_a_real_test",
        "the rules table names a check that does not exist",
        lambda: _edit(
            "docs/TESTING.md",
            "`test_every_pytest_invocation_in_ci_sets_pythonpath`",
            "`test_every_pytest_invocation_in_ci_sets_a_pythonpath`",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_asserted_rule_names_a_real_test",
        "a rule is listed as checked with no test named against it",
        lambda: _edit(
            "docs/TESTING.md",
            "| `test_every_service_directory_has_a_row_in_the_enough_table` |",
            "| the enough table is compared to disk |",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_script_directory_is_classified",
        "a new script directory appears that the contract classifies nowhere",
        lambda: _write("scripts/adhoc/probe.py", "print('probe')\n"),
        [],
        ["scripts/adhoc/probe.py"],
    ),
    (
        "test_every_script_directory_is_classified",
        "a script bucket is renamed and the contract keeps describing the old one",
        lambda: _edit("docs/TESTING.md", "| `scripts/oneoff/` |", "| `scripts/spent/` |"),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_unmeasured_script_bucket_is_omitted_from_coverage",
        "a bucket the contract calls unmeasured re-enters the denominator",
        lambda: _edit("pyproject.toml", 'omit = ["scripts/oneoff/*"]', "omit = []"),
        ["pyproject.toml"],
        [],
    ),
    (
        "test_every_unmeasured_script_bucket_is_omitted_from_coverage",
        "coverage stops measuring a bucket the contract calls measured",
        lambda: _edit(
            "pyproject.toml",
            'omit = ["scripts/oneoff/*"]',
            'omit = ["scripts/oneoff/*", "scripts/*"]',
        ),
        ["pyproject.toml"],
        [],
    ),
    # Plan 162 Stage U. The runtime half of this rule is a hook, and a hook
    # that stopped noticing would leave every job green -- so the four checks
    # standing behind it are the ones that most need to have been watched fail.
    (
        "test_every_declared_skip_names_a_test_that_exists",
        "a declared skip goes on naming a test that has been renamed away",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "::test_every_sha_a_recap_names_is_a_real_commit",
            "::test_every_sha_a_recap_names_is_a_real_commit_renamed",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "test_no_declared_skip_sits_at_a_layer_that_admits_none",
        "a Layer 2 skip is declared instead of the fixture being fixed",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "DECLARED_SKIPS = (",
            "DECLARED_SKIPS = (\n"
            "    DeclaredSkip(\n"
            '        "tests/integration/sql/test_dashboard_queries.py::test_widgets",\n'
            '        reason="the widget table is not seeded in CI",\n'
            '        condition="no widgets",\n'
            "        since=date(2026, 9, 4),\n"
            "    ),",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "test_every_pytest_step_runs_under_the_declared_skip_gate",
        "the workflow-level gate is removed, as one unguarded line once was",
        lambda: _edit(
            ".github/workflows/ci.yml",
            'env:\n  REQUIRE_DECLARED_SKIPS: "1"\n',
            "",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_pytest_step_runs_under_the_declared_skip_gate",
        "the plugin registration is dropped, leaving the gate set and unread",
        lambda: _edit(
            "pyproject.toml",
            'addopts = "-p tests.plugins.declared_skips'
            ' -p tests.plugins.sql_execution_recorder"\n',
            "",
        ),
        ["pyproject.toml"],
        [],
    ),
    (
        "test_the_declared_skip_registry_only_ratchets_down",
        "a third skip is declared without the ceiling moving with it",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "DECLARED_SKIPS = (",
            "DECLARED_SKIPS = (\n"
            "    DeclaredSkip(\n"
            '        "tests/scripts/test_audit_git_refs.py::test_divergence",\n'
            '        reason="git behaves differently on the runner",\n'
            '        condition="no git",\n'
            "        since=date(2026, 9, 4),\n"
            "    ),",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    # ----------------------------------------------------------------------
    # Plan 162 Stage X. Seven rules arrived at once, and the stage's own
    # argument is that a denominator fitted to what exists when it is written
    # will be wrong -- so each is mutated in the direction it exists to catch.
    # ----------------------------------------------------------------------
    (
        "test_no_test_module_holds_a_sql_statement",
        "a test goes back to typing its statement inline",
        lambda: _edit(
            "tests/integration/sql/test_ops_views.py",
            'SQL("insert_ops_blocked_cooldown"),',
            repr(_statement("insert_ops_blocked_cooldown")) + ",",
        ),
        ["tests/integration/sql/test_ops_views.py"],
        [],
    ),
    (
        "test_every_test_sql_file_is_named_by_the_module_it_mirrors",
        "a test statement is left behind with nothing loading it",
        lambda: _write(
            "tests/sql/integration/sql/test_ops_views/select_orphaned.sql",
            _statement("insert_ops_blocked_cooldown"),
        ),
        [],
        ["tests/sql/integration/sql/test_ops_views/select_orphaned.sql"],
    ),
    (
        "test_every_test_statement_that_holds_a_template_is_waived",
        "a statement becomes a template without joining the G19 ledger",
        lambda: _edit(
            "tests/sql/integration/sql/test_ops_views/insert_ops_blocked_cooldown.sql",
            "ops.blocked_cooldown",
            "{schema}.blocked_cooldown",
        ),
        ["tests/sql/integration/sql/test_ops_views/insert_ops_blocked_cooldown.sql"],
        [],
    ),
    (
        "test_no_test_invents_the_shape_of_a_relation_production_defines",
        "a fixture declares a column its dbt model does not",
        # Anchored through the seed statement on the next line: the CREATE has
        # grown a second, identical occurrence in `test_negative_durations_
        # included_when_configured`, and only the seed distinguishes them.
        # Found by Stage AF's anchor rule.
        lambda: _edit(
            "tests/scripts/test_audit_adaptive_refresh_features.py",
            'CREATE TABLE int_listing_state_runs (run_duration_hours INTEGER)")\n'
            '        con.execute(SQL("duckdb/insert_int_listing_state_runs"))',
            'CREATE TABLE int_listing_state_runs (run_hours INTEGER)")\n'
            '        con.execute(SQL("duckdb/insert_int_listing_state_runs"))',
        ),
        ["tests/scripts/test_audit_adaptive_refresh_features.py"],
        [],
    ),
    (
        "test_every_dbt_model_declares_an_enforced_contract",
        "a model drops its enforced contract and nothing waives it",
        lambda: _edit(
            "dbt/models/staging/stg_dealers.schema.yml",
            '    config:\n      tags: ["hourly_core"]\n'
            "      contract:\n        enforced: true",
            '    config:\n      tags: ["hourly_core"]',
        ),
        ["dbt/models/staging/stg_dealers.schema.yml"],
        [],
    ),
    (
        "test_every_production_import_is_classified",
        "a new engine arrives as an unclassified import",
        lambda: _edit(
            "shared/db.py",
            "import psycopg2",
            "import psycopg2\nimport sqlalchemy",
        ),
        ["shared/db.py"],
        [],
    ),
    (
        "test_the_recorder_instruments_every_client_production_reaches",
        "the recorder stops wrapping a client the contract says reaches an engine",
        lambda: _edit(
            "tests/plugins/sql_execution_recorder.py",
            '"psycopg2", "duckdb", "asyncpg", "pyspark"',
            '"psycopg2", "duckdb", "asyncpg"',
        ),
        ["tests/plugins/sql_execution_recorder.py"],
        [],
    ),
    (
        "test_every_sql_corpus_exemption_is_declared",
        "an exemption quietly shrinks the coverage denominator",
        lambda: _edit(
            "tests/test_testing_contract.py",
            '_SQL_EXEMPT_ROOTS = ("db/migrations/", "dbt/", "tests/")',
            '_SQL_EXEMPT_ROOTS = ("db/migrations/", "dbt/", "tests/", "dashboard/")',
        ),
        ["tests/test_testing_contract.py"],
        [],
    ),
    (
        "test_the_production_sql_corpus_is_not_empty",
        "the corpus glob stops matching and every coverage number reads 0 of 0",
        lambda: _edit(
            "tests/test_testing_contract.py",
            "            for path in REPO_ROOT.rglob(\"*.sql\")",
            "            for path in REPO_ROOT.rglob(\"*.sqlx\")",
        ),
        ["tests/test_testing_contract.py"],
        [],
    ),
    (
        "test_every_job_that_runs_pytest_has_its_record_read_by_the_gate",
        "a job runs pytest and uploads no execution record",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "          name: sql-execution-unit-tests\n",
            "          name: coverage-unit-tests\n",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_job_that_runs_pytest_has_its_record_read_by_the_gate",
        "the coverage gate goes back to only reporting",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "run: python scripts/check_sql_execution_coverage.py",
            "run: python scripts/check_sql_execution_coverage.py --report",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    # ----------------------------------------------------------------------
    # Plan 162 Stage S. Four rules guard the carved-out CI gates and the
    # source lists they read; each is mutated in the direction it exists to
    # catch.
    # ----------------------------------------------------------------------
    (
        "test_every_ignored_path_is_invoked_by_another_step_in_the_same_job",
        "the dedicated step that runs an --ignored gate is deleted",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "pytest tests/integration/dbt/test_branch_coverage.py",
            "echo skipping the branch coverage gate",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_the_sql_corpus_shrinks_only_by_naming_the_model_that_absorbed_it",
        "a production .sql file leaves the corpus with nothing naming its absorber",
        lambda: _delete("scripts/sql/select_enabled_search_keys.sql"),
        ["scripts/sql/select_enabled_search_keys.sql"],
        [],
    ),
    (
        "test_the_non_empty_gate_reconciles_with_the_dbt_source_list",
        "a table joins the Postgres snapshot allowlist that no dbt source reads",
        lambda: _edit(
            "shared/lake_snapshot_postgres.py",
            '    ("ops", "tracked_models"),',
            '    ("ops", "tracked_models"),\n    ("ops", "widget_queue"),',
        ),
        ["shared/lake_snapshot_postgres.py"],
        [],
    ),
    (
        "test_the_snapshot_writer_and_the_source_auditor_include_the_same_tables",
        "a table leaves the snapshot writer and stays in the audit specs",
        lambda: _edit(
            "archiver/processors/lake_snapshot_export_cache.py",
            '    "blocked_cooldown_events",\n',
            "",
        ),
        ["archiver/processors/lake_snapshot_export_cache.py"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_every_documented_key_reaches_a_service",
        "a key is documented in .env.example that no Compose service delivers",
        lambda: _edit(
            ".env.example",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog\nHARNESS_UNWIRED_KEY=change_me",
        ),
        [".env.example"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_every_documented_key_reaches_a_service",
        "the same key, named only in a docker-compose.yml comment",
        lambda: (
            _edit(
                ".env.example",
                "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog",
                "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog\nHARNESS_UNWIRED_KEY=change_me",
            ),
            _edit(
                "docker-compose.yml",
                "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
                "      # HARNESS_UNWIRED_KEY is delivered here\n"
                "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
            ),
        ),
        [".env.example", "docker-compose.yml"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_no_undelivered_key_is_quietly_wired",
        "a key declared undelivered is wired into Compose after all",
        lambda: _edit(
            "docker-compose.yml",
            "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
            "      SCRAPER_RESULTS_BASE_URL: ${SCRAPER_RESULTS_BASE_URL:-}\n"
            "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py"
        "::test_every_undelivered_declaration_names_a_consumer_that_reads_it",
        "an Undelivered entry names a real file that does not read its key",
        lambda: _edit(
            "tests/test_env_example_wiring.py",
            'consumer="scraper/processors/scrape_results.py"',
            'consumer="scraper/app.py"',
        ),
        ["tests/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_every_interpolated_variable_is_documented",
        "a Compose service interpolates a variable .env.example never documents",
        lambda: _edit(
            "docker-compose.yml",
            "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
            "      HARNESS_NEW_SECRET: ${HARNESS_NEW_SECRET}\n"
            "      RESEND_API_KEY: ${RESEND_API_KEY:-}",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_every_interpolated_variable_is_documented",
        "the $$ escape strip is removed, so $${HOSTNAME} reads as interpolated",
        lambda: _edit(
            "tests/test_env_example_wiring.py",
            '_REFERENCE.findall(_ESCAPED.sub("", node))',
            "_REFERENCE.findall(node)",
        ),
        ["tests/test_env_example_wiring.py"],
        [],
    ),
    (
        "test_the_fixture_relation_corpus_is_not_empty",
        "the shadowing _CREATE_TABLE comes back and empties the corpus",
        lambda: _edit(
            "tests/test_testing_contract.py",
            "_CREATE_TABLE_BODY = re.compile(",
            "_CREATE_TABLE = re.compile(",
        ),
        ["tests/test_testing_contract.py"],
        [],
    ),
    # -----------------------------------------------------------------------
    # Plan 162 Stage AF. The twenty rules the `Asserted by` column named and
    # nobody had watched fail. Each description says what defect the rule is
    # meant to catch, because that sentence is the artifact -- see the stage's
    # note in `tests/test_testing_contract.py` on why no generator writes it.
    # -----------------------------------------------------------------------
    (
        "tests/integration/sql/test_fixture_statements.py"
        "::test_every_test_statement_plans_against_the_migrated_schema",
        "a fixture statement is left behind by a column the schema renamed",
        # The only entry here that needs an engine, and the reason `ENGINE_BOUND`
        # exists. `PREPARE` resolves `artifact_kind` against the live catalogue
        # and refuses the statement; nothing static can see this, which is the
        # whole argument for the rule sitting at Layer 2.
        lambda: _edit(
            "tests/sql/integration/sql/test_ops_views/insert_ops_artifacts_queue.sql",
            "(minio_path, artifact_type, fetched_at, status)",
            "(minio_path, artifact_kind, fetched_at, status)",
        ),
        ["tests/sql/integration/sql/test_ops_views/insert_ops_artifacts_queue.sql"],
        [],
    ),
    (
        "test_no_route_is_hidden_from_the_schema_this_rule_reads",
        "a route opts out of the schema the routing-table rule enumerates from",
        lambda: _edit(
            "ops/routers/public.py",
            # Re-anchored by Plan 162 Stage Z, which split this route's single
            # `api_route(methods=["GET", "HEAD"])` into a `get`/`head` pair so
            # the two halves stop sharing one operation ID. The old anchor
            # reached for the 404 description because both page routes shared
            # the decorator's first two lines; the pair's own decorator line is
            # distinguishing on its own, and `get` rather than `head` so the
            # mutation hides the route a reader would actually request.
            '@router.get("/recaps/{slug}", response_class=FileResponse, '
            "responses=_NO_SUCH_RECAP)",
            '@router.get("/recaps/{slug}", response_class=FileResponse, '
            "responses=_NO_SUCH_RECAP, include_in_schema=False)",
        ),
        ["ops/routers/public.py"],
        [],
    ),
    (
        "test_the_assertionless_rule_sees_a_test_that_only_executes",
        "a delegated assertion stops counting, so an empty result set means nothing",
        lambda: _edit(
            TEST,
            'if name.lstrip("_").startswith("assert"):',
            'if name.startswith("assert"):',
        ),
        [TEST],
        [],
    ),
    (
        "test_the_encoding_rule_sees_the_shape_ruff_cannot",
        "the rotating log handler drops out of the shapes the encoding rule reads",
        lambda: _edit(
            TEST,
            '    "FileHandler", "RotatingFileHandler", "TimedRotatingFileHandler",',
            '    "FileHandler", "TimedRotatingFileHandler",',
        ),
        [TEST],
        [],
    ),
    (
        "test_every_text_read_and_write_states_its_encoding",
        "a text read stops naming its encoding and asks the locale instead",
        lambda: _edit(
            "shared/query_loader.py",
            'path.read_text(encoding="utf-8")',
            "path.read_text()",
        ),
        ["shared/query_loader.py"],
        [],
    ),
    (
        "test_the_mutation_corpus_is_not_empty",
        "the UPDATE/DELETE pattern stops matching, retiring the observation rule",
        lambda: _edit(
            TEST,
            r'_MUTATING_VERB = re.compile(r"\b(?:UPDATE|DELETE\s+FROM)\b", re.I)',
            r'_MUTATING_VERB = re.compile(r"\b(?:UPSERT|TRUNCATE)\b", re.I)',
        ),
        [TEST],
        [],
    ),
    (
        "test_the_outcome_type_corpus_is_not_empty",
        "the fields that make a class an outcome type narrow to almost none",
        lambda: _edit(
            TEST,
            '_OUTCOME_FIELDS = frozenset({"status", "ok", "error", "detail", '
            '"failure_reason"})',
            '_OUTCOME_FIELDS = frozenset({"failure_reason"})',
        ),
        [TEST],
        [],
    ),
    (
        "test_the_permanent_phantom_ledger_still_describes_a_phantom",
        "the recap slug gains a type, so its declared 422 stops being a phantom",
        lambda: _edit(
            "ops/routers/public.py",
            "def recap_page(slug: str) -> FileResponse:",
            "def recap_page(slug: int) -> FileResponse:",
        ),
        ["ops/routers/public.py"],
        [],
    ),
    (
        "test_a_database_triggered_code_is_asserted_against_a_real_engine",
        "the only real-engine test of a rowcount-triggered 404 stops asserting it",
        lambda: _edit(
            "tests/integration/ops/test_user_management.py",
            '        "/admin/users/99999/revoke", follow_redirects=False\n'
            "    )\n    assert response.status_code == 404",
            '        "/admin/users/99999/revoke", follow_redirects=False\n'
            "    )\n    assert response.status_code == 303",
        ),
        ["tests/integration/ops/test_user_management.py"],
        [],
    ),
    (
        "test_the_database_triggered_corpus_is_not_empty",
        "rowcount stops being recognised where it is bound, emptying the corpus",
        lambda: _edit(
            TEST,
            # Two rules bind rowcount this way; the indentation and the guard
            # under it are what pick out `_database_triggered_codes`.
            '        bound = _names_bound_from(function, "rowcount")\n'
            "        if not bound:",
            '        bound = _names_bound_from(function, "rowcounts")\n'
            "        if not bound:",
        ),
        [TEST],
        [],
    ),
    (
        "test_the_check_constraint_corpus_is_not_empty",
        "the CHECK ... IN reader stops matching, disarming both vocabulary rules",
        lambda: _edit(
            TEST,
            r'    r"CHECK\s*\(\s*(\w+)\s+IN\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL',
            r'    r"CONSTRAIN\s*\(\s*(\w+)\s+IN\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL',
        ),
        [TEST],
        [],
    ),
    (
        "test_every_check_constrained_column_has_one_declared_vocabulary",
        "a value is renamed in the declared vocabulary and not in the migration",
        lambda: _edit(
            "shared/db_vocabularies.py",
            '    RETRY = "retry"\n    SKIP = "skip"',
            '    RETRY = "retry"\n    SKIP = "skipped"',
        ),
        ["shared/db_vocabularies.py"],
        [],
    ),
    (
        "test_no_module_retypes_a_database_vocabulary_it_could_import",
        "a call site retypes a role db/migrations/ owns instead of importing it",
        lambda: _edit(
            "ops/app.py",
            "if role == UserRole.OBSERVER and",
            'if role == "observer" and',
        ),
        ["ops/app.py"],
        [],
    ),
    (
        "test_every_dag_status_check_accepts_only_statuses_its_service_emits",
        "the DAG accepts the status Stage W found it accepting and the exporter "
        "never returns",
        lambda: _edit(
            "airflow/dags/export_ci_lake_snapshot.py",
            'acceptable = {"audited"}',
            'acceptable = {"audited", "created"}',
        ),
        ["airflow/dags/export_ci_lake_snapshot.py"],
        [],
    ),
    (
        "test_the_dag_status_rule_has_something_to_check",
        "the checker's subject is renamed and the DAG population empties",
        lambda: _edit(
            TEST,
            'if "status" not in _subject_names(node.left):',
            'if "state" not in _subject_names(node.left):',
        ),
        [TEST],
        [],
    ),
    (
        "tests/test_env_example_wiring.py"
        "::test_no_undocumented_declaration_is_quietly_documented",
        "a variable declared absent from .env.example is documented there anyway",
        lambda: _edit(
            ".env.example",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog\n"
            "HTML_COMPRESSION_DICT_ID=",
        ),
        [".env.example"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py"
        "::test_every_undocumented_declaration_names_a_file_that_interpolates_it",
        "an Undocumented entry outlives the Compose file it was written for",
        lambda: _edit(
            "tests/test_env_example_wiring.py",
            'compose_file="docker-compose.mlflow.yml"',
            'compose_file="docker-compose.lakehouse.yml"',
        ),
        ["tests/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py"
        "::test_neither_ledger_grows_without_the_ceiling_moving",
        "a fifth undocumented variable is declared and the ceiling stays at four",
        lambda: _edit(
            "tests/test_env_example_wiring.py",
            # Anchored on the tuple's last entry and its close, so the append
            # lands *inside* UNDOCUMENTED. Anchoring on the comment below it
            # put the new entry after the closing paren, and the module then
            # failed to import -- which the harness reported as CAUGHT, for
            # exactly the wrong reason.
            "        since=date(2026, 9, 8),\n    ),\n)\n\n#: Ceilings, not counts",
            "        since=date(2026, 9, 8),\n"
            "    ),\n"
            "    Undocumented(\n"
            '        "HARNESS_QUIET_APPEND",\n'
            '        compose_file="docker-compose.yml",\n'
            '        reason="appended without moving the ceiling",\n'
            "        since=date(2026, 9, 9),\n"
            "    ),\n"
            ")\n\n#: Ceilings, not counts",
        ),
        ["tests/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/test_env_example_wiring.py::test_both_corpora_are_not_empty",
        "the docker-compose glob stops matching and every rule there goes quiet",
        lambda: _edit(
            "tests/test_env_example_wiring.py",
            'return sorted(_REPO_ROOT.glob("docker-compose*.yml"))',
            'return sorted(_REPO_ROOT.glob("docker-compose*.yaml"))',
        ),
        ["tests/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/integration/sql/test_fixture_statements.py"
        "::test_there_is_something_to_check",
        "the tests/sql tree moves and the statement corpus empties under the rule",
        lambda: _edit(
            TEST,
            # Three readers walk this tree; the engine filter under it is what
            # picks out `postgres_test_statements`.
            '        for path in sorted(SQL_ROOT.rglob("*.sql"))\n'
            "        if not _ENGINE_DIRECTORIES",
            '        for path in sorted(SQL_ROOT.rglob("*.psql"))\n'
            "        if not _ENGINE_DIRECTORIES",
        ),
        [TEST],
        [],
    ),
    # The three rules Stage AF added, proved the same way as the ones they
    # guard. The anchor rule's own mutation is the stage's exit criterion.
    (
        "test_every_mutation_anchor_still_matches_its_file",
        "a documented key is repeated, and every anchor keyed on it goes ambiguous",
        # The *twice* case rather than the *missing* case, deliberately: `_edit`
        # already raises on an anchor it cannot find, so a mutation that deletes
        # one would be proving the harness's own guard. An anchor that matches
        # twice is the half nothing could see -- `_edit` silently takes the
        # first match and the entry stops describing what it changes.
        lambda: _edit(
            ".env.example",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog",
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog\n"
            "LAKEKEEPER_CATALOG_URI=http://lakekeeper:8181/catalog",
        ),
        [".env.example"],
        [],
    ),
    (
        "test_every_asserted_rule_is_proved_by_a_mutation",
        "a mutation's node is renamed and the rule it proved goes unproven",
        # Anchored on the node *and* its description. A mutation that edits
        # this file has to name the text it is looking for, which puts a second
        # copy of that text in this file -- so a one-line anchor here matches
        # itself. Two lines joined by an escaped newline do not appear in the
        # argument, only in the entry being mutated.
        lambda: _edit(
            "scripts/verify_testing_contract_mutations.py",
            '        "test_the_dag_status_rule_has_something_to_check",\n'
            "        \"the checker's subject is renamed and the DAG population "
            'empties",',
            '        "test_the_dag_status_rule_has_something_to_checked",\n'
            "        \"the checker's subject is renamed and the DAG population "
            'empties",',
        ),
        ["scripts/verify_testing_contract_mutations.py"],
        [],
    ),
    (
        "test_the_mutation_harness_corpus_is_not_empty",
        "the payload helpers narrow, and the harness reads as anchoring nothing",
        lambda: _edit(
            TEST,
            '_CHECKABLE_PAYLOADS = ("_edit", "_delete")',
            '_CHECKABLE_PAYLOADS = ("_delete",)',
        ),
        [TEST],
        [],
    ),
    # Plan 162 Stage Z. The gate is `generate_service_contracts.py --check`;
    # these four prove the rules that stop the gate being quietly removed --
    # its artifacts, its CI step, and the pins that make its output mean
    # anything. A gate deleted from the workflow fails nothing on its own.
    (
        "test_every_service_has_a_committed_contract",
        "a service's committed contract leaves the tree and nothing says it is gone",
        lambda: _delete("contracts/processing.json"),
        ["contracts/processing.json"],
        [],
    ),
    (
        "test_the_service_contract_gate_runs_in_ci",
        "CI stops diffing the contracts, so six accurate files go quietly stale",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "        run: python scripts/generate_service_contracts.py --check",
            "        run: echo the contract diff no longer runs",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_version_that_decides_the_schema_is_pinned_exactly",
        "a version that decides the schema loosens from an exact pin to a floor",
        lambda: _edit(
            "constraints.txt",
            "fastapi==0.141.1",
            "fastapi>=0.141.1",
        ),
        ["constraints.txt"],
        [],
    ),
    (
        "test_every_ci_install_runs_under_the_pinned_stack",
        "the inherited constraint is dropped and every job resolves its own versions",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "  PIP_CONSTRAINT: ${{ github.workspace }}/constraints.txt",
            "  PIP_CONSTRAINT_UNSET: ${{ github.workspace }}/constraints.txt",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
]


def main() -> int:
    with _engine() as dsn:
        return _run(dsn)


def _run(dsn: str | None) -> int:
    code, output = _pytest()
    print("baseline:", output.strip().splitlines()[-1])
    if code != 0:
        print("the suite must be green before any mutation means anything")
        return 1

    # The engine-bound nodes, unmutated, before anything is mutated. A
    # `PREPARE` suite against an unmigrated database fails every statement, and
    # a mutation measured against that reports CAUGHT having proved nothing.
    # So the engine earns its entries by passing first, or it does not get them.
    engine_ready = dsn is not None
    if engine_ready:
        for node in ENGINE_BOUND:
            code, output = _pytest(node, dsn)
            if code != 0:
                engine_ready = False
                print(f"engine: {node.split('::')[-1]} does not pass unmutated "
                      f"-- {output.strip().splitlines()[-1]}")
                break
    if engine_ready:
        print("engine: ready, and the engine-bound baseline passes")

    missed, unproven = [], []
    for node, description, mutate, snapshot, created in MUTATIONS:
        if node in ENGINE_BOUND and not engine_ready:
            unproven.append(description)
            print(f"{'UNPROVEN HERE':14} {description}")
            continue
        saved = {rel: (REPO_ROOT / rel).read_text(encoding="utf-8") for rel in snapshot}
        try:
            mutate()
            code, _ = _pytest(node, dsn)
        finally:
            for rel, text in saved.items():
                (REPO_ROOT / rel).write_text(text, encoding="utf-8")
            for rel in created:
                path = REPO_ROOT / rel
                path.unlink(missing_ok=True)
                if path.parent != REPO_ROOT and path.parent.exists():
                    if not any(path.parent.iterdir()):
                        path.parent.rmdir()
        caught = code != 0
        missed += [] if caught else [description]
        print(f"{'CAUGHT' if caught else '*** MISSED ***':14} {description}")

    code, output = _pytest()
    print("\nrestored:", output.strip().splitlines()[-1])
    if missed:
        print("\nunnoticed mutations:\n  " + "\n  ".join(missed))
    if unproven:
        # Reported, never fatal. No engine is a fact about the machine, not a
        # finding about the repository, and failing here would make the honest
        # answer indistinguishable from a rule that had actually gone quiet.
        print("\nnot proven in this run, for want of an engine:\n  "
              + "\n  ".join(unproven)
              + "\n\nStart Docker, or point TEST_DATABASE_URL at a "
                "Flyway-migrated Postgres, and run again.")
    return 1 if missed or code != 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
