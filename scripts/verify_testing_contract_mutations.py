"""Plan 161 / CAR-34: prove ``tests/rules/test_testing_contract.py`` can fail.

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
``tests/rules/test_testing_contract.py`` is that check, and it is there because an
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
TEST = "tests/rules/test_testing_contract.py"

#: Every rule this harness proves can fail, as the baseline has to run it --
#: a mutation measured against a suite that never collected its assertion
#: reports CAUGHT for the wrong reason.
#:
#: **One directory, and nothing else.** Stage AF kept a list of modules here
#: plus one node picked out of a Layer 2 suite -- the corpus floor, which takes
#: no fixture and so could be run without a database while its sibling could
#: not. Plan 162 Stage AG moved that floor into ``tests/rules/`` where it
#: belongs and the list collapsed to the directory: enumerating the rule set
#: here was a second copy of it, and this plan is named for what happens to
#: those. Everything this harness mutates now runs from one path.
TESTS = ("tests/rules/",)

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


def _drop_rewritten_bytecode(relative: str) -> None:
    """Delete pytest's rewritten bytecode for one source file.

    **Without this the harness reports the wrong verdict, silently.** pytest
    rewrites assertions and caches the result in ``__pycache__`` under a key of
    *(mtime, size)*. A mutation that changes a file's content without changing
    its length -- ``rglob("test_*.py")`` to ``rglob("rule_*.py")`` is exactly
    that -- and is restored in the same second leaves that key identical, so the
    *next* child run imports the **mutated** bytecode from a tree that is back to
    normal. Found 2026-09-09 by Plan 162 Stage AG: the entry after that one
    reported MISSED, and the final restore check reported two failures, against
    a working tree that was byte-for-byte correct.

    It is the same class as the false CAUGHT this file already records -- a
    verdict about something other than what the description claims -- and it
    cannot be left to the conventions, because a same-length mutation is a
    perfectly reasonable thing to write.
    """
    cache = (REPO_ROOT / relative).parent / "__pycache__"
    if not cache.is_dir():
        return
    stem = Path(relative).stem
    for compiled in cache.glob(f"{stem}.*.pyc"):
        compiled.unlink(missing_ok=True)


def _edit(relative: str, old: str, new: str) -> None:
    path = REPO_ROOT / relative
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(
            f"anchor not found in {relative}: {old!r}. The mutation has gone "
            f"stale, which means it has stopped testing anything."
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    _drop_rewritten_bytecode(relative)


def _write(relative: str, text: str) -> None:
    path = REPO_ROOT / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    _drop_rewritten_bytecode(relative)


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
    _drop_rewritten_bytecode(relative)


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


def _production_statement(relative: str) -> str:
    """A production statement, read from the ``.sql`` file that owns it.

    The general form of :func:`_statement`, and it is here for the same reason
    stated one function up -- except that this time the rule caught the harness
    rather than the harness anticipating the rule. Plan 162 Stage AG registered
    ``test_no_production_module_holds_a_sql_statement``, wrote two mutations
    with their payloads typed out as SQL literals, and the very next baseline
    run failed on this file: ``scripts/`` is production Python, so a statement
    typed here is inline SQL like any other. Reading the text out of the file
    that owns it is both the fix and the better mutation -- what gets filed
    twice is the *real* statement rather than a plausible-looking stand-in.
    """
    text = (REPO_ROOT / relative).read_text(encoding="utf-8")
    return "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("--")
    ).strip()


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
            "tests/rules/test_testing_contract.py",
            "    return all(a.startswith(\"{\") or a == b "
            "for a, b in zip(decorator, tail))",
            "    return False",
        ),
        ["tests/rules/test_testing_contract.py"],
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
        # Anchored on the *body* of an existing route rather than on a
        # decorator. Plan 162 Stage AA gave `/health`'s decorator a
        # `response_model=`, which broke the previous anchor, and every route
        # here is now liable to grow one -- so anchoring on any decorator text
        # is anchoring on the thing most likely to move. Appending after a
        # return statement is also safe in a way that prepending to a `def` is
        # not: inserting before `def health():` would land between that
        # function and its decorator and silently rebind it.
        lambda: _edit(
            "ops/app.py",
            '    return RedirectResponse(url="/admin/searches/")',
            '    return RedirectResponse(url="/admin/searches/")\n\n\n'
            '@app.get("/widgets")\ndef list_widgets():\n    return []',
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
        # Anchored on this one registration rather than on the whole `addopts`
        # line, which Plan 162 Stage AA broke by appending a third plugin to it.
        # Deleting the registration alone is also the more faithful mutation:
        # it leaves the other plugins loaded and the file valid, so what goes
        # red is the gate being unread rather than pytest failing to start.
        lambda: _edit(
            "pyproject.toml",
            "-p tests.plugins.declared_skips ",
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
            "tests/rules/test_testing_contract.py",
            '_SQL_EXEMPT_ROOTS = ("db/migrations/", "dbt/", "tests/")',
            '_SQL_EXEMPT_ROOTS = ("db/migrations/", "dbt/", "tests/", "dashboard/")',
        ),
        ["tests/rules/test_testing_contract.py"],
        [],
    ),
    (
        "test_the_production_sql_corpus_is_not_empty",
        "the corpus glob stops matching and every coverage number reads 0 of 0",
        lambda: _edit(
            "tests/rules/test_testing_contract.py",
            "            for path in REPO_ROOT.rglob(\"*.sql\")",
            "            for path in REPO_ROOT.rglob(\"*.sqlx\")",
        ),
        ["tests/rules/test_testing_contract.py"],
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
        "tests/rules/test_env_example_wiring.py::test_every_documented_key_reaches_a_service",
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
        "tests/rules/test_env_example_wiring.py::test_every_documented_key_reaches_a_service",
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
        "tests/rules/test_env_example_wiring.py::test_no_undelivered_key_is_quietly_wired",
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
        "tests/rules/test_env_example_wiring.py"
        "::test_every_undelivered_declaration_names_a_consumer_that_reads_it",
        "an Undelivered entry names a real file that does not read its key",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            'consumer="scraper/processors/scrape_results.py"',
            'consumer="scraper/app.py"',
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/rules/test_env_example_wiring.py::test_every_interpolated_variable_is_documented",
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
        "tests/rules/test_env_example_wiring.py::test_every_interpolated_variable_is_documented",
        "the $$ escape strip is removed, so $${HOSTNAME} reads as interpolated",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            '_REFERENCE.findall(_ESCAPED.sub("", node))',
            "_REFERENCE.findall(node)",
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "test_the_fixture_relation_corpus_is_not_empty",
        "the shadowing _CREATE_TABLE comes back and empties the corpus",
        lambda: _edit(
            "tests/rules/test_testing_contract.py",
            "_CREATE_TABLE_BODY = re.compile(",
            "_CREATE_TABLE = re.compile(",
        ),
        ["tests/rules/test_testing_contract.py"],
        [],
    ),
    # -----------------------------------------------------------------------
    # Plan 162 Stage AF. The twenty rules the `Asserted by` column named and
    # nobody had watched fail. Each description says what defect the rule is
    # meant to catch, because that sentence is the artifact -- see the stage's
    # note in `tests/rules/test_testing_contract.py` on why no generator writes it.
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
        "tests/rules/test_env_example_wiring.py"
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
        "tests/rules/test_env_example_wiring.py"
        "::test_every_undocumented_declaration_names_a_file_that_interpolates_it",
        "an Undocumented entry outlives the Compose file it was written for",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            'compose_file="docker-compose.mlflow.yml"',
            'compose_file="docker-compose.lakehouse.yml"',
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/rules/test_env_example_wiring.py"
        "::test_neither_ledger_grows_without_the_ceiling_moving",
        "a fifth undocumented variable is declared and the ceiling stays at four",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            # Anchored on the tuple's last entry and its close, so the append
            # lands *inside* UNDOCUMENTED. Anchoring on the comment below it
            # put the new entry after the closing paren, and the module then
            # failed to import -- which the harness reported as CAUGHT, for
            # exactly the wrong reason.
            "        since=date(2026, 9, 8),\n    ),\n)\n\n"
            "#: The declared size of each ledger",
            "        since=date(2026, 9, 8),\n"
            "    ),\n"
            "    Undocumented(\n"
            '        "HARNESS_QUIET_APPEND",\n'
            '        compose_file="docker-compose.yml",\n'
            '        reason="appended without moving the ceiling",\n'
            "        since=date(2026, 9, 9),\n"
            "    ),\n"
            ")\n\n#: The declared size of each ledger",
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/rules/test_env_example_wiring.py::test_both_corpora_are_not_empty",
        "the docker-compose glob stops matching and every rule there goes quiet",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            'return sorted(_REPO_ROOT.glob("docker-compose*.yml"))',
            'return sorted(_REPO_ROOT.glob("docker-compose*.yaml"))',
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    # Plan 162 Stage AK. The mutation above collapses the corpus to nothing,
    # which the `>= 2` that used to stand there caught. This one *erodes* it --
    # nine Compose files become eight -- and `>= 2` passed that, which is the
    # whole argument for the stage. It is the exit clause "a reader narrowed so
    # it resolves less than the whole corpus", and it is asserted in the two
    # files that read the corpus, because they now check each other.
    (
        "tests/rules/test_env_example_wiring.py::test_both_corpora_are_not_empty",
        "the compose glob loses one file of nine and the rules go quiet over it",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            'return sorted(_REPO_ROOT.glob("docker-compose*.yml"))',
            "return [\n"
            '        path for path in sorted(_REPO_ROOT.glob("docker-compose*.yml"))\n'
            '        if path.name != "docker-compose.mlflow.yml"\n'
            "    ]",
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheDerivationHoldsItsShape"
        "::test_every_compose_file_is_attributed_to_a_project",
        "the keep-set's compose glob loses one file of nine and prunes its images",
        lambda: _edit(
            "tests/rules/test_image_keep_set.py",
            'return sorted(_REPO_ROOT.glob("docker-compose*.yml"))',
            "return [\n"
            '        path for path in sorted(_REPO_ROOT.glob("docker-compose*.yml"))\n'
            '        if path.name != "docker-compose.mlflow.yml"\n'
            "    ]",
        ),
        ["tests/rules/test_image_keep_set.py"],
        [],
    ),
    # The other half of Stage AK's exit: a floor loosened back to a bound. The
    # rule that forbids the bound has to be the thing that notices, because
    # `test_both_corpora_are_not_empty` passes perfectly well with `>= 2` in it
    # -- that is what it did until this stage.
    (
        "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"
        "::test_no_rule_guards_itself_with_a_guessed_number",
        "an exact floor is loosened back to a number somebody chose",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            "assert {path.name for path in _compose_files()} == set(COMPOSE_PROJECTS), (",
            "assert len(_compose_files()) >= 2, (",
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"
        "::test_no_rule_guards_itself_with_a_guessed_number",
        "a guessed bound is moved behind a name, where the reader used to lose it",
        lambda: _edit(
            "tests/rules/test_env_example_wiring.py",
            "assert {path.name for path in _compose_files()} == set(COMPOSE_PROJECTS), (",
            "_COMPOSE_FLOOR = 2\n"
            "    assert len(_compose_files()) >= _COMPOSE_FLOOR, (",
        ),
        ["tests/rules/test_env_example_wiring.py"],
        [],
    ),
    (
        "test_there_is_something_to_check",
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
    # ---- Plan 162 Stage Q: CI's services are production's ----------------
    #
    # The first two shell out to `docker compose config`, so on a machine with
    # no Docker CLI their rules skip and this harness reports them as failing
    # to fail. That is a false negative to know about rather than a defect:
    # the rules are required in CI precisely because `ubuntu-latest` has the
    # CLI, and an undeclared skip there fails the run.
    (
        "tests/rules/test_ci_compose_parity.py::test_the_ci_override_is_the_whole_difference",
        "CI's services gain a difference from production's that nobody declared",
        lambda: _edit(
            "docker-compose.ci.yml",
            '      MINIO_BROWSER_REDIRECT_URL: ""',
            '      MINIO_BROWSER_REDIRECT_URL: ""\n'
            '      MINIO_PROMETHEUS_AUTH_TYPE: ""',
        ),
        ["docker-compose.ci.yml"],
        [],
    ),
    (
        "tests/rules/test_ci_compose_parity.py::test_ci_runs_productions_flyway_command",
        "production's Flyway stops baselining and CI stops noticing",
        lambda: _edit(
            "docker-compose.yml",
            "      -baselineOnMigrate=true\n",
            "",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_ci_compose_parity.py::test_no_heavy_job_declares_its_own_services",
        "a job goes back to hand-declaring the database it tests against",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "  service-integration:\n"
            "    name: Service integration tests (Postgres)\n",
            "  service-integration:\n"
            "    name: Service integration tests (Postgres)\n"
            "    services:\n"
            "      postgres:\n"
            "        image: postgres:16\n",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "tests/rules/test_ci_compose_parity.py::test_every_heavy_job_starts_the_compose_services",
        "a job stops starting its services and only the Flyway step still names them",
        # Anchored on the one `up` line that does not start MinIO, so it
        # matches exactly once across the five jobs.
        lambda: _edit(
            ".github/workflows/ci.yml",
            "--env-file .env.ci up -d --wait postgres\n",
            "--env-file .env.ci version\n",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestGapReferences::test_every_gap_a_stage_claims_exists",
        "a gap entry is renamed and the stage claiming it points at nothing",
        # G29 is Stage AF's, and its header claims it. Renaming the row is a
        # truer mutation than deleting it: the gap list is allowed to lose a
        # row when the gap is repaired, and what must not survive that is a
        # stage still claiming the number.
        lambda: _edit(
            "docs/TESTING.md",
            "| G29 | ",
            "| G299 | ",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestGapReferences::test_the_gap_claim_corpus_is_not_empty",
        "the gap-claim pattern stops matching and the rule reads an empty set",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'_GAP_CLAIM = re.compile(r"\*\*Gap:\*\*\s*((?:G\d+(?:,\s*)?)+)")',
            r'_GAP_CLAIM = re.compile(r"\*\*Gaps:\*\*\s*((?:G\d+(?:,\s*)?)+)")',
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    # Plan 162 Stage AK. The entry above breaks the *label* half of the claim
    # pattern, which empties the set and `>= 10` caught. These two break the
    # halves a number could not see: the value half, where the field is still
    # found and no longer read, and the gap table growing a column, where every
    # row is still there and none of them parses.
    (
        "tests/rules/test_planning_docs.py"
        "::TestGapReferences::test_the_gap_claim_corpus_is_not_empty",
        "the gap-claim pattern still finds the field and stops reading its value",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'_GAP_CLAIM = re.compile(r"\*\*Gap:\*\*\s*((?:G\d+(?:,\s*)?)+)")',
            r'_GAP_CLAIM = re.compile(r"\*\*Gap:\*\*\s*((?:H\d+(?:,\s*)?)+)")',
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestGapReferences::test_the_gap_claim_corpus_is_not_empty",
        "the gap list gains a leading column and every definition stops parsing",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'_GAP_ENTRY = re.compile(r"^\| (G\d+) \|", re.MULTILINE)',
            r'_GAP_ENTRY = re.compile(r"^\| \| (G\d+) \|", re.MULTILINE)',
        ),
        ["tests/rules/test_planning_docs.py"],
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
    # -----------------------------------------------------------------------
    # Plan 162 Stage AG. The rules that moved into `tests/rules/` and had no
    # row, and therefore owed no mutation -- which is G30 exactly: the
    # obligation reads the `Asserted by` column, so an unregistered rule was
    # asked for nothing. Registering them is what made these owed.
    # -----------------------------------------------------------------------
    # `tests/rules/test_readme_contract.py`. The subject is `README.md`, which
    # is published on merge with no deploy in between, so every mutation here
    # is an edit to the file itself rather than to the rule that reads it.
    (
        "tests/rules/test_readme_contract.py::test_every_local_readme_link_resolves",
        "a README link stops resolving, which is a 404 on the front door",
        lambda: _edit(
            "README.md",
            "](docs/TESTING.md)",
            "](docs/TESTING_STRATEGY.md)",
        ),
        ["README.md"],
        [],
    ),
    (
        "tests/rules/test_readme_contract.py"
        "::test_the_readme_states_both_halves_of_the_production_split",
        "a heading is reworded, and the two section rules would pass vacuously",
        lambda: _edit(
            "README.md",
            "**Proven but not production-serving.**",
            "**Proven, but not yet serving production.**",
        ),
        ["README.md"],
        [],
    ),
    (
        "tests/rules/test_readme_contract.py"
        "::test_no_experimental_component_is_listed_as_production",
        "a migration-track name appears in the production list",
        lambda: _edit(
            "README.md",
            "- MinIO holds replayable bronze HTML and permanent Parquet history.",
            "- MinIO holds replayable bronze HTML and permanent Iceberg history.",
        ),
        ["README.md"],
        [],
    ),
    (
        "tests/rules/test_readme_contract.py"
        "::test_the_experimental_stack_is_still_disclaimed_by_name",
        "an experimental name is deleted rather than moved, which the rule above allows",
        lambda: _edit(
            "README.md",
            "- dbt-Spark parity work and MLflow experiment provenance.",
            "- dbt-Spark parity work and experiment provenance.",
        ),
        ["README.md"],
        [],
    ),
    (
        "tests/rules/test_readme_contract.py"
        "::test_duckdb_is_named_as_what_actually_serves",
        "the README stops naming what actually serves the dashboard",
        lambda: _edit(
            "README.md",
            "- dbt and DuckDB build and serve every analytical mart used by the public page,",
            "- dbt builds and serves every analytical mart used by the public page,",
        ),
        ["README.md"],
        [],
    ),
    # `tests/rules/test_declared_skips.py`. The subject is the hook in
    # `tests/plugins/declared_skips.py`, so each mutation takes out one of the
    # three directions it enforces. A hook that quietly stopped noticing is the
    # defect it exists to prevent, which is why these are worth having at all.
    (
        "tests/rules/test_declared_skips.py::test_an_undeclared_skip_fails_the_run",
        "the hook stops noticing a skip nobody declared",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "    undeclared = sorted("
            "nodeid for nodeid in observed if nodeid not in declared)",
            "    undeclared = []",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py::test_a_declared_skip_is_accepted_and_named",
        "the hook stops reporting on a green run, so a job where it never "
        "loaded looks identical to one where it was satisfied",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            '    terminalreporter.section("Declared skips", red=failed)',
            '    if failed:\n'
            '        terminalreporter.section("Declared skips", red=failed)',
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py"
        "::test_a_declared_skip_that_stops_skipping_fails_the_run",
        "a declaration whose reason has stopped being true goes unreported",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "    silent = sorted(\n"
            '        f"{nodeid} (declared {entry.since}: {entry.condition})"',
            "    silent = []\n"
            "    _unused = (\n"
            '        f"{nodeid} (declared {entry.since}: {entry.condition})"',
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py"
        "::test_a_declared_skip_a_run_never_selected_is_not_a_failure",
        "the drift direction stops reading the selected set, so every job "
        "fails for the declarations belonging to the other jobs",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "        if nodeid in _selected and nodeid not in observed",
            "        if nodeid not in observed",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py"
        "::test_a_declared_skip_firing_for_a_different_cause_fails_the_run",
        "a test skipping for a new cause inherits the old declaration",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "    mismatched = sorted(\n"
            '        f"{nodeid}\\n      declared condition: '
            '{declared[nodeid].condition!r}"',
            "    mismatched = []\n"
            "    _unused = (\n"
            '        f"{nodeid}\\n      declared condition: '
            '{declared[nodeid].condition!r}"',
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py::test_the_gate_is_off_by_default",
        "the gate defaults on, and every local run fails for the recap check "
        "that correctly does not skip on a full clone",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "    if not os.environ.get(GATE):\n        return",
            "    if os.environ.get(GATE) == \"off\":\n        return",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    (
        "tests/rules/test_declared_skips.py"
        "::test_an_unfamiliar_report_shape_is_read_rather_than_indexed",
        "an unfamiliar report shape is indexed instead of stringified, so it "
        "surfaces as an IndexError in a terminal summary rather than a "
        "mismatch someone can read",
        lambda: _edit(
            "tests/plugins/declared_skips.py",
            "    if isinstance(longrepr, tuple) and len(longrepr) == 3:",
            "    if longrepr is not None:",
        ),
        ["tests/plugins/declared_skips.py"],
        [],
    ),
    # `tests/rules/test_maintenance_running_set.py`. The subject is
    # `maintenance-running-set.txt`, and the derivation is negative -- anything
    # not named is expected running -- so most of these delete or reclassify an
    # entry rather than adding a wrong one.
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestRegistryShape::test_the_file_exists",
        "the manifest leaves the tree, and a restore can no longer tell a "
        "deliberately-stopped service from a forgotten one",
        lambda: _delete("maintenance-running-set.txt"),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestRegistryShape::test_every_entry_declares_a_known_class",
        "an entry declares a class no restore knows how to act on",
        lambda: _edit(
            "maintenance-running-set.txt",
            "snapshot-worker on-demand Profile-gated",
            "snapshot-worker on-demand-oneshot Profile-gated",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestRegistryShape::test_every_entry_carries_a_reason",
        "an entry loses the written reason somebody has to re-evaluate at 2am",
        # Stage AK narrowed this mutation, and the narrowing is the honest
        # record of what the stage cost here. It used to thin the reason to
        # "same as dbt." -- 12 characters, caught by `len(reason) > 40`. That
        # floor was a guess (the shortest real reason is 51, so 40 was picked to
        # sit under the corpus of the day) and it is gone, and with it the only
        # mechanical objection to a reason too thin to act on. Nothing exact
        # replaces it: no length is evidence that prose means something. So the
        # mutation now removes the reason outright, which is what the rule still
        # claims, rather than asserting a catch the rule no longer makes.
        lambda: _edit(
            "maintenance-running-set.txt",
            "dbt_test on-demand Profile-gated (`tools`) tools image, same as `dbt`.",
            "dbt_test on-demand",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestRegistryShape::test_every_entry_names_a_real_service",
        "a renamed service leaves an entry behind that then covers whatever "
        "takes its name next",
        lambda: _edit(
            "maintenance-running-set.txt",
            "april-processor on-demand Profile-gated",
            "april-processor-run on-demand Profile-gated",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestDefaultProjectIsFullyClassified::test_profile_gated_services_are_all_classified",
        "a profile-gated service is unclassified, so a plain `up -d` gets it "
        "wrong in one direction and nothing says which",
        lambda: _edit(
            "maintenance-running-set.txt",
            "dbt on-demand Profile-gated (`tools`) tools image, invoked as a one-shot\n"
            "    `docker compose run`. Never started by `up -d` and never restored.\n"
            "\n",
            "",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestDefaultProjectIsFullyClassified::test_services_without_a_restart_policy_are_classified",
        "a service Docker will not restart after a reboot is named nowhere, "
        "so the fleet comes back missing it and nobody is looking",
        lambda: _edit(
            "maintenance-running-set.txt",
            "flyway oneshot Runs the Flyway migrations to completion and exits. Every\n"
            "    consumer gates on `condition: service_completed_successfully`. A restore\n"
            '    that waits for it to be "running" waits forever.\n'
            "\n",
            "",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestDefaultProjectIsFullyClassified::test_long_running_services_are_not_silently_absent",
        "the solver is reclassified on-demand, so the expected-running set "
        "stops containing a service that must be restored",
        lambda: _edit(
            "maintenance-running-set.txt",
            "trawl profile-running Profile-gated",
            "trawl on-demand Profile-gated",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestKnownFindingsStayRecorded::test_caddy_restart_gap_is_recorded_while_it_exists",
        "caddy loses the restart policy Plan 142 gave it and no entry records "
        "the gap, so :80 and :443 stay down after a reboot with nothing "
        "reporting it",
        lambda: _edit(
            "docker-compose.yml",
            "    # leave the policy silently unapplied.\n    restart: unless-stopped",
            "    # leave the policy silently unapplied.\n    restart: \"no\"",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestKnownFindingsStayRecorded::test_the_four_soak_containers_are_all_aux_paused",
        "one of the four stale unhealthy containers the Plan 140 soak found "
        "is reclassified, re-arming that finding",
        lambda: _edit(
            "maintenance-running-set.txt",
            "cartracker-mlflow/mlflow aux-paused Standalone",
            "cartracker-mlflow/mlflow on-demand Standalone",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_maintenance_running_set.py"
        "::TestKnownFindingsStayRecorded::test_trawl_and_redis_trawl_are_restored_together",
        "the solver's Redis stops being restored with it, which is the "
        "2026-08-14 outage with an extra step",
        lambda: _edit(
            "maintenance-running-set.txt",
            "redis-trawl profile-running Backs",
            "redis-trawl on-demand Backs",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    # `tests/rules/test_image_keep_set.py`. Two subjects -- the Compose files
    # and the manifest the keep-set is derived from, and the runbook block that
    # carries the answer to the operator holding the prune command.
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheDerivationHoldsItsShape::test_every_compose_file_is_attributed_to_a_project",
        "a Compose file is attributed to no project, so its images are "
        "invisible to the keep-set and a prune takes them",
        lambda: _edit(
            "tests/rules/test_image_keep_set.py",
            '    "docker-compose.test.yml": "cartracker-test",\n',
            "",
        ),
        ["tests/rules/test_image_keep_set.py"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheDerivationHoldsItsShape::test_a_shared_image_is_joined_to_every_service_that_builds_it",
        "a service stops sharing the image two others build, and the join "
        "that protects it loses a holder",
        lambda: _edit(
            "docker-compose.yml",
            "    image: cartracker-archiver\n"
            "    container_name: cartracker-pack-worker",
            "    image: cartracker-pack-worker\n"
            "    container_name: cartracker-pack-worker",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheDerivationHoldsItsShape::test_one_running_service_protects_a_shared_image",
        "the holder join goes from any-to-all, which is the label-to-image "
        "hazard Plan 170 Stage A named: an image two running services need "
        "enters the keep-set because a third holder is on-demand",
        lambda: _edit(
            "tests/rules/test_image_keep_set.py",
            "        if not keys & held",
            "        if not keys <= held",
        ),
        ["tests/rules/test_image_keep_set.py"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheDerivationHoldsItsShape::test_interpolated_and_digest_references_resolve_to_what_the_host_shows",
        "an interpolated image reference stops resolving to its default, so "
        "the rendered block cannot be matched by eye against the host",
        lambda: _edit(
            "tests/rules/test_image_keep_set.py",
            "    interpolated = _INTERPOLATED.match(reference)\n"
            "    if interpolated:\n"
            "        reference = interpolated.group(\"default\")",
            "    interpolated = None",
        ),
        ["tests/rules/test_image_keep_set.py"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheKeepSetProtectsWhatTheManifestNames::test_every_entry_is_classified_by_the_manifest",
        "a kept image is held only by services no manifest entry names, so "
        "the block says it is kept and cannot say why",
        lambda: _edit(
            "maintenance-running-set.txt",
            "cartracker-mlflow/mlflow aux-paused Standalone Plan 112 Gate B tracking server",
            "cartracker-mlflow-server/mlflow aux-paused Standalone Plan 112 Gate B server",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheKeepSetProtectsWhatTheManifestNames::test_every_entry_carries_a_protected_class",
        "a kept image's only class is one that does not justify keeping it, "
        "so the keep-set is over-protecting and nothing says so",
        lambda: _edit(
            "maintenance-running-set.txt",
            "cartracker-lakehouse/lakekeeper-migrate aux-paused Same project and pause.",
            "cartracker-lakehouse/lakekeeper-migrate oneshot Same project and pause.",
        ),
        ["maintenance-running-set.txt"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheKeepSetProtectsWhatTheManifestNames::test_the_four_paused_and_on_demand_images_are_all_in_it",
        "a profile gate is dropped, and one of the four images `docker image "
        "prune -a` would take silently leaves the keep-set",
        # **Not a reclassification in the manifest.** The first draft of this
        # entry dropped `on-demand` from `PROTECTED_CLASSES` and was MISSED:
        # that tuple is read by `test_every_entry_carries_a_protected_class`
        # alone, and `keep_set()` asks only whether a container holds the image.
        # The mutation has to change the *derivation's* answer, and a `profiles:`
        # key is what decides it.
        lambda: _edit(
            "docker-compose.yml",
            '    profiles: [ "tools" ]',
            "    # the profile gate that kept this image out of `up -d`",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheKeepSetProtectsWhatTheManifestNames::test_the_third_party_catalog_is_carried_by_name",
        "the third-party catalog loses one of the two services that carry it, "
        "which is the only reason an unlabelled image is in the block at all",
        lambda: _edit(
            "docker-compose.lakehouse.yml",
            "    image: ${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}\n"
            "    container_name: cartracker-lakekeeper-migrate",
            "    image: quay.io/lakekeeper/catalog:v0.13.2\n"
            "    container_name: cartracker-lakekeeper-migrate",
        ),
        ["docker-compose.lakehouse.yml"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheKeepSetProtectsWhatTheManifestNames::test_the_solver_and_its_redis_are_both_kept",
        "the solver's Redis loses its profile gate, so it leaves the keep-set "
        "and a prune in the window before `--profile trawl` takes it",
        # Same correction as the entry above: the class in the manifest is not
        # what `keep_set()` reads, so reclassifying `redis-trawl` was MISSED.
        lambda: _edit(
            "docker-compose.yml",
            '    restart: unless-stopped\n    profiles: ["trawl"]\n'
            "    # Plan 124: bound Redis memory alongside the trawl solver it backs.",
            "    restart: unless-stopped\n"
            "    # Plan 124: bound Redis memory alongside the trawl solver it backs.",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheRunbookCarriesTheDerivation::test_the_rendered_block_matches_the_derivation",
        "the runbook block stops matching the derivation, so the operator "
        "holding the prune command is told the old answer",
        lambda: _edit(
            "docs/runbooks/runbook_storage_maintenance.md",
            "on-demand       cartracker-dbt:latest",
            "on-demand       cartracker-dbt:v2",
        ),
        ["docs/runbooks/runbook_storage_maintenance.md"],
        [],
    ),
    # The five rules that were already in `test_testing_contract.py` and had
    # never been registered, so the obligation never asked them for anything.
    (
        "test_no_sql_comment_contains_a_parameter_placeholder",
        "a comment written to explain a parameter adds one, and psycopg2 "
        "counts it -- so the caller passes too few and the route answers 503",
        lambda: _edit(
            "ops/sql/select_user_role.sql",
            "-- Resolve a caller's role from the hash of their email address.",
            "-- Resolve a caller's role from the hash %s of their email address.",
        ),
        ["ops/sql/select_user_role.sql"],
        [],
    ),
    (
        "test_no_two_production_sql_files_hold_the_same_statement",
        "a statement is filed twice, which is two things to edit and one to forget",
        # `_write` rather than `_edit`, because an `_edit` anchor into a `.sql`
        # file *is* a SQL literal in this module and fails the rule one entry
        # down. The anchor rule reads only the path and the anchor, so a
        # computed replacement is fine -- but here there is no anchor to read
        # that would not itself be a statement.
        lambda: _write(
            "ops/sql/select_live_cooldown_listings.sql",
            _production_statement("ops/sql/select_user_role.sql"),
        ),
        ["ops/sql/select_live_cooldown_listings.sql"],
        [],
    ),
    (
        "test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement",
        "the detector goes back to reading an f-string's head only, which is "
        "Rule 5b's hole restored: an f-string leads with a verb and nothing "
        "after it, so the clause grammar judges it not a statement",
        lambda: _edit(
            "tests/rules/test_testing_contract.py",
            "    if isinstance(node, ast.JoinedStr):\n"
            '        return "".join(\n'
            "            value.value\n"
            "            if isinstance(value, ast.Constant) "
            "and isinstance(value.value, str)\n"
            '            else " ? "\n'
            "            for value in node.values\n"
            "        )",
            "    if isinstance(node, ast.JoinedStr):\n"
            "        return next(\n"
            "            (value.value for value in node.values\n"
            "             if isinstance(value, ast.Constant)\n"
            "             and isinstance(value.value, str)),\n"
            "            None,\n"
            "        )",
        ),
        ["tests/rules/test_testing_contract.py"],
        [],
    ),
    (
        "test_no_production_module_holds_a_sql_statement",
        "a production module grows a SQL statement in Python, so it is in no "
        "`.sql` file and the Layer 2 census cannot count it",
        lambda: _edit(
            "shared/db_vocabularies.py",
            '    ("vin_to_listing_events", "event_type"): VinToListingEvent,\n}',
            '    ("vin_to_listing_events", "event_type"): VinToListingEvent,\n}\n\n'
            'ROLE_LOOKUP = """'
            + _production_statement("ops/sql/select_user_role.sql")
            + '"""',
        ),
        ["shared/db_vocabularies.py"],
        [],
    ),
    (
        "test_no_waiver_is_listed_twice",
        "a waiver is listed twice, making the ledger look longer than the debt "
        "it records -- and every ceiling in this repository is read off a "
        "ledger's length",
        lambda: _edit(
            "tests/rules/test_testing_contract.py",
            "    Waiver(\n"
            '        "ops/sql/cancel_coordination_state.sql == "\n'
            '        "ops/sql/release_deploy_coordination.sql",\n'
            '        gap="G17",\n'
            "        owner=162,\n"
            "    ),",
            "    Waiver(\n"
            '        "ops/sql/cancel_coordination_state.sql == "\n'
            '        "ops/sql/release_deploy_coordination.sql",\n'
            '        gap="G17",\n'
            "        owner=162,\n"
            "    ),\n"
            "    Waiver(\n"
            '        "ops/sql/cancel_coordination_state.sql == "\n'
            '        "ops/sql/release_deploy_coordination.sql",\n'
            '        gap="G17",\n'
            "        owner=162,\n"
            "    ),",
        ),
        ["tests/rules/test_testing_contract.py"],
        [],
    ),
    (
        "tests/rules/test_image_keep_set.py"
        "::TestTheRunbookCarriesTheDerivation::test_the_block_appears_exactly_once",
        "a second block is appended rather than the stale one replaced, which "
        "satisfies the match rule and leaves the runbook holding two answers",
        lambda: _edit(
            "docs/runbooks/runbook_storage_maintenance.md",
            "# Derived from docker-compose*.yml and maintenance-running-set.txt\n",
            "# Derived from docker-compose*.yml and maintenance-running-set.txt\n"
            "# Derived from docker-compose*.yml and maintenance-running-set.txt\n",
        ),
        ["docs/runbooks/runbook_storage_maintenance.md"],
        [],
    ),
    # The three rules Stage AG added, proved by the instrument they are part
    # of -- the same self-measurement Stage AF recorded when three of its own
    # twenty-two entries proved the rules it had just written.
    (
        "test_the_rules_directory_is_not_empty",
        "the rules directory's glob stops matching, which retires both "
        "membership directions in silence -- G30 one level up from itself",
        lambda: _edit(
            "tests/rules/test_testing_contract.py",
            'for path in sorted(RULES_DIR.rglob("test_*.py")):',
            'for path in sorted(RULES_DIR.rglob("rule_*.py")):',
        ),
        ["tests/rules/test_testing_contract.py"],
        [],
    ),
    (
        "test_every_test_in_the_rules_directory_is_named_in_the_contract",
        "a rule joins the directory with no row, so nothing obliges it to have "
        "been watched fail -- which is Stage Q's four rules and 3,899 passed",
        # The stage's own exit reads *demonstrated by an unregistered rule
        # failing, not asserted*, and this is that demonstration made durable:
        # the payload is a real new rule module, not an edit to an existing one.
        lambda: _write(
            "tests/rules/test_an_unregistered_rule.py",
            '"""Layer 0. A rule nobody registered, written to be caught."""\n'
            "\n\ndef test_a_rule_that_joined_no_row():\n"
            "    assert True\n",
        ),
        [],
        ["tests/rules/test_an_unregistered_rule.py"],
    ),
    (
        "test_every_asserted_rule_lives_in_the_rules_directory",
        "the engine-bound exemption empties, so the one rule that legitimately "
        "lives outside the directory is reported misplaced -- the fail-closed "
        "direction, which is what stops an unreadable ENGINE_BOUND exempting "
        "everything instead of nothing",
        # Anchored on the line *above* the tuple rather than on
        # ``ENGINE_BOUND = (``, which the replacement would reintroduce -- the
        # anchor rule caught that on the first run, matching three times
        # instead of once. Commenting the binding out empties the set the
        # exemption reads, because the parse finds no such assignment.
        lambda: _edit(
            "scripts/verify_testing_contract_mutations.py",
            "#: throwaway database this script provisions and destroys; "
            "see :func:`_engine`.\nENGINE_BOUND",
            "#: throwaway database this script provisions and destroys; "
            "see :func:`_engine`.\n_NO_LONGER_ENGINE_BOUND",
        ),
        ["scripts/verify_testing_contract_mutations.py"],
        [],
    ),
    (
        "test_every_asserted_rule_lives_in_the_rules_directory",
        "a rule is registered and written outside the directory, where the "
        "membership rule above cannot see it and the next Stage Q would land",
        lambda: _edit(
            "docs/TESTING.md",
            "| The rules table may not claim a check the suite does not "
            "implement | `test_every_asserted_rule_names_a_real_test` |",
            "| The rules table may not claim a check the suite does not "
            "implement | `test_every_asserted_rule_names_a_real_test`, "
            "`test_the_first_entry_line_is_service_then_reason` |",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    # -----------------------------------------------------------------------
    # `tests/rules/test_planning_docs.py` -- 43 rules, the largest single
    # block Stage AG registered. The subject is the planning system itself:
    # `docs/PLANS.md`, the archive, the plan documents and the recaps. Seven of
    # these mutate what Plan 146 Stage 5 mutated by hand before the writing
    # skill existed, when all eighteen assertions then in the file passed on
    # every one.
    # -----------------------------------------------------------------------
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanTableCoverage::test_every_plan_document_filename_declares_an_identifier",
        "a plan document arrives under a name the parser cannot read, so it is "
        "in no table by construction and excluded from coverage in silence",
        lambda: _write("docs/plans/notes_on_the_next_one.md", "# Notes\n"),
        [],
        ["docs/plans/notes_on_the_next_one.md"],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanTableCoverage::test_every_plan_document_appears_in_a_table",
        "a plan has a document and no row, which is how Plan 65 shipped and "
        "disappeared for four months",
        lambda: _write("docs/plans/plan_999_unclaimed.md", "# Plan 999: unclaimed\n"),
        [],
        ["docs/plans/plan_999_unclaimed.md"],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanTableCoverage::test_no_plan_number_appears_in_two_tables",
        "a plan is claimed by two tables, so 'is plan N done?' is unanswerable "
        "from the index and the reader takes whichever row they saw first",
        lambda: _edit(
            "docs/PLANS.md",
            "| Plan | Title | Priority | Effort | Trigger |\n|---|---|---:|---|---|\n",
            "| Plan | Title | Priority | Effort | Trigger |\n|---|---|---:|---|---|\n"
            "| [162](plans/plan_162_testing_census_and_restructure.md) | "
            "Testing census and CI restructure | 75 | L | **A trigger** |\n",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract"
        "::test_published_build_order_window_carries_what_this_plan_is_for",
        "a plan inside the published build-order window loses the section the "
        "public page renders, and no waiver may cover that window",
        lambda: _edit(
            "docs/plans/plan_134_archiver_endpoint_failure_contract.md",
            "## What this plan is for",
            "## What this plan was for",
        ),
        ["docs/plans/plan_134_archiver_endpoint_failure_contract.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_published_archive_window_carries_public_summary",
        "a plan inside the published archive window loses its public summary",
        lambda: _edit(
            "docs/plans/plan_151_distributed_tracing_and_runtime_topology_audit.md",
            "## Public summary",
            "## Public summary of the work",
        ),
        ["docs/plans/plan_151_distributed_tracing_and_runtime_topology_audit.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_no_waiver_covers_a_published_plan",
        "a waiver reaches into the published window, which is the one place "
        "the contract allows none",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    SectionWaiver(64), SectionWaiver(66), SectionWaiver(69), SectionWaiver(70),",
            "    SectionWaiver(162),\n"
            "    SectionWaiver(64), SectionWaiver(66), SectionWaiver(69), SectionWaiver(70),",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_live_plans_carry_what_this_plan_is_for_or_a_waiver",
        "a live plan loses the section and is covered by no waiver",
        lambda: _edit(
            "docs/plans/plan_179_derived_service_call_graph.md",
            "## What this plan is for",
            "## What this plan is about",
        ),
        ["docs/plans/plan_179_derived_service_call_graph.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_closeout_plans_carry_the_checks_or_a_waiver",
        "a plan in closeout owes `## The checks` and does not carry it, which "
        "is the section `close-out` writes on exactly that transition",
        lambda: _edit(
            "docs/plans/plan_170_container_image_reclaim_policy.md",
            "## The checks",
            "## The checks it ran",
        ),
        ["docs/plans/plan_170_container_image_reclaim_policy.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_no_waiver_outlives_the_plan_it_names",
        "a waiver names a plan that has left every live table, so it grandfathers "
        "nothing and nobody notices",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    SectionWaiver(64), SectionWaiver(66),",
            "    SectionWaiver(4), SectionWaiver(66),",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract"
        "::test_no_waiver_names_a_plan_that_postdates_the_contract",
        "a plan drafted under the contract is waived rather than fixed, which "
        "is the contract being bypassed the week it landed",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    SectionWaiver(169), SectionWaiver(171),",
            "    SectionWaiver(169), SectionWaiver(179),",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_neither_waiver_list_has_grown",
        "the escape valve widens by one legitimate-looking entry, which every "
        "per-entry check passes cleanly and only the count can see",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    SectionWaiver(149), SectionWaiver(160),\n)",
            "    SectionWaiver(149), SectionWaiver(160), SectionWaiver(154),\n)",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    # Plan 162 Stage AK, and this is the half `<=` never held. A waiver is
    # repaired, the entry goes, and the number stays where it was -- nothing
    # was raised, no rule was bypassed, and the list now has room for one
    # append that no diff has to argue for. `MAX_WHAT_THIS_PLAN_IS_FOR_WAIVERS`
    # was sitting at 35 against 34 entries when this stage arrived, so the
    # headroom was not hypothetical; it is 34 now, and this proves it stays
    # honest.
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_neither_waiver_list_has_grown",
        "a waiver is repaired and its ceiling is left above the ledger, which "
        "is headroom for a silent append rather than a repair",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    SectionWaiver(149), SectionWaiver(160),\n)",
            "    SectionWaiver(149),\n)",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanDocumentContract::test_every_live_plan_without_a_document_is_named",
        "a live plan with no document is named nowhere, so it disappears from "
        "both the compliant count and the waived one",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "NO_DOCUMENT_LIVE_PLANS = frozenset({88})",
            "NO_DOCUMENT_LIVE_PLANS = frozenset()",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestRowExitConditions::test_every_closeout_row_has_a_parsable_lands_date",
        "a closeout row's date stops parsing, and the day somebody looks is no "
        "longer a day",
        lambda: _edit(
            "docs/PLANS.md",
            "[142](plans/plan_142_planned_host_maintenance.md) | **2026-09-30**",
            "[142](plans/plan_142_planned_host_maintenance.md) | **end of September**",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestRowExitConditions::test_every_closeout_row_has_a_gate",
        "a closeout row names nothing that removes it, so the date arrives and "
        "nothing changes",
        lambda: _edit(
            "docs/PLANS.md",
            "| Plan | Lands | Gate — what removes this row |\n|---|---|---|\n",
            "| Plan | Lands | Gate — what removes this row |\n|---|---|---|\n"
            "| [66](plans/plan_66_sql_injection.md) | **2026-12-01** | -- |\n",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestRowExitConditions::test_every_backlog_row_has_a_trigger",
        "a backlog row names no trigger, and a row with no trigger is a wish",
        lambda: _edit(
            "docs/PLANS.md",
            "| [66](plans/plan_66_sql_injection.md) | SQL injection audit | 55 | M |",
            "| [66](plans/plan_66_sql_injection.md) | SQL injection audit | 55 | M | -- |\n"
            "| [66](plans/plan_66_sql_injection.md) | SQL injection audit | 55 | M |",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestBuildOrderBlockers::test_every_blocker_names_a_known_plan_or_a_date",
        "a blocker becomes a vague wait with no plan and no date, so nobody can "
        "tell when the row becomes workable",
        lambda: _edit(
            "docs/PLANS.md",
            "| **N** | Plan 125 Gate D | 76 | L |",
            "| **N** | once the dust settles | 76 | L |",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestBuildOrderBlockers::test_no_blocker_names_a_plan_that_does_not_exist",
        "a typo'd plan number reads as a real dependency and blocks a row forever",
        lambda: _edit(
            "docs/PLANS.md",
            "| **N** | Plan 112 | 74 | M |",
            "| **N** | Plan 912 | 74 | M |",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestIndexLineBudget::test_the_index_states_a_line_budget",
        "the index stops stating a budget, which makes the check below "
        "unreadable rather than false",
        lambda: _edit(
            "docs/PLANS.md",
            "**Line budget: 250 lines.**",
            "**Line allowance: 250 lines.**",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestIndexLineBudget::test_the_index_is_under_its_stated_budget",
        "the index exceeds the budget it states, which is narrative that "
        "belongs in the decision log",
        lambda: _edit(
            "docs/PLANS.md",
            "**Line budget: 250 lines.**",
            "**Line budget: 10 lines.**",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestDocumentationLinks::test_no_markdown_link_in_docs_is_dangling",
        "a link under docs/ stops resolving, which is how Plan 146 started",
        lambda: _edit(
            "docs/PLANS.md",
            "(plans/plan_66_sql_injection.md)",
            "(plans/plan_66_sql_injection_audit.md)",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestDocumentationLinks::test_the_scan_actually_reads_links",
        "the link pattern stops matching, and a link checker that matches "
        "nothing passes forever",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'    r"(?<!\\)\[[^\]]*\]\(\s*([^)\s]+?)\s*(?:\"[^\"]*\")?\s*\)"',
            r'    r"(?<!\\)\[\[[^\]]*\]\]\(\s*([^)\s]+?)\s*(?:\"[^\"]*\")?\s*\)"',
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestParserAgreesWithTheDocuments::test_every_plan_cell_parses",
        "a Plan cell stops parsing, in a file whose whole argument is that a "
        "row nobody parsed is a row nobody enforced",
        lambda: _edit(
            "docs/PLANS.md",
            "| [73](plans/plan_73_scraper_refactor.md) | Scraper code review",
            "| plan seventy-three | Scraper code review",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestParserAgreesWithTheDocuments::test_the_archive_still_holds_the_bulk_of_the_record",
        "the archive is truncated, so completed plans have gone missing rather "
        "than being archived",
        lambda: _write(
            "docs/planning/completed_plans.md",
            "\n".join(
                (REPO_ROOT / "docs/planning/completed_plans.md")
                .read_text(encoding="utf-8").splitlines()[:35]
            ) + "\n",
        ),
        ["docs/planning/completed_plans.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestSupersededRowExitConditions::test_every_superseded_row_names_what_superseded_it",
        "a superseded row names nothing that replaced it, so nobody can tell it "
        "was replaced rather than abandoned",
        lambda: _edit(
            "docs/PLANS.md",
            "| Plan | Title | Superseded by |\n|---|---|---|\n",
            "| Plan | Title | Superseded by |\n|---|---|---|\n"
            "| [66](plans/plan_66_sql_injection.md) | SQL injection audit | -- |\n",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestArchiveOrdering::test_every_archive_row_has_a_parsable_date",
        "an archive Date stops parsing, so the row sorts nowhere and the "
        "ordering check silently stops seeing it -- Plan 146 Stage 5's mutation D",
        lambda: _edit(
            "docs/planning/completed_plans.md",
            "The record is the deliverable rather than the pipeline surviving. | 2026-09-08 |",
            "The record is the deliverable rather than the pipeline surviving. "
            "| sometime in September |",
        ),
        ["docs/planning/completed_plans.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestArchiveOrdering::test_the_archive_is_newest_first",
        "a row is appended rather than prepended -- what `>>` does, and what the "
        "archive's own header says not to -- so the order inverts. Plan 146 "
        "Stage 5's mutation B",
        lambda: _edit(
            "docs/planning/completed_plans.md",
            "which the harness refused rather than skipped. | 2026-09-08 |",
            "which the harness refused rather than skipped. | 2026-09-20 |",
        ),
        ["docs/planning/completed_plans.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestBuildOrderNumbering::test_the_build_order_is_numbered_one_to_n_without_gaps",
        "the Order column jumps, which reads as a row somebody deleted -- Plan "
        "146 Stage 5's mutation C",
        # Anchored on the order number alone: a row 2 exists however the build
        # order is re-ranked, where a plan's row number moves with every insert.
        # The build order is the only table with an Order column, so `| 2 | [`
        # cannot land in the backlog, closeout or superseded tables.
        lambda: _edit("docs/PLANS.md", "| 2 | [", "| 99 | ["),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestPlanLinksNameTheirOwnPlan"
        "::test_every_linked_plan_cell_points_at_that_plans_document",
        "a Plan cell's link text and target disagree: the row shows one number "
        "and links to another plan's real document, so nothing dangles for the "
        "dangling-link check to see. Plan 146 Stage 5's mutation E",
        # Anchored on the order number alone, like mutation C: whichever plan
        # holds row 2, a leading 1 makes its link text name another number
        # while the target stays that plan's real document.
        lambda: _edit("docs/PLANS.md", "| 2 | [", "| 2 | [1"),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestNoRowVanishesSilently::test_the_census_reads_the_reconciliation_record",
        "the reconciliation record leaves the tree, removing the only defence "
        "the six documentless index rows have",
        lambda: _delete("docs/planning/plan_state_reconciliation.md"),
        ["docs/planning/plan_state_reconciliation.md"],
        [],
    ),
    # Plan 162 Stage AK. The entry above deletes the record outright, which the
    # `len(found) > 50` floor caught. This one breaks a single census pattern:
    # `_CENSUS_BOLD` stops matching, 15 rows go unresolved and the count falls
    # from 67 to **54** -- still over the old floor, which is the erosion case
    # the number could not see. Measured by breaking each of the three patterns
    # in turn: TITLED gives 43 and LIST gives 38, both of which `> 50` caught,
    # so BOLD is the one that makes the point.
    (
        "tests/rules/test_planning_docs.py"
        "::TestNoRowVanishesSilently::test_the_census_reads_the_reconciliation_record",
        "one census pattern stops matching and the reader quietly reads less",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'_CENSUS_BOLD = re.compile(r"^\*\*(\d+)\*\*$")',
            r'_CENSUS_BOLD = re.compile(r"^\*\*(\d+)\*\*\*$")',
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestNoRowVanishesSilently::test_every_reconciled_plan_is_still_claimed_by_a_table",
        "a row disappears rather than moving, which is the leak Plan 146 exists "
        "to close -- its Stage 1 sweep found 33 across 16 separate days. Stage "
        "5's mutation F",
        lambda: _edit(
            "docs/PLANS.md",
            "| [73](plans/plan_73_scraper_refactor.md) | Scraper code review and refactor |",
            "| [66](plans/plan_66_sql_injection.md) | Scraper code review and refactor |",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestTheIndexCountsTheArchiveCorrectly::test_the_index_states_the_archives_row_count",
        "the index stops claiming how many rows the archive holds, which is "
        "what made it checkable",
        lambda: _edit(
            "docs/PLANS.md",
            "— 124 rows, newest first",
            "— every finished plan, newest first",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestTheIndexCountsTheArchiveCorrectly::test_the_stated_count_matches_the_archive",
        "the index's count goes stale, which is what happens the moment the "
        "archiving skill succeeds -- archiving is two edits and this is the "
        "second, a number in a sentence nothing read",
        lambda: _edit(
            "docs/PLANS.md",
            "— 124 rows, newest first",
            "— 123 rows, newest first",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_every_recap_is_named_for_the_sunday_that_ends_its_window",
        "a recap is filed under a day that is not the Sunday ending its window, "
        "so the deferred days stop showing on the filesystem",
        lambda: _write("docs/recaps/2026-09-05.md", "# Week of 2026-08-31\n"),
        [],
        ["docs/recaps/2026-09-05.md"],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_every_recap_carries_its_required_sections",
        "a recap drops a required section, and a missing section is silence you "
        "cannot tell from an oversight",
        lambda: _edit(
            "docs/recaps/2026-09-06.md",
            "## Merges",
            "## Merged branches",
        ),
        ["docs/recaps/2026-09-06.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_every_recap_states_its_window_run_date_and_commit_count",
        "a recap loses its publication marker, and both defaults are wrong -- "
        "true publishes an unread week, false drops one off the site in silence",
        lambda: _edit(
            "docs/recaps/2026-09-06.md",
            "**Publish:** true",
            "**Published:** true",
        ),
        ["docs/recaps/2026-09-06.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_no_recap_borrows_the_archives_provenance_labels",
        "a recap reuses the archive's provenance vocabulary as generic hedging, "
        "which makes 25 backfilled archive rows look like hedging too",
        lambda: _edit(
            "docs/recaps/2026-09-06.md",
            "**Recapped:** 2026-09-07",
            "**Recapped:** 2026-09-07 *(observed)*",
        ),
        ["docs/recaps/2026-09-06.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_the_recap_series_has_no_interior_gap",
        "a week is skipped and nobody notices, which is a week whose work has "
        "no durable why",
        lambda: _delete("docs/recaps/2026-06-14.md"),
        ["docs/recaps/2026-06-14.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_the_recap_series_is_not_stale",
        "the staleness deadline moves a week forward, so a current recap set "
        "reads as stale",
        # **The deadline rather than a deletion, and the reason is decay.**
        # Deleting the newest recap would be the truer mutation on the day it
        # is written and a different one a week later: the file named here
        # becomes interior as recaps accumulate, and the entry would quietly
        # start proving `test_the_recap_series_has_no_interior_gap` instead.
        # Moving the boundary is date-independent, so it means the same thing
        # in 2027 as it does today. Unconditional on `within_grace`, because a
        # mutation that only fires Monday through Wednesday is a mutation that
        # reports MISSED on a Thursday.
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "    return last_complete - timedelta(days=7 if within_grace else 0)",
            "    return last_complete - timedelta(days=-7 if within_grace else -7)",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_the_deadline_lands_on_the_wednesday",
        "the grace window slips by a day, which is exactly the failure a rule "
        "of this shape has -- easy to state correctly in prose and wrong by one "
        "day in code",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            "RECAP_GRACE_DAYS = 3",
            "RECAP_GRACE_DAYS = 4",
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_the_recap_scan_actually_reads_recaps",
        "the sha pattern stops matching, so the check below it proves nothing "
        "and the emptying of docs/recaps/ stops being visible",
        lambda: _edit(
            "tests/rules/test_planning_docs.py",
            r'_SHORT_SHA = re.compile(r"\b[0-9a-f]{7}\b")',
            r'_SHORT_SHA = re.compile(r"\b[0-9a-f]{40}\b")',
        ),
        ["tests/rules/test_planning_docs.py"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestWeeklyRecaps::test_every_sha_a_recap_names_is_a_real_commit",
        "a recap cites a commit that does not exist, which reads as evidence",
        lambda: _edit(
            "docs/recaps/2026-09-06.md",
            "**Commits in window:** 392",
            "**Commits in window:** 392, opened by `fffff00`",
        ),
        ["docs/recaps/2026-09-06.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestTheStateParserClassifiesEveryLiveHeading::test_no_live_heading_is_silently_skipped",
        "the index grows a heading the state parser neither maps nor ignores, "
        "and the parser skips what it does not recognise -- so the table under "
        "it drops out of every state timeline in silence",
        # `## Parked`, not `## Notes to self`: the first draft used the latter
        # and was MISSED, because `notes` is one of `IGNORED_HEADING_PREFIXES`
        # and the rule was right to pass. That is the rule's own subject
        # arriving in its mutation -- a heading is unclassified only if the
        # script neither maps *nor ignores* it, and reading only the map is the
        # half-answer this entry nearly shipped.
        lambda: _edit(
            "docs/PLANS.md",
            "## Completed",
            "## Parked\n\n## Completed",
        ),
        ["docs/PLANS.md"],
        [],
    ),
    (
        "tests/rules/test_planning_docs.py"
        "::TestTheStateParserClassifiesEveryLiveHeading"
        "::test_every_live_table_section_is_mapped_to_a_state",
        "a section this file parses as a table stops being read as a state by "
        "the script -- the shape `current closeout` was in for nine days, during "
        "which every closeout plan read as absent",
        lambda: _edit(
            "scripts/audit_plan_state_history.py",
            '    "default build order": "build",\n',
            "",
        ),
        ["scripts/audit_plan_state_history.py"],
        [],
    ),
    # ------------------------------------------------------------------
    # Plan 162 Stage AA, G32. The snapshot manifest is the one document two
    # services exchange through object storage rather than over HTTP, so no
    # OpenAPI schema describes it and the contract gate cannot see it.
    # ------------------------------------------------------------------
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_the_writer_matches_the_record_for_the_version_it_stamps",
        "archiver's manifest writer gains a key and the record does not move",
        lambda: _edit(
            "archiver/processors/lake_snapshot_archive.py",
            '    manifest["archived_at"] = datetime.now(timezone.utc).isoformat()',
            '    manifest["archived_at"] = datetime.now(timezone.utc).isoformat()\n'
            '    manifest["harness_added_key"] = "x"',
        ),
        ["archiver/processors/lake_snapshot_archive.py"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_every_ops_model_declares_exactly_the_recorded_fields",
        "the reader's model loses a field the record still names -- the silent "
        "deletion this gap exists for, since FastAPI filters the response to the "
        "model and the key stops reaching callers with nothing else red",
        lambda: _edit(
            "ops/api_models.py",
            "    archived_at: str | None = None",
            "",
        ),
        ["ops/api_models.py"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_ops_has_a_model_for_every_recorded_version",
        "a recorded format loses its reader model, so every snapshot written in "
        "it answers 409 -- the case a pinned ML rehearsal depends on",
        # Re-pins the model at a format nothing recorded rather than emptying
        # `ARCHIVE_MANIFEST_MODELS`, which was the first attempt: an empty tuple
        # makes `Union[()]` raise at import, so pytest exited 4, the rule never
        # ran, and the harness reported NO RUN rather than CAUGHT. A mutation
        # has to leave the tree importable or it tests the collector.
        lambda: _edit(
            "ops/api_models.py",
            "    archive_cache_schema_version: Literal[1]",
            "    archive_cache_schema_version: Literal[7]",
        ),
        ["ops/api_models.py"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_the_fixture_builder_produces_exactly_the_recorded_shape",
        "the builder every test manifest comes from starts returning a subset, "
        "putting the hand-written fixture's defect back with a derivation's "
        "reputation",
        lambda: _edit(
            "scripts/generate_lake_snapshot_manifest_contract.py",
            '    manifest = build(records[pair]["shape"])',
            '    manifest = build(records[pair]["shape"])\n'
            '    manifest.pop("tier", None)',
        ),
        ["scripts/generate_lake_snapshot_manifest_contract.py"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_the_generator_check_passes_as_a_subprocess",
        "the generator stops working outside an interpreter that has already "
        "imported archiver, so CI's step fails while these in-process imports pass",
        lambda: _edit(
            "scripts/generate_lake_snapshot_manifest_contract.py",
            "if str(REPO_ROOT) not in sys.path:\n    sys.path.insert(0, str(REPO_ROOT))",
            "",
        ),
        ["scripts/generate_lake_snapshot_manifest_contract.py"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_every_record_is_valid_json_and_names_its_own_version",
        "a retired record is edited to declare a version its filename does not, "
        "which nothing can settle -- the writer is gone and the archives cannot "
        "be re-read",
        lambda: _edit(
            "contracts/lake_snapshot_manifest/export3-archive1.json",
            '"archive_cache_schema_version": 1,',
            '"archive_cache_schema_version": 9,',
        ),
        ["contracts/lake_snapshot_manifest/export3-archive1.json"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_the_registry_is_not_empty",
        "the registry directory empties and every rule above it loops over "
        "nothing and passes",
        lambda: _delete("contracts/lake_snapshot_manifest/export3-archive1.json"),
        ["contracts/lake_snapshot_manifest/export3-archive1.json"],
        [],
    ),
    (
        "tests/rules/test_lake_snapshot_manifest_registry.py"
        "::test_the_response_fidelity_plugin_is_registered",
        "the plugin registration is dropped, so a response model short of its "
        "handler goes back to deleting keys in production with the suite green",
        lambda: _edit(
            "pyproject.toml",
            " -p tests.plugins.response_model_fidelity",
            "",
        ),
        ["pyproject.toml"],
        [],
    ),
    (
        "tests/rules/test_response_models_match_their_producers.py"
        "::test_no_response_model_is_short_of_its_producer",
        "a processor grows a key its endpoint's response model does not declare, "
        "so FastAPI deletes it on the way out -- invisible to the runtime plugin "
        "because the endpoint's tests mock that processor, and to the contract "
        "gate because the artifact is built from the model",
        lambda: _edit(
            "archiver/processors/pack_bronze_html.py",
            '        "buckets": [],\n    }',
            '        "buckets": [],\n        "harness_added_key": 1,\n    }',
        ),
        ["archiver/processors/pack_bronze_html.py"],
        [],
    ),
    (
        "tests/rules/test_response_models_match_their_producers.py"
        "::test_the_producer_corpus_is_not_empty",
        "the decorator keyword this rule reads is renamed, so no route resolves "
        "to a producer and the rule above compares nothing over an empty corpus",
        # Plan 162 Stage AA moved the route reader into tests/response_fixtures.py
        # so the fixture builder and the rules could share one resolver; the
        # anchor followed it.
        lambda: _edit(
            "tests/response_fixtures.py",
            'if keyword.arg == "response_model":',
            'if keyword.arg == "response_model_renamed":',
        ),
        ["tests/response_fixtures.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_shape.py"
        "::test_no_mock_invents_a_shape_production_defines",
        "a mock goes back to restating a producer's shape as a literal, which "
        "is the transcription this stage drained 57 of -- correct on the day "
        "and free to drift the next, with the suite green either way",
        lambda: _edit(
            "tests/ops/routers/test_maintenance.py",
            'produced_by("_reap_stuck_processing", stuck=0, retried=0, skipped=0)',
            '{"stuck": 0, "retried": 0, "skipped": 0}',
        ),
        ["tests/ops/routers/test_maintenance.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_shape.py"
        "::test_the_mock_site_corpus_is_not_empty",
        "the producer reader stops resolving, so the rule above walks an empty "
        "corpus and accuses nobody rather than failing",
        lambda: _edit(
            "tests/response_fixtures.py",
            "            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):",
            "            if isinstance(node, ast.ClassDef):",
        ),
        ["tests/response_fixtures.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_shape.py"
        "::test_every_waiver_names_a_site_that_still_exists",
        "a waiver names a mock site that no longer fabricates, so it "
        "grandfathers nothing and hides whichever site takes that line next",
        lambda: _edit(
            "tests/rules/test_no_mock_invents_a_shape.py",
            "FABRICATED_PRODUCER_WAIVERS: tuple[str, ...] = ()",
            'FABRICATED_PRODUCER_WAIVERS: tuple[str, ...] = ("tests/nowhere.py:1",)',
        ),
        ["tests/rules/test_no_mock_invents_a_shape.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_no_mock_invents_a_service_response",
        "a caller's test goes back to writing another service's response down "
        "by hand, which passes for whatever its author typed and keeps passing "
        "when that service changes",
        lambda: _edit(
            "tests/ops/test_coordination_release.py",
            'response.json.return_value = service_response(\n'
            '        "container_health", "GET", "/project-status/{project}",\n'
            "    )",
            'response.json.return_value = {"known": False}',
        ),
        ["tests/ops/test_coordination_release.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_the_service_seam_corpus_is_not_empty",
        "a caller stops resolving to the service it calls -- here by the compose "
        "file no longer naming the dockerfile that says which package answers "
        "-- so every fabrication behind that seam goes unreported",
        lambda: _edit(
            "docker-compose.yml",
            "      dockerfile: container_health/Dockerfile",
            "      dockerfile: Dockerfile",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_every_waiver_names_a_fabrication_that_still_exists",
        "a waiver names a fabrication that has been repaired, so it "
        "grandfathers nothing and hides whichever one lands on that line next",
        lambda: _edit(
            "tests/rules/test_no_mock_invents_a_service_response.py",
            "FABRICATED_RESPONSE_WAIVERS: tuple[str, ...] = ()",
            'FABRICATED_RESPONSE_WAIVERS: tuple[str, ...] = ("tests/nowhere.py:1",)',
        ),
        ["tests/rules/test_no_mock_invents_a_service_response.py"],
        [],
    ),
    # Plan 162 Stage AB. These rules stand between the suite and every system
    # this repository does not build, and each of them is a set difference --
    # the shape that reads green by matching nothing. So the mutations here
    # lean toward emptying a reader rather than only toward breaking a value:
    # an anchor that quietly stopped matching would disarm the census in
    # silence, which is the failure the census exists to name.
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_census_entry_is_replayed_or_carries_a_reason",
        "a census entry is left with neither a replay nor a reason",
        lambda: _edit(
            "tests/external_vocabulary_census.py",
            '"verdict": OUT_OF_SCOPE,\n        "why": (\n'
            '            "Replaying it means sending a message.',
            '"verdict": "pending",\n        "why": (\n'
            '            "Replaying it means sending a message.',
        ),
        ["tests/external_vocabulary_census.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_replayed_census_entry_names_something_that_exists",
        "a census entry names a replay whose file has been renamed away",
        lambda: _edit(
            "tests/external_vocabulary_census.py",
            '            "tests/rules/test_external_vocabularies.py"\n'
            '            "::test_every_restated_curl_cffi_target_is_a_real_browser_type"',
            '            "tests/rules/test_external_vocabularies_renamed.py"\n'
            '            "::test_every_restated_curl_cffi_target_is_a_real_browser_type"',
        ),
        ["tests/external_vocabulary_census.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_census_site_still_exists",
        "a census entry goes on naming a site that has moved",
        lambda: _edit(
            "tests/external_vocabulary_census.py",
            '"sites": ("shared/challenge.py",),',
            '"sites": ("shared/challenge_markers.py",),',
        ),
        ["tests/external_vocabulary_census.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_no_declared_vocabulary_is_empty",
        "a members tuple is emptied, which turns every rule keyed on it green",
        lambda: _edit(
            "tests/external_vocabulary_census.py",
            '"members": ("failed",),',
            '"members": (),',
        ),
        ["tests/external_vocabulary_census.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_no_test_fabricates_a_cars_com_status_production_has_never_seen",
        "a test fabricates a cars.com status the recorded corpus does not hold",
        lambda: _edit(
            "tests/scraper/processors/test_scrape_detail.py",
            # Six sites in this file assign 403 and four share the whole
            # `<html>blocked</html>` pair, so the anchor rides the one whose
            # body is bare `b"blocked"` -- unique today and loud when it
            # stops being.
            '        mock_resp.status_code = 403\n'
            '        mock_resp.content = b"blocked"',
            '        mock_resp.status_code = 429\n'
            '        mock_resp.content = b"blocked"',
        ),
        ["tests/scraper/processors/test_scrape_detail.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_no_test_fabricates_a_cars_com_status_production_has_never_seen",
        "the recorded corpus drifts away from what the suite fabricates",
        lambda: _edit("tests/fixtures/external/cars_com_responses.json", "    403,\n", ""),
        ["tests/fixtures/external/cars_com_responses.json"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_cars_com_corpus_is_not_empty",
        "the corpus is emptied, which would make the rule above vacuous",
        # **This entry was a false CAUGHT until Stage AG landed.** Its node
        # name was bare, so it resolved into `test_testing_contract.py` where
        # no such test exists, and pytest's exit 4 read as a failure under the
        # old `code != 0`. The payload was wrong too: it dropped two statuses
        # from a list of seven against a floor of two, so even correctly
        # addressed it would not have tripped the rule. Both halves are fixed
        # here, and the second was only visible once the first was.
        lambda: _edit(
            "tests/fixtures/external/cars_com_responses.json",
            '"statuses_observed": [\n    200,\n    302,\n    403,\n'
            '    500,\n    502,\n    503,\n    504\n  ],',
            '"statuses_observed": [],',
        ),
        ["tests/fixtures/external/cars_com_responses.json"],
        [],
    ),
    # Plan 162 Stage AK, and the entry above wrote this one's justification two
    # stages early: *"it dropped two statuses from a list of seven against a
    # floor of two, so even correctly addressed it would not have tripped the
    # rule."* That payload was abandoned as unusable. Against the equality that
    # replaced the floor it is exactly the right payload, so here it is --
    # `statuses_observed` loses one of its seven while `observations` keeps all
    # ten rows, which is what a half-failed recording looks like and what
    # `>= 2` was blind to.
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_cars_com_corpus_is_not_empty",
        "the corpus summary loses a status its own observations still record",
        lambda: _edit(
            "tests/fixtures/external/cars_com_responses.json",
            '"statuses_observed": [\n    200,\n    302,\n    403,\n'
            '    500,\n    502,\n    503,\n    504\n  ],',
            '"statuses_observed": [\n    200,\n    403,\n'
            '    500,\n    502,\n    503,\n    504\n  ],',
        ),
        ["tests/fixtures/external/cars_com_responses.json"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_corpus_records_which_instrument_saw_each_status",
        "an observation loses the instrument that produced it",
        lambda: _edit(
            "tests/fixtures/external/cars_com_responses.json",
            '"instrument": "prometheus",',
            '"instrument": "",',
        ),
        ["tests/fixtures/external/cars_com_responses.json"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_restated_curl_cffi_target_is_a_real_browser_type",
        "the census names an impersonation target curl_cffi does not have",
        lambda: _edit(
            "tests/external_vocabulary_census.py",
            '"chrome146",',
            '"chrome146", "chrome999",',
        ),
        ["tests/external_vocabulary_census.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_declared_curl_cffi_targets_are_the_ones_the_scraper_holds",
        "the scraper's target list drifts away from the census",
        lambda: _edit("scraper/processors/cf_session.py", '    (146, "chrome146"),\n', ""),
        ["scraper/processors/cf_session.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_no_chrome_target_curl_cffi_offers_is_missing_from_the_scraper",
        "curl_cffi offers a target the scraper never added -- the silent direction",
        lambda: _edit("scraper/processors/cf_session.py", '    (145, "chrome145"),\n', ""),
        ["scraper/processors/cf_session.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_airflow_state_the_dags_compare_against_is_declared",
        "a DAG compares a task state against a word the census does not declare",
        lambda: _edit(
            "airflow/dags/notifications.py",
            'state)) == "failed" and task_id',
            'state)) == "upstream_failed" and task_id',
        ),
        ["airflow/dags/notifications.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_every_observed_cars_com_status_is_handled",
        "a status the corpus has observed loses its handling and takes the catchall",
        lambda: _edit(
            "scraper/fetch_outcomes.py",
            "    503: FetchOutcome.TRANSIENT,\n",
            "",
        ),
        ["scraper/fetch_outcomes.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_unknown_catchall_is_reachable",
        "the catchall stops being conservative and lets an unknown body reach the parser",
        lambda: _edit(
            "scraper/fetch_outcomes.py",
            "    return outcome in (FetchOutcome.OK, FetchOutcome.BLOCKED)",
            "    return outcome is not FetchOutcome.REDIRECTED",
        ),
        ["scraper/fetch_outcomes.py"],
        [],
    ),
    # The Cloudflare marker set, and the two directions that make it a
    # discriminator rather than a string search. Plan 128's outage was eight
    # hours of interstitials counted as successful scrapes; the fix that was
    # tried first keyed on `cdn-cgi/challenge-platform`, which Cloudflare
    # injects into *every* cars.com page including the good ones. So the
    # positive case alone is not evidence, and both mutations below exist
    # because either one alone would leave the other direction unproved.
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_challenge_marker_set_still_classifies_the_recorded_interstitial",
        "the marker set narrows and stops recognising a captured interstitial",
        lambda: _edit(
            "shared/challenge.py",
            r'r"just a moment|attention required|checking your browser"',
            r'r"attention required|checking your browser"',
        ),
        ["shared/challenge.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_a_real_detail_page_is_not_classified_as_an_interstitial",
        "the marker set widens until it matches a real detail page too",
        lambda: _edit(
            "shared/challenge.py",
            r'r"just a moment|attention required|checking your browser"',
            r'r"just a moment|attention required|checking your browser|cars"',
        ),
        ["shared/challenge.py"],
        [],
    ),
    (
        "tests/rules/test_external_vocabularies.py"
        "::test_the_curl_cffi_version_is_pinned_exactly",
        "curl_cffi loosens to a floor, and the target rules stop having a referent",
        lambda: _edit(
            "scraper/requirements.txt",
            "curl_cffi==0.16.3",
            "curl_cffi>=0.16.3",
        ),
        ["scraper/requirements.txt"],
        [],
    ),
    (
        "tests/rules/test_the_artifact_declares_what_the_handler_returns.py"
        "::test_the_artifact_declares_no_code_its_handler_cannot_return",
        "a handler stops producing a code its committed contract declares, so "
        "the artifact describes a response nothing sends -- which no other rule "
        "reads, because they check the decorator and the AST rather than the "
        "generated file",
        lambda: _edit(
            "ops/routers/admin.py",
            "status_code=409, context=context,",
            "status_code=418, context=context,",
        ),
        ["ops/routers/admin.py"],
        [],
    ),
    (
        "tests/rules/test_the_artifact_declares_what_the_handler_returns.py"
        "::test_the_artifact_declaration_corpus_is_not_empty",
        "a route stops resolving to a handler in its own service, so the rule "
        "above measures less than the whole surface while still passing",
        lambda: _edit(
            "tests/rules/test_the_artifact_declares_what_the_handler_returns.py",
            "    suffix = f\"_{slug.lstrip('_')}_{verb}\"",
            '    suffix = "_" + "_".join(p for p in f"{slug}_{verb}".split("_") if p)',
        ),
        ["tests/rules/test_the_artifact_declares_what_the_handler_returns.py"],
        [],
    ),
    (
        "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"
        "::test_no_rule_guards_itself_with_a_guessed_number",
        "a floor is loosened from a derived equality back to a chosen number, "
        "which is the exact edit this stage made and then had to undo",
        lambda: _edit(
            "tests/rules/test_no_mock_invents_a_shape.py",
            "    assert expected and not missing, (",
            "    assert len(shapes) > 50 and not missing, (",
        ),
        ["tests/rules/test_no_mock_invents_a_shape.py"],
        [],
    ),
    (
        "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"
        "::test_every_waiver_names_a_bound_that_still_exists",
        "a waiver names a floor that is no longer a guess, so it grandfathers "
        "nothing and hides whichever bound is written next",
        lambda: _edit(
            "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py",
            'GUESSED_BOUND_WAIVERS: tuple[str, ...] = (\n',
            'GUESSED_BOUND_WAIVERS: tuple[str, ...] = (\n'
            '    "tests/rules/nowhere.py: len(x) >= 99",\n',
        ),
        ["tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"],
        [],
    ),
    (
        "tests/rules/test_no_rule_guards_itself_with_a_guessed_number.py"
        "::test_every_rule_file_is_read",
        "a rule module stops parsing, so its floors go unexamined and the rule "
        "above reports on a directory it only partly read",
        lambda: _write(
            "tests/rules/test_harness_unparseable.py",
            "def test_x(:\n",
        ),
        # `created`, not `snapshot`: the harness restores a snapshot by writing
        # its content back, which would leave this file in the tree. A file the
        # mutation brings into existence has to be named as one so it is
        # removed again.
        [],
        ["tests/rules/test_harness_unparseable.py"],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_no_mock_invents_a_code_the_service_cannot_answer",
        "a caller's test writes down a status its callee does not declare, "
        "which is the body defect one field over and just as silent: the test "
        "goes on agreeing with itself whatever the service answers",
        # Anchored with the enclosing `def`, because the assignment alone
        # appears three times in that module and an ambiguous anchor mutates a
        # site nobody chose.
        lambda: _edit(
            "tests/ops/routers/test_admin.py",
            "def test_dbt_docs_generate_ok(mock_client, mock_requests, "
            "mock_dbt_context, mock_templates):\n"
            '    mock_requests["post"].return_value.status_code = 200',
            "def test_dbt_docs_generate_ok(mock_client, mock_requests, "
            "mock_dbt_context, mock_templates):\n"
            '    mock_requests["post"].return_value.status_code = 599',
        ),
        ["tests/ops/routers/test_admin.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_the_fabricated_code_reader_is_not_blind",
        "the shared fixture goes back to patching the global `requests`, which "
        "is the defect this stage found: the seam then names no service, so "
        "the rule above reads nothing and reports nothing",
        lambda: _edit(
            "tests/conftest.py",
            '"get": mocker.patch("ops.routers.admin.http_requests.get"),',
            '"get": mocker.patch("requests.get"),',
        ),
        ["tests/conftest.py"],
        [],
    ),
    (
        "tests/rules/test_no_mock_invents_a_service_response.py"
        "::test_no_mock_invents_a_service_response",
        "a body goes back to being written by hand inside a `side_effect` "
        "list -- the shape that was invisible to this reader until it learned "
        "to follow sequences, and the one that hid two invented "
        "`ops/coordination_release.py` bodies while the rule reported zero",
        lambda: _edit(
            "tests/ops/test_coordination_release.py",
            '        service_response(\n'
            '            "container_health", "GET", "/project-status/{project}",\n'
            '            services=["lakekeeper"],\n'
            '        ),',
            '        {"known": True, "services": ["lakekeeper"]},',
        ),
        ["tests/ops/test_coordination_release.py"],
        [],
    ),
    (
        "tests/rules/test_no_service_imports_another_services_package.py"
        "::test_no_service_imports_another_services_package",
        "a service's import into another service moves to a module the ledger "
        "does not name -- a new cross-service dependency, and the seeded entry "
        "beside it goes stale, so both directions of the rule fire at once",
        lambda: _edit(
            "ops/coordination_release.py",
            "from container_health.expected import EXPECTED_SERVICES, "
            "HEALTHCHECK_EXEMPT_SERVICES",
            "from container_health.collector import EXPECTED_SERVICES, "
            "HEALTHCHECK_EXEMPT_SERVICES",
        ),
        ["ops/coordination_release.py"],
        [],
    ),
    (
        "tests/rules/test_no_service_imports_another_services_package.py"
        "::test_the_service_root_corpus_is_complete",
        "a service's dockerfile path stops naming its package, and the "
        "compose derivation loses that whole service's code -- every import "
        "it makes then goes unread rather than unwaived",
        lambda: _edit(
            "docker-compose.yml",
            "      dockerfile: dashboard/Dockerfile",
            "      dockerfile: Dockerfile",
        ),
        ["docker-compose.yml"],
        [],
    ),
    (
        "tests/rules/test_every_endpoint_has_a_caller.py"
        "::test_every_endpoint_has_a_caller_or_is_declared_externally_reachable",
        "the one caller of container_health's /oneoff-processes moves to a "
        "path no contract declares, and the endpoint keeps answering with "
        "nobody left to notice -- the dbt_runner deletion shape, before the "
        "deletion",
        lambda: _edit(
            "ops/coordination_drain.py",
            '}/oneoff-processes"',
            '}/oneoff-processes-moved"',
        ),
        ["ops/coordination_drain.py"],
        [],
    ),
    (
        "tests/rules/test_every_endpoint_has_a_caller.py"
        "::test_the_endpoint_coverage_readers_are_not_blind",
        "Prometheus stops scraping the scraper while its contract still "
        "declares GET /metrics -- dead observability, and the coverage set "
        "quietly shrinking under the caller rule",
        lambda: _edit(
            "prometheus/prometheus.yml",
            "      - targets: ['scraper:8000']",
            "      - targets: []",
        ),
        ["prometheus/prometheus.yml"],
        [],
    ),
    (
        "tests/rules/test_no_test_fabricates_a_response_objects_behaviour.py"
        "::test_no_test_fabricates_a_response_objects_behaviour",
        "the 503-as-busy test rebuilds its bare mock under a fresh name -- a "
        "fabricated response object the ledger does not key, and the entry it "
        "abandoned goes stale, so both directions fire at once",
        lambda: _edit(
            "tests/ops/test_coordination_drain.py",
            "def test_service_503_body_is_still_known_positive_evidence(mocker):\n"
            "    response = mocker.Mock()\n"
            "    response.json.return_value = service_response(",
            "def test_service_503_body_is_still_known_positive_evidence(mocker):\n"
            "    response = mocker.Mock()\n"
            "    fresh = response\n"
            "    fresh.json.return_value = service_response(",
        ),
        ["tests/ops/test_coordination_drain.py"],
        [],
    ),
    (
        "tests/rules/test_no_test_fabricates_a_response_objects_behaviour.py"
        "::test_the_object_reader_sees_the_shape_a_bare_mock_takes",
        "the reader's predicate quietly narrows to one of the two shapes, and "
        "every side_effect fabrication in the suite stops being read",
        lambda: _edit(
            "tests/rules/test_no_test_fabricates_a_response_objects_behaviour.py",
            '_SHAPES = (".json.return_value", ".json.side_effect")',
            '_SHAPES = (".json.return_value",)',
        ),
        ["tests/rules/test_no_test_fabricates_a_response_objects_behaviour.py"],
        [],
    ),
    (
        "tests/rules/test_the_refusal_envelope_is_one_declaration.py"
        "::test_the_refusal_envelope_is_one_declaration",
        "the container_health copy of the envelope quietly changes what its "
        "database 503 means, and the two files answer differently for the "
        "same class -- the drift a copy nothing compares always reaches",
        lambda: _edit(
            "container_health/api_envelope.py",
            '    meaning = "Database unavailable."',
            '    meaning = "The database is briefly busy."',
        ),
        ["container_health/api_envelope.py"],
        [],
    ),
    (
        "tests/rules/test_the_refusal_envelope_is_one_declaration.py"
        "::test_the_envelope_declares_what_a_refusal_needs",
        "a refusal class leaves the DECLARED_REFUSALS registry while its "
        "class body stays -- resolvable by name to nothing, so every lookup "
        "rule reads one member fewer with nothing going red",
        lambda: _edit(
            "shared/api_envelope.py",
            "        Busy,\n        DatabaseUnavailable,",
            "        DatabaseUnavailable,",
        ),
        ["shared/api_envelope.py"],
        [],
    ),
    (
        "tests/rules/test_the_artifact_declares_what_the_handler_returns.py"
        "::test_every_handlers_exits_are_readable",
        "a readable handler grows an exit the reader cannot resolve -- a "
        "bare-dict return -- and slips out of the judged set without joining "
        "the ledger, which is exactly how 81 of 93 declarations went "
        "unjudged with everything green",
        lambda: _edit(
            "ops/routers/public.py",
            '    return FileResponse(page, media_type="text/html")',
            "    outcome = dict(page=str(page))\n    return outcome",
        ),
        ["ops/routers/public.py"],
        [],
    ),
    (
        "tests/rules/test_no_call_site_retypes_a_declared_status.py"
        "::test_no_call_site_retypes_a_status_the_envelope_declares",
        "a handler's retyped 404 becomes a retyped 409 -- a new member "
        "literal the ledger does not key, and the abandoned entry goes "
        "stale, so both directions fire at once",
        lambda: _edit(
            "processing/routers/artifact.py",
            "            raise HTTPException(\n"
            "                status_code=404,",
            "            raise HTTPException(\n"
            "                status_code=409,",
        ),
        ["processing/routers/artifact.py"],
        [],
    ),
    (
        "tests/rules/test_no_call_site_retypes_a_declared_status.py"
        "::test_the_retyping_reader_sees_every_shape",
        "the shared walker stops reading the HTTPException positional shape, "
        "and every raise in five packages goes unread while the ledger "
        "entries for the two other shapes keep the rule looking alive",
        lambda: _edit(
            "tests/rules/test_no_call_site_retypes_a_declared_status.py",
            '            if (\n'
            '                name == "HTTPException"\n'
            "                and node.args",
            '            if (\n'
            '                name == "NeverThisException"\n'
            "                and node.args",
        ),
        ["tests/rules/test_no_call_site_retypes_a_declared_status.py"],
        [],
    ),
    (
        "tests/rules/test_a_status_code_means_the_same_thing_to_both.py"
        "::test_every_declared_meaning_of_an_ambiguous_code_is_proven",
        "a handler whose 503 provably means what its route declares starts "
        "raising a different meaning under the same code -- the declaration "
        "goes on describing what the handler used to say",
        lambda: _edit(
            "ops/routers/coordination.py",
            '        raise HTTPException(status_code=409, '
            'detail="Coordination is not requested.")\n'
            '    raise HTTPException(status_code=503, '
            'detail="Database unavailable.")',
            '        raise HTTPException(status_code=409, '
            'detail="Coordination is not requested.")\n'
            '    raise HTTPException(status_code=503, '
            'detail="The database took a holiday.")',
        ),
        ["ops/routers/coordination.py"],
        [],
    ),
    (
        "tests/rules/test_a_status_code_means_the_same_thing_to_both.py"
        "::test_the_meaning_rule_has_something_to_read",
        "the ambiguity filter quietly stops matching any code, and the "
        "meaning rule reads nothing while its 28-entry ledger keeps it "
        "looking busy",
        lambda: _edit(
            "tests/rules/test_a_status_code_means_the_same_thing_to_both.py",
            "        if len(meanings) > 1",
            "        if len(meanings) > 99",
        ),
        ["tests/rules/test_a_status_code_means_the_same_thing_to_both.py"],
        [],
    ),
    (
        "tests/rules/test_a_status_code_means_the_same_thing_to_both.py"
        "::test_every_ambiguous_meaning_is_declared_somewhere",
        "Busy's meaning text collapses into DependencyUnreachable's, so the "
        "vocabulary looks fully declared and the entry recording that "
        "nothing declares busy goes stale",
        lambda: _edit(
            "shared/api_envelope.py",
            '    meaning = "Busy: jobs are in flight, and the body carries '
            'the evidence."',
            '    meaning = "A dependency this service needs is not '
            'reachable."',
        ),
        ["shared/api_envelope.py"],
        [],
    ),
    (
        "tests/rules/test_every_response_declares_a_shape_or_a_kind.py"
        "::test_every_response_declares_a_shape_or_a_kind",
        "a ledgered bodyless response gains its kind declaration and the "
        "entry stays behind -- the list stops describing the debt, which is "
        "the direction that rots every ledger that only fails one way",
        lambda: _edit(
            "contracts/scraper.json",
            '        "operationId": "metrics_metrics_get",\n'
            '        "responses": {\n'
            '          "200": {\n'
            '            "description": "Successful Response"\n'
            "          }",
            '        "operationId": "metrics_metrics_get",\n'
            '        "responses": {\n'
            '          "200": {\n'
            '            "content": {"text/plain": {}},\n'
            '            "description": "Successful Response"\n'
            "          }",
        ),
        ["contracts/scraper.json"],
        [],
    ),
    (
        "tests/rules/test_every_response_declares_a_shape_or_a_kind.py"
        "::test_the_body_reader_still_tells_the_three_apart",
        "the shape classifier keys on a media type that does not exist, "
        "every JSON body reads as a kind, and the counts the rule beside it "
        "relies on go wrong with nothing else red",
        lambda: _edit(
            "tests/rules/test_every_response_declares_a_shape_or_a_kind.py",
            '                    if (content.get("application/json") or {})'
            '.get("schema"):',
            '                    if (content.get("application/jsonx") or {})'
            '.get("schema"):',
        ),
        ["tests/rules/test_every_response_declares_a_shape_or_a_kind.py"],
        [],
    ),
    (
        "tests/rules/test_no_module_calls_a_service_by_hand.py"
        "::test_no_module_calls_a_service_by_hand",
        "a ledgered module stops naming its owned host -- converted, or its "
        "call went dead -- and the entry stays behind, which is the ledger "
        "no longer describing the callers it exists to watch",
        lambda: _edit(
            "airflow/dags/dbt_build.py",
            'DBT_RUNNER_URL = "http://dbt_runner:8080"',
            'DBT_RUNNER_URL = "http://dbt-runner-elsewhere:8080"',
        ),
        ["airflow/dags/dbt_build.py"],
        [],
    ),
    (
        "tests/rules/test_no_module_calls_a_service_by_hand.py"
        "::test_the_caller_signature_and_the_resolver_agree",
        "a DAG's call path drifts off its service's contract while the host "
        "stays named -- the dead-call shape, visible only because two "
        "independent readers are held equal",
        lambda: _edit(
            "airflow/dags/dbt_build.py",
            'result = post_json(f"{DBT_RUNNER_URL}/dbt/build", '
            "payload=payload, timeout=600)",
            'result = post_json(f"{DBT_RUNNER_URL}/dbt/build-all", '
            "payload=payload, timeout=600)",
        ),
        ["airflow/dags/dbt_build.py"],
        [],
    ),
    (
        "tests/rules/test_every_declared_code_is_one_the_standard_names.py"
        "::test_every_declared_code_is_one_the_standard_names",
        "a route declares a code the standard does not name -- the stage "
        "exit's own demonstration, and the shape /admin's 307 already has",
        lambda: _edit(
            "contracts/processing.json",
            '          "404": {\n'
            '            "content": {\n'
            '              "application/json": {\n'
            '                "schema": {\n'
            '                  "$ref": "#/components/schemas/ErrorResponse"',
            '          "418": {\n'
            '            "content": {\n'
            '              "application/json": {\n'
            '                "schema": {\n'
            '                  "$ref": "#/components/schemas/ErrorResponse"',
        ),
        ["contracts/processing.json"],
        [],
    ),
    (
        "tests/rules/test_every_declared_code_is_one_the_standard_names.py"
        "::test_every_standard_code_is_declared_somewhere",
        "the standard's table grows a row no route declares -- dead "
        "vocabulary, a meaning nobody can rely on anybody honouring",
        lambda: _edit(
            "docs/TESTING.md",
            "| `400` | the request is unusable as sent | a rejection of its "
            "*content*, which is `422` |",
            "| `400` | the request is unusable as sent | a rejection of its "
            "*content*, which is `422` |\n"
            "| `402` | payment is required | anything this repository "
            "answers |",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "tests/rules/test_every_declared_code_is_one_the_standard_names.py"
        "::test_the_envelope_stays_inside_the_standard",
        "the envelope grows a code its owner's table does not have -- the "
        "copy outgrowing the standard it restates",
        lambda: _edit(
            "shared/api_envelope.py",
            "    status_code = 400",
            "    status_code = 402",
        ),
        ["shared/api_envelope.py"],
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
                _drop_rewritten_bytecode(rel)
            for rel in created:
                path = REPO_ROOT / rel
                path.unlink(missing_ok=True)
                _drop_rewritten_bytecode(rel)
                if path.parent != REPO_ROOT and path.parent.exists():
                    if not any(path.parent.iterdir()):
                        path.parent.rmdir()
        # **Exit 1 exactly, not merely non-zero.** pytest exits 1 when a test
        # fails and something else when it could not run one: 4 for a node it
        # cannot find, 5 for nothing collected, 2 for a module that would not
        # import. Reading every non-zero code as CAUGHT conflates *the rule
        # noticed* with *the harness never asked it*, and both halves of that
        # have already happened here. Stage AF found the import half -- an
        # `UNDOCUMENTED` append landed outside its tuple, pytest exited on a
        # `SyntaxError`, and this loop called it a success. Stage AG found the
        # other half: six entries named a bare node for a rule living in
        # `test_ci_compose_parity.py` or `test_planning_docs.py`, so the target
        # resolved to `TEST::<name>`, pytest exited 4 saying *not found*, and
        # all six had reported CAUGHT since the day they were written while
        # never running the rule at all. Stage AF caught its case by a human
        # reading the failure; this is the same catch made mechanical.
        caught = code == 1
        if not caught:
            label = "*** MISSED ***" if code == 0 else f"*** NO RUN ({code}) ***"
            missed.append(
                description if code == 0
                else f"{description} [pytest exited {code}: the rule never ran]"
            )
        print(f"{'CAUGHT' if caught else label:14} {description}")

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
