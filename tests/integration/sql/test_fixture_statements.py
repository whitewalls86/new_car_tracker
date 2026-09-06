"""Layer 2 — every statement `tests/` owns, planned against the real schema.

Plan 162 Stage X. The sibling modules here execute *production* statements and
assert on what comes back. This one executes nothing and asserts about the
suite's own SQL: that every statement under ``tests/sql/`` still parses and
plans against a Flyway-migrated Postgres.

**The point is that it does not wait for the test that uses it.** Today a
fixture seed is checked only if its own test happens to run — skip the suite,
run `-m "not integration"`, or leave the seed behind a branch nobody takes, and
a renamed column goes unnoticed until something else breaks. That is the same
conditional coverage [G14](../../../docs/TESTING.md#the-gap-list) found on the
production side, and it is why this module iterates the *files* rather than
anything the suite executes.

**`PREPARE` is the instrument, and its limits are the reason the real suites
still exist.** `PREPARE stmt AS <sql>` parses the statement and plans it
against the live catalogue: a renamed column, a dropped table, a mistyped
comparison all fail loudly, with no rows written and nothing to clean up. It
does **not** run the statement, so it catches no constraint violation, and it
refuses DDL outright. What it buys is unconditional coverage of the half that
drifts most — the column and relation names — for every statement at once.

Three things it is not asked to plan, each derived rather than listed:

* **Another engine's statements**, which live under a ``duckdb/`` path segment.
  The engine comes from the path and the default is Postgres, so a statement
  for an engine nobody has thought about lands in the default bucket and fails
  here until it is filed. A table of engines would have to be remembered.
* **Another schema's**, under ``airflow/``: ``airflow.dag_run`` is created by
  ``airflow db migrate``, not by Flyway, so a Flyway-only database cannot plan
  it. `tests/integration/sql/test_airflow_dag_queries.py` owns those.
* **Templates the call site computes**, and only those. A statement holding a
  ``{placeholder}`` is no longer skipped for holding one: ``tests/sql_bindings``
  reads the bindings out of the module that owns it and this plans every text
  the statement is actually executed as. What is still waived against G19 in
  ``tests/test_testing_contract.py`` is the one statement whose placeholder is
  filled by ``", ".join(...)`` over a per-case list, which exists only at run
  time. That ledger is asserted in both directions at Layer 0 — this module
  cannot, since a job with no Postgres never reaches it.
"""
import re

import psycopg2
import pytest

from tests.sql_bindings import holds_a_placeholder, renderings
from tests.test_testing_contract import (
    TEST_SQL_TEMPLATE_WAIVERS,
    _relative,
    postgres_test_statements,
)

pytestmark = pytest.mark.integration

_WAIVED = {waiver.subject for waiver in TEST_SQL_TEMPLATE_WAIVERS}


def _to_dollar_placeholders(sql: str) -> str:
    """``%s`` is psycopg2's client-side marker; ``PREPARE`` wants ``$1``.

    The rewrite is positional and total, which is what keeps it honest: a
    statement whose parameters do not line up fails to plan rather than
    silently planning a different one.
    """
    count = 0

    def replace(_match: re.Match) -> str:
        nonlocal count
        count += 1
        return f"${count}"

    return re.sub(r"%s", replace, sql)


def _checkable():
    return [
        path for path in postgres_test_statements()
        if _relative(path) not in _WAIVED
    ]


def test_there_is_something_to_check():
    """A glob that silently matches nothing is a green test that checks nothing.

    This is the same guard `test_every_production_sql_file_is_touched_by_a_
    layer_2_test` puts on its own corpus, and for the same reason: every other
    assertion in this module is a loop, and a loop over an empty list passes.
    """
    assert len(_checkable()) > 250, (
        f"only {len(_checkable())} test statements found under tests/sql/ — "
        "the tree moved, or the engine filter is eating the corpus"
    )


def _plannable(path):
    """Every text *path* reaches an engine as: itself, or each of its renderings.

    A template is planned as the statements it actually becomes rather than
    skipped. ``tests/sql_bindings`` reads the bindings out of the call site --
    ``RECEIPT_TABLE``, the tuples the modules iterate, the ``parametrize`` rows
    -- so ``select_h_from_table`` is planned five times, once per protected
    table, and a rename to any one of them fails here.

    Every rendering rather than the first, because they are different
    statements: five relation names is five catalogue lookups, and planning one
    of them proves nothing about the other four.

    **The placeholder check comes first, and it is not an optimisation.**
    Reading a call site means importing the module that owns it, and importing
    a module under ``tests/integration/`` runs its package ``conftest`` --
    ``tests/integration/ops/conftest.py`` imports the FastAPI app, whose
    ``Form`` routes need ``python-multipart``, which this job does not install.
    Asking for bindings a statement does not have imported the whole test tree
    to answer a question about a statement with nothing to bind, and turned a
    dependency absent by design into a failure of this suite. A statement with
    no placeholder is its own rendering and needs no call site at all.
    """
    text = path.read_text(encoding="utf-8")
    if not holds_a_placeholder(text):
        return [text]
    rendered = renderings(path)
    return rendered if rendered is not None else [text]


def test_every_test_statement_plans_against_the_migrated_schema(cur):
    """Every one of them, reported together rather than one failure at a time.

    Collecting all the failures matters more here than in an ordinary test: a
    migration that renames a column breaks every statement that reads it, and
    the useful output is the list, not whichever file sorts first.
    """
    refused = []
    for path in _checkable():
        for statement in _plannable(path):
            try:
                cur.execute(
                    "PREPARE _fixture_probe AS " + _to_dollar_placeholders(statement)
                )
            except psycopg2.Error as error:
                first = str(error).strip().splitlines()[0]
                refused.append(f"{_relative(path)}: {first}")
                cur.connection.rollback()
            else:
                cur.execute("DEALLOCATE _fixture_probe")

    assert not refused, (
        f"{len(refused)} statement(s) under tests/sql/ no longer plan against "
        "the migrated schema:\n  " + "\n  ".join(sorted(refused))
    )
