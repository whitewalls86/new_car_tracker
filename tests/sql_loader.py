"""Where a test's SQL lives, and how it gets there.

Plan 162 Stage X. Production SQL has lived in ``.sql`` files since Plan 162
Stage L, held there by a rule that fails on a statement written in Python.
``tests/`` was exempt, for the reason
[Plan 161 question 3](../docs/plans/plan_161_testing_contract.md) gave: a
checker that cannot tell a fixture seed from a paraphrase of production fails
on correct code. **That premise stops holding once no SQL literal appears in a
test file at all** -- the ambiguity has nothing left to live in -- which is why
this module exists and why the judgement rule it replaces is gone from the
contract.

**The tree mirrors the test tree, down to the module.** ``tests/sql/`` is
laid out so that the directory holding a module's statements is derivable from
the module rather than declared by it: statements for
``tests/integration/ops/test_scrape.py`` live in
``tests/sql/integration/ops/test_scrape/``. Mirroring only as far as the
*directory* was the alternative and was rejected on collision -- two modules
beside each other both want ``seed_artifact``, and resolving that by hand is a
naming judgement repeated 500 times. Mirroring to the module means a name has
to be unique only where a reader can see all of them at once.

**This root is not the production census.** ``tests/`` is exempt from
:func:`tests.test_testing_contract.production_sql_files`, so nothing here
inflates the production denominator or owes a Layer 2 test. That is what
answers Stage T's circularity objection without keeping the exemption: a
read-back assertion is still not a production statement -- it is simply no
longer a literal typed inside a test.

What every file here *does* owe is
``test_every_test_statement_plans_against_the_migrated_schema``, which
``PREPARE``s it against a Flyway-migrated Postgres whether or not the test
consuming it runs.
"""
from pathlib import Path
from typing import Callable

from shared.query_loader import SqlText, load_query

TESTS_ROOT = Path(__file__).resolve().parent
SQL_ROOT = TESTS_ROOT / "sql"


def sql_dir_for(module_file: str) -> Path:
    """The directory holding *module_file*'s statements, derived not declared."""
    relative = Path(module_file).resolve().relative_to(TESTS_ROOT).with_suffix("")
    return SQL_ROOT / relative


def queries(module_file: str) -> Callable[[str], SqlText]:
    """``SQL = queries(__file__)`` at the top of a module; then ``SQL("name")``.

    Returns :class:`~shared.query_loader.SqlText`, so a statement a test
    executes carries its origin into the execution recorder exactly as a
    production statement does.
    """
    directory = sql_dir_for(module_file)

    def load(name: str) -> SqlText:
        return load_query(directory, name)

    return load
