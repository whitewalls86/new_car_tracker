"""Every production statement executed somewhere in this run.

Plan 162 Stage X. This is the half of the execution recorder that no single
pytest job can be: a statement may execute in any of five, so no one job's
record is a coverage reading and only something downstream of all of them is.

**What it replaces.** ``test_every_production_sql_file_is_touched_by_a_layer_2_
test`` credits a file when a Layer 2 module names its stem as a whole word.
That is a proxy for execution and this plan has called it the weakest available
reading from the day it was written. Here the question is answered directly:
did this file's text reach a database client in this run?

Two checks, and the second is narrower than it looks:

* **Every production ``.sql`` file was executed. No waiver list.** It landed
  with one, shaped like the ``*_WAIVERS`` tuples, and the reading came back 161
  of 161 -- so the ledger was deleted rather than kept empty. An empty ledger
  and no ledger differ in exactly one way: what happens the next time a
  statement executes nowhere. With one, the cheapest repair is a tuple append.
  Without one, the cheapest repair is a Layer 2 test, and re-introducing the
  escape hatch costs a diff that has to argue for it. An execution carries
  every file it was composed from, so a statement formatted into another
  credits both: the first reading of this gate called fourteen archiver
  selectors unexecuted when each was running nested inside
  ``wrap_candidate_query.sql``, which would have bought fourteen tests for
  statements that already had coverage.
* **The routes SQL travels are the ones this repository declares.** Every
  execution records *how* it arrived -- ``duckdb.execute``,
  ``psycopg2.execute_values``. Compared to :data:`EXECUTION_ROUTES` in both
  directions: a route nobody declared fails, and a declared route nothing used
  fails as stale. This is the answer to "can we say deterministically how SQL
  gets executed here", and it is derived rather than declared-and-trusted --
  the list is checked against what a real CI run actually did, so it cannot
  quietly stop describing the repository.

  **It is a tripwire, not a filter.** The recorder records every string handed
  to every driver method; this list does not decide what gets recorded. That
  distinction is the whole difference between it and the method-name inventory
  it replaced, which decided what got seen and so could go silently blind.

* **No execution is unattributable.** An execution with no origins whose text
  matches a ``.sql`` file means something loaded that file and lost its
  provenance on the way -- a loader returning a plain ``str``, a transformation
  ``SqlText`` does not survive. It caught ``airflow/dags/dag_queries.py``.

  **It does not catch a helper that composes a new statement**, because the
  composed text matches no file. ``psycopg2``'s ``execute_values`` is that
  shape, and what caught it was the first check, not this one: its file stopped
  appearing as an origin at all. Recorded here so the second check is not read
  as covering the first's ground.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

#: How SQL reaches an engine in this repository, asserted both ways against
#: what a CI run actually recorded. Adding a route here is a decision; a route
#: appearing that is not here is a question worth asking, because the last time
#: the recorder could not see a route it reported working statements as dead.
EXECUTION_ROUTES = frozenset({
    # Measured, not guessed. ``duckdb.execute`` and the two psycopg2 routes are
    # what a run actually records; ``executemany`` was in the first draft of
    # this list and came straight back out, because nothing in the repository
    # calls it -- which is the stale direction earning its place before this
    # ever reached CI.
    "duckdb.execute",
    "psycopg2.execute",
    "psycopg2.execute_values",
    "dbt.target/run",
})

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tests.test_testing_contract import production_sql_files  # noqa: E402


def _normalise(text: str) -> str:
    """Comment- and whitespace-insensitive, matching the census's own reading."""
    return " ".join(re.sub(r"--[^\n]*", " ", text).split()).lower()


def _load(records: Path) -> list[dict]:
    files = sorted(records.rglob("*.json"))
    if not files:
        raise SystemExit(
            f"no execution records under {records}. Every pytest job writes one "
            f"into $SQL_EXECUTION_RECORD and uploads it; if none arrived, the "
            f"recorder did not load and this gate is measuring nothing."
        )
    executions: list[dict] = []
    for path in files:
        executions.extend(json.loads(path.read_text(encoding="utf-8"))["executions"])
    print(f"read {len(executions)} executions from {len(files)} record(s)")
    return executions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=Path, default=REPO_ROOT / "sql-execution-records")
    parser.add_argument(
        "--report",
        action="store_true",
        help="print the reading and exit 0. For seeding the ledger the first "
             "time, when no honest value for it exists yet.",
    )
    arguments = parser.parse_args()

    executions = _load(arguments.records)
    executed = {origin for e in executions for origin in e["origins"]}
    produced = list(production_sql_files())
    # Only executions carrying a known ``.sql`` file, so the reading is "how
    # our SQL reaches an engine" and not "every string any driver method took".
    # The proxy records table names and encodings too; those are harmless in the
    # record and would be noise in this list.
    routes = {e["via"] for e in executions if e["origins"]}
    undeclared = sorted(routes - EXECUTION_ROUTES)
    unused = sorted(EXECUTION_ROUTES - routes)

    unrecorded = sorted(set(produced) - executed)

    corpus = {
        _normalise((REPO_ROOT / relative).read_text(encoding="utf-8")): relative
        for relative in produced
    }
    unattributable = sorted({
        corpus[_normalise(e["statement"])]
        for e in executions
        if not e["origins"] and _normalise(e["statement"]) in corpus
    })

    print(f"execution routes:      {', '.join(sorted(routes))}")
    print(f"production .sql files: {len(produced)}")
    print(f"recorded executing:    {len(produced) - len(unrecorded)}")
    print(f"not recorded:          {len(unrecorded)}")

    if unrecorded:
        print("\nnot recorded executing:")
        for relative in unrecorded:
            print(f"    {relative}")
    if undeclared:
        print("\nSQL arrived by a route this repository does not declare:")
        for route in undeclared:
            print(f"    {route}")
    if unused:
        print("\ndeclared routes nothing used (stale):")
        for route in unused:
            print(f"    {route}")
    if unattributable:
        print("\nexecuted but unattributable -- a loader lost the origin:")
        for relative in unattributable:
            print(f"    {relative}")

    if arguments.report:
        print("\n--report: measuring only, not gating.")
        return 0

    failed = False
    if unrecorded:
        print(
            f"\nFAIL: {len(unrecorded)} production statement(s) executed nowhere "
            "in this run.\n\n"
            "**Check this before writing a test for them.** This gate reports "
            "the recorder's blind spots the same way it reports real gaps: if a "
            "statement reaches its engine by a route the recorder does not "
            "watch, it is invisible here and reads as dead while running "
            "perfectly well. That has already happened once -- fourteen "
            "archiver selectors reported unexecuted because each ran nested "
            "inside another statement -- and the repair it pointed at was "
            "fourteen redundant tests. Confirm the statement genuinely has no "
            "Layer 2 test before writing one; the route list above is the first "
            "place to look.\n\n"
            "If it is a real gap, give each one a Layer 2 test that runs it "
            "against a real engine. There is deliberately no waiver list: this "
            "gate read 161 of 161 when it landed, and an escape hatch nobody "
            "needs is an escape hatch the next person reaches for. If a "
            "statement genuinely cannot execute in CI, say so in a diff that "
            "argues for it rather than in a tuple."
        )
        failed = True
    if undeclared:
        print(
            f"\nFAIL: SQL arrived by {len(undeclared)} route(s) this repository "
            "does not declare. Either production started reaching an engine a "
            "new way -- worth knowing, and the reason this check exists -- or "
            "EXECUTION_ROUTES has gone stale. Add the route once you know which."
        )
        failed = True
    if unused:
        print(
            f"\nFAIL: {len(unused)} declared route(s) carried no SQL. A list "
            "that still names a route nothing uses stops describing how this "
            "repository executes anything."
        )
        failed = True
    if unattributable:
        print(
            f"\nFAIL: {len(unattributable)} file(s) executed without their "
            "origin. Something loaded them and returned a plain str; the "
            "recorder cannot credit what it cannot attribute."
        )
        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
