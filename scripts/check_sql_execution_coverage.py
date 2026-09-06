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

    print(f"production .sql files: {len(produced)}")
    print(f"recorded executing:    {len(produced) - len(unrecorded)}")
    print(f"not recorded:          {len(unrecorded)}")

    if unrecorded:
        print("\nnot recorded executing:")
        for relative in unrecorded:
            print(f"    {relative}")
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
            "in this run. Give each one a Layer 2 test that runs it against a "
            "real engine. There is deliberately no waiver list: this gate read "
            "161 of 161 when it landed, and an escape hatch nobody needs is an "
            "escape hatch the next person reaches for. If a statement genuinely "
            "cannot execute in CI, say so in a diff that argues for it rather "
            "than in a tuple."
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
