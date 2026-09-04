"""
The DAG's accepted statuses are the exporter's, read from the exporter.

`check_snapshot_result` accepted only `{"created"}` for a non-dry-run while
`export_ci_lake_snapshot` returned `"exported"`. A DAG-triggered export would
have published its archive and both pointers and then failed the task. Nothing
caught it for two reasons worth separating:

* the DAG had never run — `airflow.dag_run` held zero rows for it, so
  production could not report the defect either;
* and `tests/integration/airflow/test_export_ci_lake_snapshot_dag.py` seeds
  `{"status": ...}` itself, so both halves of the contract are written in one
  file. That test passes for any string the author picks, including one the
  exporter never emits.

This is the missing half: the strings come from the two modules, not from here.
It is the general shape Plan 162 Stage 10 names — **a DAG-side checker keyed on
a string a service returns needs one test that reads the string from the
service.**

Runs in the main venv, so it cannot import the DAG (which imports `airflow`).
It reads the DAG by AST and imports the exporter directly, the same split
`test_coordination_admission.py` uses.
"""
import ast
from pathlib import Path

from archiver.processors import export_ci_lake_snapshot as exporter

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG = REPO_ROOT / "airflow" / "dags" / "export_ci_lake_snapshot.py"
EXPORTER = REPO_ROOT / "archiver" / "processors" / "export_ci_lake_snapshot.py"


def _dag_accepted_statuses() -> set[str]:
    """Every string literal assigned to `acceptable` in the DAG module.

    AST rather than import, and a set literal rather than a call, so this reads
    the same three branches a reviewer sees: audit, dry run, and a real export.
    """
    tree = ast.parse(DAG.read_text(encoding="utf-8"), filename=str(DAG))
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "acceptable"
            for target in node.targets
        ):
            continue
        assert isinstance(node.value, ast.Set), (
            "`acceptable` is expected to be a set literal so this test can read "
            "it without importing airflow"
        )
        for element in node.value.elts:
            assert isinstance(element, ast.Constant), element
            found.add(element.value)
    return found


def _exporter_emitted_statuses() -> set[str]:
    """Every string the exporter passes as `status=` when building a result."""
    tree = ast.parse(EXPORTER.read_text(encoding="utf-8"), filename=str(EXPORTER))
    return {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "status"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    }


def test_the_dag_accepts_only_statuses_the_exporter_emits():
    """The assertion that would have caught it.

    A status the DAG accepts and the exporter never returns is a branch that can
    only ever fail — which is exactly what `created` was.
    """
    accepted = _dag_accepted_statuses()
    emitted = _exporter_emitted_statuses()
    assert accepted, "no `acceptable` set found in the DAG module"
    unreachable = sorted(accepted - emitted)
    assert not unreachable, (
        f"the DAG accepts {unreachable}, which the exporter never returns. "
        f"It emits {sorted(emitted)}."
    )


def test_the_exporters_success_status_is_accepted():
    """The other direction, and the one that actually bit.

    The check above passes on a DAG that accepts nothing at all. This one fails
    unless a successful export is a status the DAG will let through.
    """
    assert "exported" in _exporter_emitted_statuses(), (
        "the exporter no longer returns 'exported' on success; this test and the "
        "DAG's `acceptable` set both need updating together"
    )
    assert "exported" in _dag_accepted_statuses()


def test_the_status_this_test_reads_is_the_one_the_dataclass_carries():
    """Guards the AST reader itself.

    Reading `status=` keywords is only as good as the exporter continuing to
    build its result that way. If `SnapshotResult` grew a default or the status
    were assigned rather than passed, the reader above would return a smaller
    set and quietly stop asserting anything.
    """
    result = exporter.SnapshotResult(
        snapshot_id="adaptive-refresh-x", tier="ci", status="exported",
    )
    assert result.status in _exporter_emitted_statuses()
