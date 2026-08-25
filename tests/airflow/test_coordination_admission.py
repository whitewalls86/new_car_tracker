"""Plan 142 scoped Airflow admission declarations and gate contract."""

import ast
from pathlib import Path

import pytest

from ops.coordination_contract import SURFACES

REPO_ROOT = Path(__file__).parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
SUPPORT_MODULES = {"coordination_contract", "pools", "sensors"}


def _load_contract():
    namespace = {}
    path = DAGS_DIR / "coordination_contract.py"
    exec(compile(path.read_text(), str(path), "exec"), namespace)  # noqa: S102
    return namespace


def _sensor_declarations() -> dict[str, str]:
    declarations = {}
    for path in DAGS_DIR.glob("*.py"):
        if path.name in {"sensors.py", "coordination_contract.py"}:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "deploy_intent_sensor"
        ]
        for call in calls:
            assert len(call.args) == 1 and isinstance(call.args[0], ast.Constant), (
                f"{path.name} must name one literal DAG declaration"
            )
            declarations[path.stem] = call.args[0].value
    return declarations


def test_every_declared_dag_has_one_matching_gate_and_no_stale_declaration():
    declared = _load_contract()["ADMISSION_SURFACES"]
    gated = _sensor_declarations()
    dag_modules = {
        path.stem
        for path in DAGS_DIR.glob("*.py")
        if not path.name.startswith("_") and path.stem not in SUPPORT_MODULES
    }

    assert set(declared) == dag_modules, (
        "Every DAG module must declare admission surfaces; new support-only "
        "modules require an explicit SUPPORT_MODULES classification."
    )
    assert set(gated) == dag_modules
    assert all(filename == dag_id for filename, dag_id in gated.items())


def test_dag_admission_sets_use_only_canonical_nonempty_surfaces():
    declared = _load_contract()["ADMISSION_SURFACES"]

    for dag_id, surfaces in declared.items():
        assert surfaces, f"{dag_id} has an empty admission set"
        assert surfaces <= SURFACES - {"host"}, dag_id


def test_unknown_dag_fails_closed():
    admission_surfaces = _load_contract()["admission_surfaces"]

    with pytest.raises(ValueError, match="no coordination declaration"):
        admission_surfaces("future_mutating_dag")


def test_every_dag_names_real_mutating_tasks_for_drain_evidence():
    contract = _load_contract()
    declared = contract["ADMISSION_SURFACES"]
    drain_tasks = contract["DRAIN_TASKS"]

    assert set(drain_tasks) == set(declared)
    for dag_id, task_ids in drain_tasks.items():
        assert task_ids, dag_id
        source = (DAGS_DIR / f"{dag_id}.py").read_text()
        literal_task_ids = {
            node.value
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        assert task_ids <= literal_task_ids, f"{dag_id}: stale drain task declaration"
        assert not any("sensor" in task_id or "notify" in task_id for task_id in task_ids)


def test_sensor_is_scoped_dual_signal_rescheduling_and_practically_unbounded():
    source = (DAGS_DIR / "sensors.py").read_text()
    tree = ast.parse(source)
    factory = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "deploy_intent_sensor"
    )
    factory_source = ast.get_source_segment(source, factory)

    assert "dag_id=dag_id" in factory_source
    assert 'mode="reschedule"' in factory_source
    assert "timeout=timedelta.max" in factory_source
    assert "silent_fail=True" in factory_source
    assert "timeout=600" not in factory_source

    sensor = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "_DeployIntentSensor"
    )
    sensor_source = ast.get_source_segment(source, sensor)
    assert "deploy_intent" in sensor_source
    assert "coordination_state" in sensor_source
    assert "scope ? 'host'" in sensor_source
    assert "scope ?| %s::text[]" in sensor_source
