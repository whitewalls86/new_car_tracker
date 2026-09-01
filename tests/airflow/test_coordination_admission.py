"""Plan 142 scoped Airflow admission declarations and gate contract."""

import ast
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.coordination_contract import SURFACES
from ops.coordination_drain import gate_observation_query

REPO_ROOT = Path(__file__).parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
# Modules in airflow/dags/ that define no DAG and so declare no admission
# surface. `notifications` is the shared Telegram failure notifier (Plan 134
# Stage 1) — it is called *by* notify tasks and never gated by one.
SUPPORT_MODULES = {"coordination_contract", "notifications", "pools", "sensors"}


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
    assert '{"requested", "draining", "active", "validating"}' in sensor_source

    # Plan 158 moved the write into a module-level helper so it is reachable
    # from both hold paths and executable by tests/integration/sql.
    observation = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_record_observation"
    )
    write_source = ast.get_source_segment(source, observation)
    assert "coordination_gate_observations" in source
    assert "ON CONFLICT (generation, dag_id, run_id)" in source
    assert "GATE_OBSERVATION_SQL" in write_source
    assert "_record_observation(" in sensor_source


# ===========================================================================
# Plan 158 -- the seam between the gate and the drain
#
# Everything above reads source with `ast`. These drive the real sensor,
# because the defect Plan 158 fixes was invisible to a source-shape test: both
# halves of poke() were correct and the composition was dead. Real Airflow is
# not installed in the unit suite, so the two names sensors.py imports are
# stubbed; nothing else about the sensor is faked.
#
# The gate SELECT uses the jsonb `?|` operator and cannot run outside Postgres,
# so the row it returns is supplied directly. What the observation INSERT does
# in Postgres, and whether it actually empties the drain's count, is
# tests/integration/sql/test_ops_queries.py's business.
# ===========================================================================

BLOCKING_PHASES = ("requested", "draining", "active", "validating")
ALL_PHASES = ("none",) + BLOCKING_PHASES
RUN_ID = "scheduled__2026-08-30T06:00:00+00:00"


def _install_airflow_stubs() -> None:
    """sensors.py imports two Airflow names; the unit suite has no Airflow."""
    if "airflow.sdk.bases.sensor" in sys.modules:
        return

    class BaseSensorOperator:
        def __init__(self, **kwargs):
            self.sensor_kwargs = kwargs

    class PostgresHook:
        """Import-time placeholder; every test replaces it with a recorder."""

        def __init__(self, **kwargs):
            raise AssertionError("the sensor must be driven with a recording hook")

    for name in (
        "airflow.sdk",
        "airflow.sdk.bases",
        "airflow.providers",
        "airflow.providers.postgres",
        "airflow.providers.postgres.hooks",
    ):
        sys.modules.setdefault(name, types.ModuleType(name))
    sensor_module = types.ModuleType("airflow.sdk.bases.sensor")
    sensor_module.BaseSensorOperator = BaseSensorOperator
    sys.modules["airflow.sdk.bases.sensor"] = sensor_module
    hook_module = types.ModuleType("airflow.providers.postgres.hooks.postgres")
    hook_module.PostgresHook = PostgresHook
    sys.modules["airflow.providers.postgres.hooks.postgres"] = hook_module


def _load_sensors():
    """Import airflow/dags/sensors.py the way the DAGs directory does."""
    _install_airflow_stubs()
    for name in ("coordination_contract", "sensors"):
        if name in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(name, DAGS_DIR / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules["sensors"]


def _gate_row(intent="none", phase="none", intersects=False, generation=17):
    """One row of the gate SELECT, in the column order that query declares.

    Guarded by test_the_gate_read_selects_the_columns_this_row_supplies.
    """
    return (intent, phase, intersects, generation)


class _RecordingHook:
    """PostgresHook stand-in: canned gate read, recorded writes."""

    def __init__(self, gate):
        self._gate = gate

    def get_first(self, sql, parameters=None):
        self._gate.reads.append((sql, parameters))
        return self._gate.row

    def run(self, sql, parameters=None):
        self._gate.writes.append((sql, parameters))


class _Gate:
    """Drives the real sensor against a settable coordination read."""

    def __init__(self, sensors, mocker):
        self.sensors = sensors
        self.row = _gate_row()
        self.reads = []
        self.writes = []
        mocker.patch.object(sensors, "PostgresHook", lambda **_kwargs: _RecordingHook(self))

    def poke(self, dag_id="orphan_checker", run_id=RUN_ID, context=None):
        sensor = self.sensors.deploy_intent_sensor(dag_id)
        dag_run = SimpleNamespace(run_id=run_id) if run_id is not None else None
        return sensor.poke({"dag_run": dag_run} if context is None else context)

    @property
    def observations(self):
        """The (generation, dag_id, run_id) keys written, in write order."""
        for sql, _ in self.writes:
            assert "coordination_gate_observations" in sql
            assert "(generation, dag_id, run_id, observed_at)" in sql
        return [parameters[:3] for _, parameters in self.writes]


@pytest.fixture
def gate(mocker):
    return _Gate(_load_sensors(), mocker)


def test_the_gate_read_selects_the_columns_this_row_supplies():
    """_gate_row's positions are only meaningful if the real SELECT agrees."""
    source = (DAGS_DIR / "sensors.py").read_text()
    sensor = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ClassDef) and node.name == "_DeployIntentSensor"
    )
    select = next(
        node.value
        for node in ast.walk(sensor)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.lstrip().upper().startswith("SELECT")
    )
    positions = [
        select.index(column)
        for column in ("di.intent", "cs.phase", "AS intersects", "cs.generation")
    ]
    assert all(position > 0 for position in positions)
    assert positions == sorted(positions)


# --- Plan 158 test 1: the seam. This is the test whose absence let the defect
# --- ship, and it must fail against the sensor as it stood at 220395a.
def test_a_run_parked_by_deploy_intent_records_exactly_one_observation(gate):
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=17)

    assert gate.poke(dag_id="orphan_checker", run_id=RUN_ID) is False
    assert gate.observations == [(17, "orphan_checker", RUN_ID)]


def test_each_live_run_of_a_dag_is_observed_under_its_own_key(gate):
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=17)

    gate.poke(dag_id="orphan_checker", run_id="run-a")
    gate.poke(dag_id="orphan_checker", run_id="run-b")
    gate.poke(dag_id="results_processing", run_id="run-a")

    assert gate.observations == [
        (17, "orphan_checker", "run-a"),
        (17, "orphan_checker", "run-b"),
        (17, "results_processing", "run-a"),
    ]


# --- Plan 158 test 2 (unit half): every live affected run leaves a key the
# --- drain's NOT EXISTS can find. That the count then reads zero is proved
# --- against real SQL in tests/integration/sql/test_ops_queries.py.
def test_every_live_affected_run_leaves_the_key_the_drain_looks_up(gate):
    scope = frozenset({"processing"})
    generation = 17
    query = gate_observation_query(scope, generation)
    affected, drain_generation = query[1][:-1], query[1][-1]
    live_runs = [(dag_id, f"{dag_id}-run") for dag_id in affected]

    gate.row = _gate_row(
        intent="pending", phase="draining", intersects=True, generation=generation
    )
    for dag_id, run_id in live_runs:
        gate.poke(dag_id=dag_id, run_id=run_id)

    assert set(gate.observations) == {
        (drain_generation, dag_id, run_id) for dag_id, run_id in live_runs
    }


# --- Plan 158 test 5: the ON CONFLICT path.
def test_repeated_pokes_write_one_key_not_one_per_poke(gate):
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=17)

    for _ in range(3):
        assert gate.poke() is False

    assert set(gate.observations) == {(17, "orphan_checker", RUN_ID)}
    assert all(
        "ON CONFLICT (generation, dag_id, run_id)" in sql
        and "DO UPDATE SET observed_at = EXCLUDED.observed_at" in sql
        for sql, _ in gate.writes
    )


# --- Plan 158 test 4 (unit half): the key carries the generation it observed.
def test_an_observation_is_written_against_the_generation_it_saw(gate):
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=17)
    gate.poke()
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=18)
    gate.poke()

    assert gate.observations == [
        (17, "orphan_checker", RUN_ID),
        (18, "orphan_checker", RUN_ID),
    ]


def test_an_unavailable_run_id_still_blocks_and_writes_nothing(gate):
    """The pre-existing guard: no run_id, no key to write, still no admission."""
    gate.row = _gate_row(intent="pending", phase="draining", intersects=True, generation=17)

    assert gate.poke(run_id=None, context={"dag_run": None}) is False
    assert gate.poke(run_id=None, context={"dag_run": None, "run_id": ""}) is False
    assert gate.writes == []


# --- Plan 158 test 3: admission is unchanged. The sensor returns False in
# --- exactly the cases it returned False at 220395a, which was:
# ---     intent != "none"                       -> False
# ---     phase in BLOCKING_PHASES and intersects -> False
# ---     otherwise                               -> True
# --- and that is the expectation computed below, over the full cross product.
@pytest.mark.parametrize("intersects", [True, False])
@pytest.mark.parametrize("phase", ALL_PHASES)
@pytest.mark.parametrize("intent", ["none", "pending", "deploying"])
def test_admission_is_unchanged_by_the_observation_write(gate, intent, phase, intersects):
    gate.row = _gate_row(intent=intent, phase=phase, intersects=intersects)

    expected = intent == "none" and not (phase in BLOCKING_PHASES and intersects)
    assert gate.poke() is expected


def test_a_missing_coordination_row_fails_closed_and_writes_nothing(gate):
    gate.row = None

    assert gate.poke() is False
    assert gate.writes == []


def test_an_unblocked_run_records_nothing(gate):
    """The observation means "parked at the gate"; an admitted run is not."""
    gate.row = _gate_row(intent="none", phase="draining", intersects=False)

    assert gate.poke() is True
    assert gate.writes == []


# --- The sensor's `intersects` term and the drain's dag_id filter both derive
# --- from ADMISSION_SURFACES. They must not drift apart.
@pytest.mark.parametrize("surface", sorted(SURFACES - {"host"}))
def test_the_drain_and_the_sensor_read_the_same_declaration(gate, surface):
    scope = frozenset({surface})
    query = gate_observation_query(scope, 17)
    drained = set(query[1][:-1]) if query else set()
    gated = {
        dag_id
        for dag_id in _load_contract()["ADMISSION_SURFACES"]
        if set(gate.sensors.deploy_intent_sensor(dag_id).admission_surfaces) & scope
    }

    assert drained == gated
