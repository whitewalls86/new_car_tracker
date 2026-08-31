"""Plan 140 Stage 4: http_health_sensor is a gate, not a notifier.

Plan 140 opens with a page that said `DAG scrape_listings failed` while the
actual fault was an Airflow apiserver connection pool. That is what a health
signal looks like when its only consumer is a DAG sensor: it arrives late and
named after a downstream component. Stages 1-3 built the replacement --
healthchecks on every service, `cartracker_container_health`, and CI coverage
-- and the `flaresolverr` fire test on 2026-08-20 showed the alert going
Pending inside a minute, far ahead of any DAG run.

The sensors themselves stay. They are load-bearing for DAG correctness: do not
start scraping if the scraper is unreachable. Only the notification moves.

These read DAG source with `ast` rather than building a DagBag, matching
tests/airflow/test_maintenance_pool.py, so they run in the ordinary suite
without Airflow installed.
"""
import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
SENSORS = DAGS_DIR / "sensors.py"

HEALTH_FACTORY = "http_health_sensor"


def _dag_files():
    return sorted(p for p in DAGS_DIR.glob("*.py") if not p.name.startswith("_"))


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def _kwarg(call: ast.Call, name: str):
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _health_sensor_vars(tree: ast.Module) -> set:
    """Names bound to an http_health_sensor(...) call, e.g. `archiver_up`."""
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        func = node.value.func
        if isinstance(func, ast.Name) and func.id == HEALTH_FACTORY:
            names.update(
                t.id for t in node.targets if isinstance(t, ast.Name)
            )
    return names


def _notify_vars(tree: ast.Module) -> set:
    """Names bound to an operator whose task_id is a notification task."""
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        task_id = _kwarg(node.value, "task_id")
        if isinstance(task_id, ast.Constant) and task_id.value == "notify":
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return names


class TestTheSensorSkipsRatherThanFails:
    def test_the_factory_sets_soft_fail(self):
        """The whole demotion, in one keyword.

        Without it a timeout raises AirflowSensorTimeout, fails the run, and
        increments airflow_dagrun_duration_failed_count -- which is the input
        to ct-pipeline-failures, whose annotation is "DAG {{ $labels.dag_id }}
        failed". The service that is actually down appears nowhere in it.
        """
        tree = _tree(SENSORS)
        func = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == HEALTH_FACTORY
        )
        call = next(
            n for n in ast.walk(func)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "_ServiceHealthSensor"
        )
        soft_fail = _kwarg(call, "soft_fail")
        assert isinstance(soft_fail, ast.Constant) and soft_fail.value is True, (
            f"{HEALTH_FACTORY} no longer sets soft_fail=True, so a down service "
            "pages again as a DAG failure named after the wrong component"
        )

    def test_the_sensor_is_not_deferrable(self):
        """Airflow issue #61130: a deferrable sensor ignores soft_fail on
        timeout and fails anyway. Switching modes here would silently undo the
        demotion while leaving soft_fail=True in place to suggest otherwise.

        Checked as a keyword argument rather than as a substring of the file:
        this module documents why deferrable mode is wrong, and a text search
        cannot tell an explanation from a setting.
        """
        passed = [
            kw.arg for node in ast.walk(_tree(SENSORS))
            if isinstance(node, ast.Call)
            for kw in node.keywords
            if kw.arg == "deferrable"
        ]
        assert not passed, (
            "sensors.py passes `deferrable`. Deferrable sensors do not honour "
            "soft_fail on timeout in Airflow 3 (apache/airflow#61130), which "
            "restores the DAG-failure page Plan 140 Stage 4 removed."
        )

    def test_the_gate_survives_the_demotion(self):
        """A skip is not a removal. Every DAG that had a health sensor still
        has one, and it is still upstream of the work -- Plan 140 is explicit
        that these must be demoted rather than deleted."""
        wired = {
            path.name for path in _dag_files()
            if HEALTH_FACTORY in path.read_text()
        } - {"sensors.py"}
        # 15 when Plan 140 Stage 4 landed; 13 since Plan 134's survey deleted
        # cleanup_parquet.py and cleanup_artifacts.py, whose endpoint had been
        # a no-op since V036 dropped raw_artifacts.
        assert len(wired) == 13, (
            f"{len(wired)} DAGs wire a health sensor, expected 13. If a DAG "
            "dropped its sensor, work can now start against a service that is "
            "not answering; the sensors gate DAG correctness independently of "
            "who reports the outage."
        )

    def test_deploy_intent_is_left_alone(self):
        """Deliberately untouched. A stuck deploy intent is a different
        condition with a different owner -- Plan 142 Stage 1 item 3 replaces
        that sensor's 600-second failure with a maintenance-aware gate -- and
        silently skipping it here would let work start mid-deploy."""
        tree = _tree(SENSORS)
        func = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "deploy_intent_sensor"
        )
        assert "soft_fail" not in ast.get_source_segment(SENSORS.read_text(), func)


class TestNoSensorFeedsANotifier:
    """The other half, and the one that was literally wired.

    `hourly_analytics_refresh` and `pack_bronze_html` each took a health sensor
    as a direct upstream of a `trigger_rule="one_failed"` Telegram task, so a
    health failure sent "hourly analytics refresh FAILED" -- Plan 140's founding
    complaint, in a message rather than an alert. soft_fail alone would have
    quieted these, because a skipped upstream does not satisfy `one_failed`, but
    relying on that leaves the wiring in place to be reactivated by a later
    trigger_rule change.
    """

    @pytest.mark.parametrize(
        "filename", ["hourly_analytics_refresh.py", "pack_bronze_html.py"]
    )
    def test_the_notify_task_does_not_depend_on_a_health_sensor(self, filename):
        tree = _tree(DAGS_DIR / filename)
        sensors = _health_sensor_vars(tree)
        notifiers = _notify_vars(tree)
        assert sensors and notifiers, f"{filename} no longer has both to check"

        for node in ast.walk(tree):
            if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.RShift):
                continue
            downstream = {
                n.id for n in ast.walk(node.right) if isinstance(n, ast.Name)
            }
            if not (downstream & notifiers):
                continue
            upstream = {
                n.id for n in ast.walk(node.left) if isinstance(n, ast.Name)
            }
            offending = upstream & sensors
            assert not offending, (
                f"{filename}: {sorted(offending)} feeds the notify task. A "
                "health-sensor failure would send a Telegram message named "
                "after the DAG rather than the service that is down."
            )
