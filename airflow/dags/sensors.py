"""
Shared sensors for cartracker DAGs.

Two primitives:

  deploy_intent_sensor(dag_id)
      Blocks while either the legacy deploy flag is set or scoped coordination
      intersects the DAG's checked-in admission surfaces. Database uncertainty
      fails closed and reschedules rather than manufacturing a failed DAG.

  http_health_sensor(service_name, health_url)
      Blocks until the given /health endpoint returns HTTP 200. Use one per
      HTTP service the DAG depends on. Chain after deploy_intent_sensor.

      It is a **gate, not a notifier** (Plan 140 Stage 4). On timeout it skips
      rather than fails, so a down service no longer pages as "DAG X failed".

Usage in a DAG:

    from sensors import deploy_intent_sensor, http_health_sensor

    with DAG(...):
        intent   = deploy_intent_sensor("example_dag")
        archiver = http_health_sensor("archiver", "http://archiver:8001")
        work     = SomeOperator(...)

        intent >> archiver >> work
"""
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.sensor import BaseSensorOperator
from coordination_contract import admission_surfaces

logger = logging.getLogger(__name__)


class JsonPostError(requests.HTTPError):
    """HTTPError that preserves the parsed response body for downstream alerts."""

    def __init__(self, message: str, *, result: Dict[str, Any]):
        super().__init__(message)
        self.result = result


# Loaded from airflow/sql/ the way delete_stale_emails.py does, by path rather
# than through shared.query_loader: nothing under airflow/dags may import
# shared (G12), so the DAG tree reads its own .sql files directly.
GATE_OBSERVATION_SQL = (
    Path(__file__).parent.parent / "sql" / "record_gate_observation.sql"
).read_text(encoding="utf-8")


def _record_observation(hook, generation: int, dag_id: str, context) -> None:
    """
    Record that this DAG run has seen the coordination gate and is holding.

    `ops.coordination_drain.gate_observation_query` counts active runs of
    affected DAGs with no row here for the current generation, and refuses to
    authorize while that count is non-zero. The key must therefore be exactly
    the (generation, dag_id, run_id) that query correlates on.

    Module-level, and its SQL a module-level constant, so
    tests/integration/sql can execute the real statement -- the same reason
    `coordination_drain`'s query builders are module-level.

    A run with no discoverable run_id has no key to write. It still blocks;
    it just leaves no trace, which is the pre-existing behaviour.
    """
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
    if not run_id:
        return
    hook.run(
        GATE_OBSERVATION_SQL,
        parameters=(generation, dag_id, run_id),
    )


class _DeployIntentSensor(BaseSensorOperator):
    def __init__(self, dag_id: str, **kwargs):
        super().__init__(**kwargs)
        self.coordination_dag_id = dag_id
        self.admission_surfaces = tuple(sorted(admission_surfaces(dag_id)))

    def poke(self, context) -> bool:
        hook = PostgresHook(postgres_conn_id="cartracker_db")
        row = hook.get_first(
            """SELECT di.intent, cs.phase,
                      cs.scope ? 'host' OR cs.scope ?| %s::text[] AS intersects,
                      cs.generation
                 FROM deploy_intent di
                 CROSS JOIN coordination_state cs
                WHERE di.id = 1 AND cs.id = 1""",
            parameters=(list(self.admission_surfaces),),
        )
        if row is None:
            return False

        # Request fixes the immutable scope and is the admission boundary. Do
        # not admit another run merely because the operator has not yet asked
        # for the first drain read.
        blocked_by_coordination = (
            row[1] in {"requested", "draining", "active", "validating"} and row[2]
        )
        # Above both returns, not below them. The observation records that this
        # run has seen the gate and is holding, which is equally true whether
        # the hold comes from deploy intent or from coordination phase -- and
        # the drain waits on exactly this row. See Plan 158.
        if blocked_by_coordination:
            _record_observation(hook, row[3], self.coordination_dag_id, context)

        if row[0] != "none" or blocked_by_coordination:
            return False
        return True


class _ServiceHealthSensor(BaseSensorOperator):
    def __init__(self, service_name: str, health_url: str, **kwargs):
        super().__init__(**kwargs)
        self.service_name = service_name
        self.health_url = health_url

    def poke(self, context) -> bool:
        try:
            resp = requests.get(self.health_url, timeout=5)
            return resp.ok
        except requests.RequestException:
            return False


def deploy_intent_sensor(dag_id: str, **kwargs) -> _DeployIntentSensor:
    """
    Poll both coordination contracts every 60 seconds without an operational
    timeout. Use as the first task in every mutating DAG.

    ``timedelta.max`` is Airflow 3.2's supported practical no-timeout value;
    BaseSensorOperator does not accept ``None``. ``silent_fail`` turns a failed
    database read into another false poke, preserving fail-closed admission
    without turning a planned Postgres outage into a failed DAG.
    """
    return _DeployIntentSensor(
        dag_id=dag_id,
        task_id="check_deploy_intent",
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta.max,
        silent_fail=True,
        **kwargs,
    )


def http_health_sensor(service_name: str, health_url: str, **kwargs) -> _ServiceHealthSensor:
    """
    Polls {health_url}/health every 15s for up to 5 minutes.

    A gate, never a notifier — Plan 140 Stage 4.

    `soft_fail=True` is the whole of that demotion. Until 2026-08-25 a timeout
    here failed the task, failed the DAG run, and fired `ct-pipeline-failures`
    as "DAG {dag_id} failed" — which is the defect Plan 140 opens with. The
    2026-08-18 page said `DAG scrape_listings failed`; the actual fault was
    Airflow apiserver connection exhaustion. A health signal that arrives named
    after a downstream consumer sends triage to the wrong component, late.

    It skips instead, so downstream `all_success` tasks skip and the run ends
    successfully having done nothing. **The gate is unchanged** — no work runs
    against a service that is not answering, and these sensors stay
    load-bearing for DAG correctness. What is gone is only the notification.

    What notifies now is `ct-container-unhealthy` on
    `cartracker_container_health`, which reads 0 within one 15s scrape and goes
    Pending inside a minute — far ahead of any DAG run. That the alert covers a
    *stopped* container and not merely an unhealthy one is Stage 4a's
    expected-service set; before it, `archiver` and `pack-worker` had no other
    notifier and this change would have replaced a mis-named page with silence.

    Airflow 3.2.0 honours `soft_fail` on timeout by raising AirflowSkipException
    (task-sdk `bases/sensor.py`, the `execute` timeout branch). Issue #61130 —
    deferrable sensors ignoring `soft_fail` — does not apply: these are
    `mode="reschedule"`, and switching them to `deferrable=True` would silently
    restore the failure this exists to remove.

    Args:
        service_name: Used as the task_id suffix — must be unique within the DAG.
        health_url:   Base URL of the service, e.g. "http://archiver:8001".
    """
    return _ServiceHealthSensor(
        task_id=f"check_{service_name}_health",
        service_name=service_name,
        health_url=f"{health_url}/health",
        mode="reschedule",
        poke_interval=15,
        timeout=600,
        soft_fail=True,
        **kwargs,
    )


def post_json(
    url: str,
    *,
    timeout: int,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    POST JSON to an internal service and return a normalized response body.

    Active-job 409 responses are treated as a graceful skip so manual DAG
    triggers do not fail just because an hourly run already owns the work.
    Other HTTP errors raise JsonPostError with the parsed body attached so
    notification tasks can include useful stderr/stdout details.
    """
    resp = requests.post(url, json=payload, timeout=timeout)

    if resp.status_code == 409:
        logger.info("job already running (409) - skipping: %s", resp.text)
        return {"ok": True, "skipped": True}

    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "stdout": "", "stderr": resp.text}

    result = body.get("detail", body) if isinstance(body.get("detail"), dict) else body
    if not resp.ok:
        raise JsonPostError(
            f"{resp.status_code} Error for url: {url}",
            result=result,
        )
    return result
