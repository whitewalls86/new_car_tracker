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


class _DeployIntentSensor(BaseSensorOperator):
    def __init__(self, dag_id: str, **kwargs):
        super().__init__(**kwargs)
        self.coordination_dag_id = dag_id
        self.admission_surfaces = tuple(sorted(admission_surfaces(dag_id)))

    def poke(self, context) -> bool:
        hook = PostgresHook(postgres_conn_id="cartracker_db")
        row = hook.get_first(
            """SELECT di.intent, cs.phase,
                      cs.scope ? 'host' OR cs.scope ?| %s::text[] AS intersects
                 FROM deploy_intent di
                 CROSS JOIN coordination_state cs
                WHERE di.id = 1 AND cs.id = 1""",
            parameters=(list(self.admission_surfaces),),
        )
        if row is None or row[0] != "none":
            return False
        return row[1] == "requested" or row[1] == "none" or not row[2]


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
