"""
Shared sensors for cartracker DAGs.

Two primitives:

  deploy_intent_sensor()
      Blocks until deploy_intent.intent = 'none'. Implicitly validates that
      Postgres is reachable — a passing check means the DB is up and no
      deployment is imminent. All DAGs should start with this.

  http_health_sensor(service_name, health_url)
      Blocks until the given /health endpoint returns HTTP 200. Use one per
      HTTP service the DAG depends on. Chain after deploy_intent_sensor.

Usage in a DAG:

    from sensors import deploy_intent_sensor, http_health_sensor

    with DAG(...):
        intent   = deploy_intent_sensor()
        archiver = http_health_sensor("archiver", "http://archiver:8001")
        work     = SomeOperator(...)

        intent >> archiver >> work
"""
import logging
from typing import Any, Dict

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.sensor import BaseSensorOperator

logger = logging.getLogger(__name__)


class JsonPostError(requests.HTTPError):
    """HTTPError that preserves the parsed response body for downstream alerts."""

    def __init__(self, message: str, *, result: Dict[str, Any]):
        super().__init__(message)
        self.result = result


class _DeployIntentSensor(BaseSensorOperator):
    def poke(self, context) -> bool:
        hook = PostgresHook(postgres_conn_id="cartracker_db")
        row = hook.get_first("SELECT intent FROM deploy_intent LIMIT 1")
        return row is not None and row[0] == "none"


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


def deploy_intent_sensor(**kwargs) -> _DeployIntentSensor:
    """
    Polls deploy_intent every 60s for up to 5 minutes.
    Use as the first task in every DAG.
    """
    return _DeployIntentSensor(
        task_id="check_deploy_intent",
        mode="reschedule",
        poke_interval=60,
        timeout=600,
        **kwargs,
    )


def http_health_sensor(service_name: str, health_url: str, **kwargs) -> _ServiceHealthSensor:
    """
    Polls {health_url}/health every 15s for up to 5 minutes.

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
