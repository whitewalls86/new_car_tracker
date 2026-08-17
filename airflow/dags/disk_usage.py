"""Plan 135 Stage 4: publish what is filling each disk.

Runs daily and walks the root disk plus the small Docker volumes, which is
cheap. The MinIO volume is ~4M inodes and a ``du`` over it has run 20+ minutes
without finishing, so it is walked once a week, off-peak, and carried forward
in between. See ``archiver/processors/disk_usage.py`` for why that carry-forward
lives in the .prom file rather than in a second file.
"""

import logging
from typing import Any, Dict

PACK_WORKER_URL = "http://pack-worker:8001"

# Sunday. The walk contends with live scraping, so it wants the quietest day at
# the quietest hour rather than whichever day the DAG happened to be created.
MINIO_WALK_WEEKDAY = 6

# The walk itself has run 20+ minutes; the timeout is deliberately far above
# that so a slow week reports a number instead of a failure.
_LONG_JOB_TIMEOUT_SECONDS = 7_200

logger = logging.getLogger(__name__)


def should_include_minio(logical_date) -> bool:
    """Whether this run walks the expensive MinIO volume."""
    return logical_date.weekday() == MINIO_WALK_WEEKDAY


def check_disk_usage_result(result: Dict[str, Any]) -> None:
    """Fail loudly when a scheduled measurement did not happen.

    A carried-forward value is indistinguishable from a fresh one in the gauge
    itself -- that is the design -- so the run that failed to measure is the
    only place the failure can be caught. Letting it pass silently is how a
    band on the graph quietly freezes at last week's number.
    """
    if result.get("failed"):
        raise RuntimeError(
            f"disk_usage: {result['failed']} watchlist target(s) could not be "
            f"measured: {result.get('unpublished')}"
        )
    if not result.get("measured"):
        raise RuntimeError("disk_usage: nothing was measured; check the host mounts")

    carried = result.get("carried_forward") or 0
    if carried:
        logger.info(
            "disk_usage: %d series carried forward (MinIO walks weekly), %d measured",
            carried, result["measured"],
        )


def _run_disk_usage(**context):
    from sensors import post_json

    include_minio = should_include_minio(context["logical_date"])
    logger.info(
        "disk_usage: walking the watchlist (include_minio=%s)", include_minio
    )
    result = post_json(
        f"{PACK_WORKER_URL}/disk-usage/run",
        payload={"include_minio": include_minio},
        timeout=_LONG_JOB_TIMEOUT_SECONDS,
    )
    check_disk_usage_result(result)
    return result


try:
    import pendulum
    from airflow.providers.standard.operators.python import PythonOperator
    from sensors import deploy_intent_sensor, http_health_sensor

    from airflow import DAG

    with DAG(
        dag_id="disk_usage",
        schedule="0 4 * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["maintenance"],
    ) as dag:
        ready = deploy_intent_sensor()
        pack_worker_up = http_health_sensor("pack_worker", PACK_WORKER_URL)
        measure = PythonOperator(
            task_id="disk_usage",
            python_callable=_run_disk_usage,
        )

        ready >> pack_worker_up >> measure
except ImportError:
    # Keep the predicates importable in the ordinary unit-test venv; the
    # Airflow integration suite imports the real DAG and asserts it exists.
    pass
