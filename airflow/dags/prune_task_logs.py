"""Bound local Airflow task logs to 30 days (Plan 135 Stage 5d)."""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

LOG_ROOT = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "logs"
RETENTION_DAYS = 30


def _latest_mtime(path: Path) -> float:
    """Return the newest mtime below a run directory without following links."""
    latest = 0.0
    for root, dirs, files in os.walk(path, followlinks=False):
        dirs[:] = [name for name in dirs if not (Path(root) / name).is_symlink()]
        for name in files:
            candidate = Path(root) / name
            if not candidate.is_symlink():
                latest = max(latest, candidate.stat().st_mtime)
    return latest or path.lstat().st_mtime


def prune_task_logs(
    *,
    log_root: Path = LOG_ROOT,
    retention_days: int = RETENTION_DAYS,
    now: datetime | None = None,
) -> dict[str, int]:
    """Delete only closed ``dag_id=*/run_id=*`` trees older than retention."""
    if retention_days < 1:
        raise ValueError("retention_days must be positive")
    if not log_root.exists():
        return {"examined": 0, "deleted": 0}

    cutoff = (now or datetime.now(timezone.utc)).timestamp() - retention_days * 86400
    examined = deleted = 0
    for dag_dir in log_root.glob("dag_id=*"):
        if not dag_dir.is_dir() or dag_dir.is_symlink():
            continue
        for run_dir in dag_dir.glob("run_id=*"):
            if not run_dir.is_dir() or run_dir.is_symlink():
                continue
            examined += 1
            if _latest_mtime(run_dir) < cutoff:
                shutil.rmtree(run_dir)
                deleted += 1

    logger.info(
        "Airflow task-log retention examined %d run directories and deleted %d older than %d days",
        examined,
        deleted,
        retention_days,
    )
    return {"examined": examined, "deleted": deleted}


def _build_dag():
    from airflow.providers.standard.operators.python import PythonOperator
    from sensors import deploy_intent_sensor

    from airflow import DAG

    with DAG(
        dag_id="prune_task_logs",
        description="Delete Airflow task-log run directories older than 30 days",
        schedule="17 4 * * 0",
        start_date=datetime(2026, 8, 1),
        catchup=False,
        max_active_runs=1,
        tags=["maintenance", "storage"],
    ) as dag:
        ready = deploy_intent_sensor("prune_task_logs")
        prune = PythonOperator(
            task_id="prune_task_logs", python_callable=prune_task_logs
        )
        ready >> prune
    return dag


try:
    dag = _build_dag()
except ImportError:  # Unit tests do not install Airflow.
    dag = None
