"""
Scrape Listings DAG — Plan 71 step 8.

Replaces the n8n "Scrape Listings" workflow.

Flow:
  deploy_intent_sensor
    → check_scraper_health
    → advance_rotation          (XCom: {slot, run_id, configs})
    → run_scrapes               (submit + poll each config×scope)

advance_rotation returns configs=[] when nothing is due — run_scrapes
detects this and exits cleanly (no error, no scrape).

Schedule: every 30 minutes. advance_rotation's own guards (min_idle=23h59m,
min_gap=3h50m) ensure at most one real scrape fires per ~4-hour window.
"""
import logging
import time

import requests

OPS_URL = "http://ops:8060"
SCRAPER_URL = "http://scraper:8000"

POLL_INTERVAL_S = 30
SCRAPE_TIMEOUT_S = 7200  # 2 hours — SRP can be slow

logger = logging.getLogger(__name__)


def _advance_rotation():
    resp = requests.post(f"{OPS_URL}/scrape/rotation/advance", timeout=30)
    resp.raise_for_status()
    result = resp.json()
    logger.info(
        "advance_rotation: slot=%s configs=%d run_id=%s reason=%s",
        result.get("slot"),
        len(result.get("configs", [])),
        result.get("run_id"),
        result.get("reason"),
    )
    return result


def _run_scrapes(**context):
    rotation = context["ti"].xcom_pull(task_ids="advance_rotation") or {}
    configs = rotation.get("configs", [])
    run_id = rotation.get("run_id")

    if not configs:
        logger.info("No configs due — skipping scrape (reason=%s)", rotation.get("reason"))
        return {"skipped": True, "reason": rotation.get("reason", "nothing_due")}

    # Submit one job per (config, scope)
    job_ids = []
    for config in configs:
        for scope in config.get("scopes", ["local", "national"]):
            resp = requests.post(
                f"{SCRAPER_URL}/scrape_results",
                params={
                    "run_id": run_id,
                    "search_key": config["search_key"],
                    "scope": scope,
                },
                json={"params": config["params"]},
                timeout=30,
            )
            resp.raise_for_status()
            job_id = resp.json()["job_id"]
            job_ids.append(job_id)
            logger.info(
                "submitted scrape job: search_key=%s scope=%s job_id=%s",
                config["search_key"], scope, job_id,
            )

    logger.info("submitted %d scrape jobs for run_id=%s", len(job_ids), run_id)

    # Poll until all jobs complete
    remaining = set(job_ids)
    results = []
    deadline = time.monotonic() + SCRAPE_TIMEOUT_S

    while remaining:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Scrape jobs timed out after {SCRAPE_TIMEOUT_S}s — "
                f"unfinished job_ids: {remaining}"
            )
        time.sleep(POLL_INTERVAL_S)

        resp = requests.get(f"{SCRAPER_URL}/scrape_results/jobs/completed", timeout=15)
        resp.raise_for_status()
        for job in resp.json():
            jid = job.get("job_id")
            if jid in remaining:
                remaining.discard(jid)
                results.append(job)
                logger.info(
                    "job done: job_id=%s status=%s artifacts=%d page_1_blocked=%s",
                    jid, job.get("status"), job.get("artifact_count", 0),
                    job.get("page_1_blocked"),
                )
                # Remove from scraper memory
                try:
                    requests.post(
                        f"{SCRAPER_URL}/scrape_results/jobs/{jid}/fetched",
                        timeout=10,
                    )
                except requests.RequestException as e:
                    logger.warning("failed to mark job %s fetched: %s", jid, e)

    total_artifacts = sum(r.get("artifact_count", 0) for r in results)
    blocked = sum(1 for r in results if r.get("page_1_blocked"))
    logger.info(
        "all scrapes done: run_id=%s jobs=%d total_artifacts=%d blocked=%d",
        run_id, len(job_ids), total_artifacts, blocked,
    )
    return {
        "run_id": run_id,
        "job_count": len(job_ids),
        "total_artifacts": total_artifacts,
        "blocked_count": blocked,
    }


try:
    from datetime import datetime

    from airflow.providers.standard.operators.python import PythonOperator
    from sensors import deploy_intent_sensor, http_health_sensor

    from airflow import DAG

    with DAG(
        dag_id="scrape_listings",
        schedule="*/30 * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["scrape", "plan71"],
    ):
        ready = deploy_intent_sensor()
        scraper_up = http_health_sensor("scraper", SCRAPER_URL)

        advance = PythonOperator(
            task_id="advance_rotation",
            python_callable=_advance_rotation,
        )

        scrape = PythonOperator(
            task_id="run_scrapes",
            python_callable=_run_scrapes,
        )

        ready >> scraper_up >> advance >> scrape

except ImportError:
    pass
