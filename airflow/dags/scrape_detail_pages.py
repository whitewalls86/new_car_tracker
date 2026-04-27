"""
Scrape Detail Pages DAG — Plan 71 step 9.

Replaces the n8n "Scrape Detail Pages V2" workflow.

Flow:
  deploy_intent_sensor
    → check_scraper_health
    → claim_batch               (XCom: {run_id, listings})
    → scrape_detail             (submit batch job, poll until done)
    → release_claims            (mark run complete in ops)

claim_batch returns listings=[] when the queue is empty — scrape_detail
detects this and skips cleanly.

Schedule: every 15 minutes. claim_batch itself marks the run skipped when
the queue is empty, so frequent polling is cheap.
"""
import logging
import time

import requests

OPS_URL = "http://ops:8060"
SCRAPER_URL = "http://scraper:8000"

BATCH_SIZE = 450
POLL_INTERVAL_S = 60
DETAIL_TIMEOUT_S = 10800  # 3 hours — large batches can take a while

logger = logging.getLogger(__name__)


def _claim_batch():
    resp = requests.post(
        f"{OPS_URL}/scrape/claims/claim-batch",
        params={"batch_size": BATCH_SIZE},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    logger.info(
        "claim_batch: run_id=%s listings=%d",
        result.get("run_id"),
        len(result.get("listings", [])),
    )
    return result


def _scrape_detail(**context):
    claim = context["ti"].xcom_pull(task_ids="claim_batch")
    run_id = claim["run_id"]
    listings = claim.get("listings", [])

    if not listings:
        logger.info("No listings claimed — queue empty, skipping scrape (run_id=%s)", run_id)
        return {"run_id": run_id, "skipped": True}

    # Submit the batch
    resp = requests.post(
        f"{SCRAPER_URL}/scrape_detail/batch",
        params={"run_id": run_id},
        json={"listings": listings},
        timeout=60,
    )
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    logger.info(
        "submitted detail batch: run_id=%s job_id=%s listing_count=%d",
        run_id, job_id, len(listings),
    )

    # Poll until the job finishes
    deadline = time.monotonic() + DETAIL_TIMEOUT_S
    while True:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Detail scrape job timed out after {DETAIL_TIMEOUT_S}s "
                f"(run_id={run_id}, job_id={job_id})"
            )
        time.sleep(POLL_INTERVAL_S)

        resp = requests.get(f"{SCRAPER_URL}/scrape_results/jobs/completed", timeout=15)
        resp.raise_for_status()
        for job in resp.json():
            if job.get("job_id") == job_id:
                logger.info(
                    "detail batch done: job_id=%s status=%s artifacts=%d",
                    job_id, job.get("status"), job.get("artifact_count", 0),
                )
                try:
                    requests.post(
                        f"{SCRAPER_URL}/scrape_results/jobs/{job_id}/fetched",
                        timeout=10,
                    )
                except requests.RequestException as e:
                    logger.warning("failed to mark job %s fetched: %s", job_id, e)

                # Push artifact summary for release_claims context
                context["ti"].xcom_push(
                    key="detail_result",
                    value={
                        "job_id": job_id,
                        "status": job.get("status"),
                        "artifact_count": job.get("artifact_count", 0),
                    },
                )
                return {
                    "run_id": run_id,
                    "job_id": job_id,
                    "artifact_count": job.get("artifact_count", 0),
                }


def _release_claims(**context):
    claim = context["ti"].xcom_pull(task_ids="claim_batch")
    run_id = claim["run_id"]
    listings = claim.get("listings", [])

    if not listings:
        logger.info("No listings to release (run_id=%s)", run_id)
        return {"run_id": run_id, "skipped": True}

    # Build release payload — treat all listings as ok (scraper handles per-artifact errors)
    results = [{"listing_id": lst["listing_id"], "status": "ok"} for lst in listings]

    resp = requests.post(
        f"{OPS_URL}/scrape/claims/release",
        json={"run_id": run_id, "results": results},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    logger.info(
        "release_claims: run_id=%s status=%s total=%d errors=%d",
        run_id, result.get("status"), result.get("total"), result.get("errors"),
    )
    return result


try:
    from datetime import datetime

    from airflow.providers.standard.operators.python import PythonOperator
    from sensors import deploy_intent_sensor, http_health_sensor

    from airflow import DAG

    with DAG(
        dag_id="scrape_detail_pages",
        schedule="*/15 * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["scrape", "plan71"],
    ):
        ready = deploy_intent_sensor()
        scraper_up = http_health_sensor("scraper", SCRAPER_URL)

        claim = PythonOperator(
            task_id="claim_batch",
            python_callable=_claim_batch,
        )

        scrape = PythonOperator(
            task_id="scrape_detail",
            python_callable=_scrape_detail,
        )

        release = PythonOperator(
            task_id="release_claims",
            python_callable=_release_claims,
            trigger_rule="all_done",  # release even if scrape fails
        )

        ready >> scraper_up >> claim >> scrape >> release

except ImportError:
    pass
