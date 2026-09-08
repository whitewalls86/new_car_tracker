"""
Layer 4 — `POST /scrape_results` end to end, with nothing mocked.

The SRP half of G8. The path is the same shape as the detail one and reaches
the origin differently: `scrape_results` composes its URL from `BASE_URL`
rather than taking it from the caller, which is why Stage 8 made that origin
configurable. Everything else is real — `curl_cffi` to the loopback origin, the
production parser over a page pulled from production MinIO, zstd, real MinIO,
real Postgres.

**The job runs in a background thread**, which is the one structural difference
from the detail suite: `POST /scrape_results` returns `queued` immediately and
`_executor.submit`s the work, so these tests have to join the job before they
can assert. They do it through the endpoints the Job Poller uses, not by
reaching into `_jobs` — polling the real contract is the point, and it means a
job that never completes fails on a deadline rather than hanging CI.
"""
import time
import uuid

import pytest

from shared.compression import decompress_frame
from shared.minio import object_exists, read_bytes
from tests.sql_loader import queries

SQL = queries(__file__)

pytestmark = pytest.mark.integration

# Generous enough to absorb a slow runner, short enough that a wedged job fails
# the build instead of sitting until the job-level timeout. The work itself is
# one loopback fetch and one parse: 127.0.0.1 is on scrape_results' unpaced
# list, so none of the 13-35s human-cadence delay applies.
_JOB_DEADLINE_S = 90


def _await_job(client, job_id: str) -> dict:
    """Block until *job_id* is reported completed or failed, or fail loudly.

    Polls ``GET /scrape_results/jobs/completed``, which is exactly what the
    Job Poller does in production; a job only appears there once its thread has
    written a terminal status.
    """
    deadline = time.monotonic() + _JOB_DEADLINE_S
    while time.monotonic() < deadline:
        done = client.get("/scrape_results/jobs/completed").json()
        for job in done:
            if job["job_id"] == job_id:
                return job
        time.sleep(0.25)
    pytest.fail(
        f"job {job_id} never reached a terminal status in {_JOB_DEADLINE_S}s. "
        "It is queued or running, so either the loopback origin did not answer "
        "or the worker thread is wedged."
    )


@pytest.fixture()
def srp_job(scraper_client, verify_cur):
    """Run one single-page SRP job and clean up everything it wrote.

    ``max_safety_pages`` caps the page count in production code, so bounding
    the job to page 1 is a real input rather than a test-only branch: the
    recorded page reports a full result set, and without the cap this would
    walk it.
    """
    run_id, search_key = str(uuid.uuid4()), f"test_{uuid.uuid4().hex[:8]}"
    queued = scraper_client.post(
        "/scrape_results",
        params={"run_id": run_id, "search_key": search_key, "scope": "national"},
        json={
            "params": {
                "makes": ["Kia"],
                "models": ["Sportage Plug-In Hybrid"],
                "max_safety_pages": 1,
                "max_listings": 50,
            }
        },
    )
    assert queued.status_code == 200
    job = _await_job(scraper_client, queued.json()["job_id"])

    yield run_id, search_key, job

    verify_cur.execute(
        SQL("delete_staging_artifacts_queue_events"), (run_id,)
    )
    verify_cur.execute(SQL("delete_ops_artifacts_queue"), (run_id,))
    scraper_client.post(f"/scrape_results/jobs/{job['job_id']}/fetched")


class TestResultsFetch:

    def test_the_job_completes_and_reports_its_artifact(self, srp_job):
        """A queued job reaches 'completed' with the page it fetched.

        `_run_scrape_job` swallows every exception into a 'failed' status and
        an error string, so a path that broke anywhere between the router and
        Postgres would still return 200 from the POST. The job's own terminal
        status is the only thing that distinguishes them.
        """
        _, _, job = srp_job
        assert job["status"] == "completed", job.get("error")
        assert job["artifact_count"] == 1
        assert job["page_1_blocked"] is False

    def test_the_page_reaches_minio_and_still_parses(self, srp_job):
        """The stored artifact is the page, not a challenge or a truncation.

        Asserting it parses rather than just that bytes landed is what makes
        this worth running: `processing` reads this object later and extracts
        VINs from it, so a round trip that silently corrupted the HTML would
        show up there and not here.
        """
        from scraper.processors.results_page_cards import (
            parse_cars_results_page_html_v3,
        )

        _, _, job = srp_job
        artifact = job["artifacts"][0]
        assert artifact["artifact_type"] == "results_page"
        assert artifact["http_status"] == 200
        assert object_exists(artifact["minio_path"])

        html = decompress_frame(read_bytes(artifact["minio_path"]))
        listings, _ = parse_cars_results_page_html_v3(html)
        assert len(listings) == 24

    def test_the_queue_row_and_its_staging_event_are_written_together(
        self, srp_job, verify_cur
    ):
        """The hot/staging pair, on the SRP side.

        Same invariant the detail path carries and a separate statement pair
        (`ENQUEUE_RESULTS_ARTIFACT` / `INSERT_RESULTS_ARTIFACT_EVENT`), so it
        needs its own assertion rather than inheriting the detail suite's.
        """
        run_id, search_key, _ = srp_job

        verify_cur.execute(
            SQL("select_artifact_id_artifact_type_status_from_ops_artifacts_queue"),
            (run_id,),
        )
        rows = verify_cur.fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["artifact_type"] == "results_page"
        assert row["status"] == "pending"

        verify_cur.execute(
            SQL("select_status_artifact_type_from_staging_artifacts_queue_events"),
            (row["artifact_id"],),
        )
        event = verify_cur.fetchone()
        assert event is not None, (
            "the queue row was written without its staging event: the pair is "
            "one transaction and the archiver flushes the half that is missing"
        )
        assert event["artifact_type"] == "results_page"
        assert event["minio_path"] == row["minio_path"]

    def test_the_search_key_is_carried_onto_the_queue_row(self, srp_job, verify_cur):
        """`search_key` is how a scrape_jobs row maps to its artifacts.

        `ENQUEUE_RESULTS_ARTIFACT` takes it as its fourth parameter, and the
        Layer 2 test proves the statement accepts one. What only this layer can
        show is that the value reaching it is the one the *request* carried,
        through the router, the background thread and the per-page save.
        """
        _, search_key, _ = srp_job
        verify_cur.execute(
            SQL("select_search_key_from_ops_artifacts_queue"),
            (search_key,),
        )
        assert verify_cur.fetchone() is not None


class TestJobLifecycle:

    def test_a_fetched_job_is_removed_from_the_poller_view(
        self, scraper_client, verify_cur
    ):
        """The Job Poller's contract: read completed, mark fetched, it is gone.

        A job that survived being marked fetched would be re-processed on the
        next poll, which is the duplicate-artifact shape the queue's own
        dedup exists downstream to absorb rather than something to rely on.
        """
        run_id = str(uuid.uuid4())
        queued = scraper_client.post(
            "/scrape_results",
            params={
                "run_id": run_id,
                "search_key": f"test_{uuid.uuid4().hex[:8]}",
                "scope": "national",
            },
            json={
                "params": {
                    "makes": ["Kia"],
                    "models": ["Sportage Plug-In Hybrid"],
                    "max_safety_pages": 1,
                    "max_listings": 50,
                }
            },
        )
        job_id = queued.json()["job_id"]
        _await_job(scraper_client, job_id)

        marked = scraper_client.post(f"/scrape_results/jobs/{job_id}/fetched")
        assert marked.status_code == 200

        remaining = scraper_client.get("/scrape_results/jobs/completed").json()
        assert job_id not in {job["job_id"] for job in remaining}

        # Second mark is a 404: the job is gone, not merely flagged.
        assert scraper_client.post(
            f"/scrape_results/jobs/{job_id}/fetched"
        ).status_code == 404

        verify_cur.execute(
            SQL("delete_staging_artifacts_queue_events"), (run_id,)
        )
        verify_cur.execute(
            SQL("delete_ops_artifacts_queue"), (run_id,)
        )
