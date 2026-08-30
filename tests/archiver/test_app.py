"""Unit tests for archiver/app.py — all HTTP endpoints."""

import pytest


@pytest.fixture(scope="module", autouse=True)
def allow_pack_jobs_in_endpoint_tests():
    """Keep the pre-D4 endpoint tests focused on their original contracts.

    The production archiver defaults to refusing pack/prune after D4. This
    override is intentionally local to this module: putting an app import in
    tests/archiver/conftest.py would run configure_logging() for every processor
    test and create its log directory as a collection-time side effect.
    """
    import archiver.app as archiver_app

    previous = archiver_app._ALLOW_PACK_JOBS
    archiver_app._ALLOW_PACK_JOBS = True
    yield
    archiver_app._ALLOW_PACK_JOBS = previous

# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_ok(self, mock_archiver_client):
        resp = mock_archiver_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


# ---------------------------------------------------------------------------
# POST /cleanup/queue  (Plan 97 — batch delete by caller-supplied IDs)
# ---------------------------------------------------------------------------

class TestCleanupQueueEndpoint:
    def test_empty_artifact_ids_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["deleted"] == 0
        assert data["failed"] == 0

    def test_all_deleted(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": True, "reason": None},
        ])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [1, 2]})
        data = resp.json()
        assert data["total"] == 2
        assert data["deleted"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {
                "artifact_id": 2, 
                "deleted": False, 
                "reason": "not deleted — row missing or status not in (complete, skip)"
            },
        ])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [1, 2]})
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 1

    def test_artifact_ids_forwarded_as_ints(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._cleanup_queue", return_value=[])
        mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [10, 20]})
        called_ids = mock_fn.call_args[0][0]
        assert called_ids == [10, 20]
        assert all(isinstance(i, int) for i in called_ids)

    def test_results_included_in_response(self, mock_archiver_client, mocker):
        fake = [{"artifact_id": 5, "deleted": True, "reason": None}]
        mocker.patch("archiver.app._cleanup_queue", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [5]})
        assert resp.json()["results"] == fake


# ---------------------------------------------------------------------------
# POST /cleanup/queue/run  (Plan 97 — full sweep of complete/skip rows)
# ---------------------------------------------------------------------------

class TestCleanupQueueRunEndpoint:
    def test_delegates_to_run_cleanup_queue(self, mock_archiver_client, mocker):
        fake = {"total": 3, "deleted": 3, "failed": 0, "results": []}
        mocker.patch("archiver.app._run_cleanup_queue", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/queue/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._run_cleanup_queue",
            return_value={"total": 0, "deleted": 0, "failed": 0, "results": []},
        )
        resp = mock_archiver_client.post("/cleanup/queue/run")
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# POST /flush/staging/run
# ---------------------------------------------------------------------------

class TestFlushStagingRunEndpoint:
    def test_delegates_to_flush_staging_events(self, mock_archiver_client, mocker):
        fake = {"total_flushed": 42, "tables": [], "error": None}
        mocker.patch("archiver.app._flush_staging_events", return_value=fake)
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zero_total(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_staging_events",
            return_value={"total_flushed": 0, "tables": [], "error": None},
        )
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.json()["total_flushed"] == 0

    def test_error_propagated_in_response(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_staging_events",
            return_value={"total_flushed": 0, "tables": [], "error": "db down"},
        )
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.status_code == 200
        assert resp.json()["error"] == "db down"


# ---------------------------------------------------------------------------
# POST /flush/silver/run
# ---------------------------------------------------------------------------

class TestFlushSilverRunEndpoint:
    def test_delegates_to_flush_silver_observations(self, mock_archiver_client, mocker):
        fake = {"flushed": 100, "error": None}
        mocker.patch("archiver.app._flush_silver_observations", return_value=fake)
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zero(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_silver_observations",
            return_value={"flushed": 0, "error": None},
        )
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.json()["flushed"] == 0

    def test_error_propagated_in_response(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_silver_observations",
            return_value={"flushed": 0, "error": "minio unreachable"},
        )
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.status_code == 200
        assert resp.json()["error"] == "minio unreachable"


# ---------------------------------------------------------------------------
# POST /compact/silver/run
# ---------------------------------------------------------------------------

class TestCompactSilverRunEndpoint:
    def test_delegates_to_compact_silver(self, mock_archiver_client, mocker):
        fake = {
            "scanned": 10, "compacted": 3, "incremental": 1, "skipped": 5, "failed": 0,
            "size_before_mb": 8.0, "size_after_mb": 2.6, "error": None, "partitions": [],
        }
        mocker.patch("archiver.app._compact_silver", return_value=fake)
        resp = mock_archiver_client.post("/compact/silver/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_ready_returns_503_while_compact_active(self, mock_archiver_client, mocker):
        """GET /ready returns 503 while active_job() counter is non-zero."""
        mocker.patch(
            "archiver.app.job_snapshot",
            return_value={
                "active_jobs": 1,
                "oldest_started_at": "2026-08-25T01:00:00+00:00",
            },
        )
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["detail"]["ready"] is False


# ---------------------------------------------------------------------------
# POST /pack/bronze/run  (Plan 131)
# ---------------------------------------------------------------------------

class TestPackBronzeRunEndpoint:
    def test_defaults_to_a_dry_run(self, mock_archiver_client, mocker):
        fake = {"mode": "dry_run", "packs_written": 0, "error": None, "buckets": []}
        mock_fn = mocker.patch("archiver.app._pack_bronze_html", return_value=fake)
        resp = mock_archiver_client.post("/pack/bronze/run")
        assert resp.status_code == 200
        assert resp.json() == fake
        assert mock_fn.call_args.kwargs == {}, "no payload means processor defaults"

    def test_passes_through_the_documented_options(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch(
            "archiver.app._pack_bronze_html", return_value={"mode": "apply"}
        )
        resp = mock_archiver_client.post(
            "/pack/bronze/run",
            json={"apply": True, "year": 2026, "month": 5, "max_packs": 2},
        )
        assert resp.status_code == 200
        assert mock_fn.call_args.kwargs == {
            "apply": True, "year": 2026, "month": 5, "max_packs": 2,
        }

    def test_unknown_payload_keys_are_ignored(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._pack_bronze_html", return_value={})
        resp = mock_archiver_client.post(
            "/pack/bronze/run", json={"apply": True, "delete_sources": True}
        )
        assert resp.status_code == 200
        assert "delete_sources" not in mock_fn.call_args.kwargs


# ---------------------------------------------------------------------------
# The Plan 131 failure contract (Stage 5 D5)
#
# Both processors return a summary rather than raising, and both translate that
# summary into a failure on the CLI. These are the HTTP half of that contract:
# a run that aborted or refused an object must not return 200, or
# raise_for_status() — the entire check a DAG performs — passes on it.
# ---------------------------------------------------------------------------

class TestPackFailureReason:
    """The predicate itself, against summary dicts, per D5."""

    def test_clean_run_is_not_a_failure(self):
        from archiver.app import _pack_failure_reason

        assert _pack_failure_reason(
            {"error": None, "read_failures": 0, "packs_written": 3}
        ) is None

    def test_error_is_a_failure(self):
        from archiver.app import _pack_failure_reason

        reason = _pack_failure_reason({"error": "disk full", "read_failures": 0})
        assert reason and "disk full" in reason

    def test_read_failures_only_warn(self):
        from archiver.app import _pack_failure_reason

        # The packer's CLI exits 1 on this; the endpoint deliberately does not.
        # Nothing is deleted here and packing is additive, so an unreadable
        # object costs a later run rather than any data — and a monthly DAG
        # must not throw away a packed month over one bad object.
        assert _pack_failure_reason({"error": None, "read_failures": 4}) is None

    def test_stopped_at_max_packs_only_warns(self):
        from archiver.app import _pack_failure_reason

        # The cap doing its job. The next run picks up where this one stopped.
        assert _pack_failure_reason({
            "error": None, "read_failures": 0, "stopped_at_max_packs": True,
        }) is None

    def test_orphan_packs_only_warn(self):
        from archiver.app import _pack_failure_reason

        # An earlier run was interrupted. The packer reports orphans and never
        # writes into them, so carrying the condition is safe.
        assert _pack_failure_reason({
            "error": None, "read_failures": 0, "orphan_packs": ["pack-00007"],
        }) is None


class TestPruneFailureReason:
    def test_clean_run_is_not_a_failure(self):
        from archiver.app import _prune_failure_reason

        assert _prune_failure_reason(
            {"error": None, "objects_refused": 0, "objects_deleted": 100}
        ) is None

    def test_error_is_a_failure(self):
        from archiver.app import _prune_failure_reason

        reason = _prune_failure_reason({
            "error": "year and month are required — this job deletes data",
            "objects_refused": 0,
        })
        assert reason and "year and month" in reason

    def test_refused_objects_are_a_failure(self):
        from archiver.app import _prune_failure_reason

        # The loudest signal this job produces: verification disagreed.
        reason = _prune_failure_reason({"error": None, "objects_refused": 40000})
        assert reason and "40000" in reason

    def test_a_drained_month_deleting_nothing_is_not_a_failure(self):
        from archiver.app import _prune_failure_reason

        # A fully pruned month legitimately deletes nothing and returns after
        # one listing. objects_deleted == 0 is not a failure predicate.
        assert _prune_failure_reason({
            "error": None, "objects_refused": 0, "objects_deleted": 0,
            "objects_surviving_before": 0,
        }) is None

    def test_capped_only_warns(self):
        from archiver.app import _prune_failure_reason

        assert _prune_failure_reason({
            "error": None, "objects_refused": 0, "capped": True,
        }) is None


class TestPackEndpointsSignalFailure:
    def test_pack_error_returns_500_carrying_the_summary(
        self, mock_archiver_client, mocker
    ):
        fake = {"mode": "apply", "error": "no free space", "read_failures": 0}
        mocker.patch("archiver.app._pack_bronze_html", return_value=fake)

        resp = mock_archiver_client.post("/pack/bronze/run", json={"apply": True})

        assert resp.status_code == 500
        detail = resp.json()["detail"]
        # The whole summary survives, so JsonPostError can carry it to notify.
        assert detail["error"] == "no free space"
        assert detail["mode"] == "apply"
        assert "no free space" in detail["failure_reason"]

    def test_pack_read_failures_still_return_200(self, mock_archiver_client, mocker):
        fake = {"mode": "apply", "error": None, "read_failures": 2, "packs_written": 31}
        mocker.patch("archiver.app._pack_bronze_html", return_value=fake)

        resp = mock_archiver_client.post("/pack/bronze/run", json={"apply": True})

        # 31 packs written is not a failed run because two objects were
        # unreadable. The count stays in the body for whoever wants it.
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_prune_refusals_return_500_carrying_the_summary(
        self, mock_archiver_client, mocker
    ):
        fake = {
            "mode": "apply", "error": None, "objects_refused": 7,
            "objects_deleted": 93,
            "failures": [{"source_key": "html/...", "error": "sha256 mismatch"}],
        }
        mocker.patch("archiver.app._delete_packed_source_html", return_value=fake)

        resp = mock_archiver_client.post(
            "/pack/bronze/prune", json={"year": 2026, "month": 4, "apply": True}
        )

        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert detail["objects_refused"] == 7
        assert detail["failures"][0]["error"] == "sha256 mismatch"
        assert "7" in detail["failure_reason"]

    def test_prune_error_returns_500(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._delete_packed_source_html",
            return_value={"error": "listing failed", "objects_refused": 0},
        )

        resp = mock_archiver_client.post(
            "/pack/bronze/prune", json={"year": 2026, "month": 4}
        )

        assert resp.status_code == 500

    def test_prune_deleting_nothing_still_returns_200(
        self, mock_archiver_client, mocker
    ):
        # The drained-month case. It must stay green, or the Stage 5 DAG fails
        # every month after the first one it packs.
        fake = {
            "mode": "apply", "error": None, "objects_refused": 0,
            "objects_deleted": 0, "objects_surviving_before": 0, "capped": False,
        }
        mocker.patch("archiver.app._delete_packed_source_html", return_value=fake)

        resp = mock_archiver_client.post(
            "/pack/bronze/prune", json={"year": 2026, "month": 4, "apply": True}
        )

        assert resp.status_code == 200
        assert resp.json() == fake

    def test_a_clean_pack_run_is_unchanged(self, mock_archiver_client, mocker):
        fake = {
            "mode": "apply", "error": None, "read_failures": 0,
            "packs_written": 32, "orphan_packs": [], "stopped_at_max_packs": False,
        }
        mocker.patch("archiver.app._pack_bronze_html", return_value=fake)

        resp = mock_archiver_client.post("/pack/bronze/run", json={"apply": True})

        assert resp.status_code == 200
        assert resp.json() == fake
        assert "failure_reason" not in resp.json()


# ---------------------------------------------------------------------------
# Single-flight (Plan 131 Stage 5 D3a)
#
# There was no lock on either endpoint. That was fine while every run was a
# human typing a command, and stops being fine the moment a DAG retries: a
# multi-hour call that dies on a dropped connection leaves the job running, and
# the retry would start a second packer on the same bucket.
# ---------------------------------------------------------------------------

class TestPackSingleFlight:
    def test_a_pack_run_is_refused_while_one_is_in_flight(
        self, mock_archiver_client, mocker
    ):
        from shared.job_counter import single_flight

        mock_fn = mocker.patch("archiver.app._pack_bronze_html", return_value={})

        with single_flight("pack_bronze"):
            resp = mock_archiver_client.post("/pack/bronze/run")

        assert resp.status_code == 409
        assert "pack_bronze" in resp.json()["detail"]
        # It must refuse *before* the processor, not after — the whole point is
        # that a second packer never lists the bucket.
        mock_fn.assert_not_called()

    def test_a_prune_run_is_refused_while_one_is_in_flight(
        self, mock_archiver_client, mocker
    ):
        from shared.job_counter import single_flight

        mock_fn = mocker.patch("archiver.app._delete_packed_source_html", return_value={})

        with single_flight("pack_prune"):
            resp = mock_archiver_client.post(
                "/pack/bronze/prune", json={"year": 2026, "month": 4}
            )

        assert resp.status_code == 409
        mock_fn.assert_not_called()

    def test_a_pack_in_flight_does_not_block_a_prune(
        self, mock_archiver_client, mocker
    ):
        from shared.job_counter import single_flight

        mocker.patch(
            "archiver.app._delete_packed_source_html",
            return_value={"error": None, "objects_refused": 0},
        )

        # Keyed per job. A pack and a prune on different months are a normal
        # thing to run at once, and a global lock would make working the
        # backlog by hand impossible.
        with single_flight("pack_bronze"):
            resp = mock_archiver_client.post(
                "/pack/bronze/prune", json={"year": 2026, "month": 4}
            )

        assert resp.status_code == 200

    def test_the_slot_is_released_after_a_run(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._pack_bronze_html",
            return_value={"error": None, "read_failures": 0},
        )

        assert mock_archiver_client.post("/pack/bronze/run").status_code == 200
        assert mock_archiver_client.post("/pack/bronze/run").status_code == 200

    def test_the_slot_is_released_after_a_failed_run(
        self, mock_archiver_client, mocker
    ):
        # A 500 must not wedge the job out of existence until the container
        # restarts.
        mocker.patch(
            "archiver.app._pack_bronze_html",
            return_value={"error": "boom", "read_failures": 0},
        )
        assert mock_archiver_client.post("/pack/bronze/run").status_code == 500

        mocker.patch(
            "archiver.app._pack_bronze_html",
            return_value={"error": None, "read_failures": 0},
        )
        assert mock_archiver_client.post("/pack/bronze/run").status_code == 200


# ---------------------------------------------------------------------------
# POST /pack/bronze/verify (Plan 131 Stage 5 Step 7b)
# ---------------------------------------------------------------------------

class TestVerifyEndpoint:
    def test_clean_run_returns_200(self, mock_archiver_client, mocker):
        fake = {"verified": 5, "failed": 0, "sampled": 5}
        mocker.patch("archiver.app._verify_pack_read_path", return_value=fake)

        resp = mock_archiver_client.post(
            "/pack/bronze/verify", json={"year": 2026, "month": 4}
        )

        assert resp.status_code == 200
        assert resp.json() == fake

    def test_forwards_only_allow_listed_keys(self, mock_archiver_client, mocker):
        import archiver.app as archiver_app

        mock_fn = mocker.patch(
            "archiver.app._verify_pack_read_path",
            return_value={"verified": 1, "failed": 0},
        )

        resp = mock_archiver_client.post(
            "/pack/bronze/verify",
            json={
                "artifact_type": "detail_page",
                "year": 2026,
                "month": 6,
                "per_pack": 7,
                "warm_reads": 9,
                "seed": 42,
                "bucket": "attacker-controlled",
                "apply": True,
            },
        )

        assert resp.status_code == 200
        assert mock_fn.call_args.kwargs == {
            "artifact_type": "detail_page",
            "year": 2026,
            "month": 6,
            "per_pack": 7,
            "warm_reads": 9,
            "seed": 42,
            "bucket": archiver_app._MINIO_BUCKET,
        }

    @pytest.mark.parametrize("payload", [{}, {"year": 2026}, {"month": 4}])
    def test_year_and_month_are_required(self, mock_archiver_client, payload):
        resp = mock_archiver_client.post("/pack/bronze/verify", json=payload)

        assert resp.status_code == 400
        assert "year and month" in resp.json()["detail"]

    def test_failed_members_return_500_with_the_summary(
        self, mock_archiver_client, mocker
    ):
        fake = {
            "verified": 4,
            "failed": 1,
            "failures": [{"source_key": "html/bad", "error": "hash mismatch"}],
        }
        mocker.patch("archiver.app._verify_pack_read_path", return_value=fake)

        resp = mock_archiver_client.post(
            "/pack/bronze/verify", json={"year": 2026, "month": 4}
        )

        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert detail["failed"] == 1
        assert detail["failures"] == fake["failures"]
        assert "1" in detail["failure_reason"]

    def test_nothing_verified_returns_500(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._verify_pack_read_path",
            return_value={"verified": 0, "failed": 0, "sidecars": 0},
        )

        resp = mock_archiver_client.post(
            "/pack/bronze/verify", json={"year": 2026, "month": 4}
        )

        assert resp.status_code == 500
        assert "no sampled members" in resp.json()["detail"]["failure_reason"]

    def test_verify_is_available_while_both_mutation_slots_are_held(
        self, mock_archiver_client, mocker
    ):
        from shared.job_counter import single_flight

        mocker.patch(
            "archiver.app._verify_pack_read_path",
            return_value={"verified": 1, "failed": 0},
        )

        with single_flight("pack_bronze"), single_flight("pack_prune"):
            resp = mock_archiver_client.post(
                "/pack/bronze/verify", json={"year": 2026, "month": 4}
            )

        assert resp.status_code == 200


class TestPackJobsGuard:
    @pytest.mark.parametrize(
        ("path", "processor", "payload", "slot"),
        [
            ("/pack/bronze/run", "_pack_bronze_html", {}, "pack_bronze"),
            (
                "/pack/bronze/prune",
                "_delete_packed_source_html",
                {"year": 2026, "month": 4},
                "pack_prune",
            ),
        ],
    )
    def test_pack_and_prune_are_refused_before_work_or_a_slot(
        self, mock_archiver_client, mocker, path, processor, payload, slot
    ):
        import archiver.app as archiver_app
        from shared.job_counter import is_idle, single_flight

        mocker.patch.object(archiver_app, "_ALLOW_PACK_JOBS", False)
        processor_mock = mocker.patch.object(archiver_app, processor)

        resp = mock_archiver_client.post(path, json=payload)

        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert "pack-worker" in detail
        assert "http://pack-worker:8001" in detail
        assert "starves flush/cleanup/compact" in detail
        assert "ARCHIVER_ALLOW_PACK_JOBS" in detail
        processor_mock.assert_not_called()
        assert is_idle(), "the guard must run before active_job()"
        with single_flight(slot):
            pass  # The refused request must not consume the mutation slot.

    def test_pack_and_prune_run_when_explicitly_allowed(
        self, mock_archiver_client, mocker
    ):
        import archiver.app as archiver_app

        mocker.patch.object(archiver_app, "_ALLOW_PACK_JOBS", True)
        mocker.patch.object(
            archiver_app,
            "_pack_bronze_html",
            return_value={"error": None, "read_failures": 0},
        )
        mocker.patch.object(
            archiver_app,
            "_delete_packed_source_html",
            return_value={"error": None, "objects_refused": 0},
        )

        assert mock_archiver_client.post("/pack/bronze/run").status_code == 200
        assert mock_archiver_client.post(
            "/pack/bronze/prune", json={"year": 2026, "month": 4}
        ).status_code == 200

    def test_verify_is_not_guarded(self, mock_archiver_client, mocker):
        import archiver.app as archiver_app

        mocker.patch.object(archiver_app, "_ALLOW_PACK_JOBS", False)
        mocker.patch.object(
            archiver_app,
            "_verify_pack_read_path",
            return_value={"verified": 1, "failed": 0},
        )

        resp = mock_archiver_client.post(
            "/pack/bronze/verify", json={"year": 2026, "month": 4}
        )

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /snapshots/adaptive-refresh/run  (Plan 120)
# ---------------------------------------------------------------------------

class TestSnapshotExportRunEndpoint:
    def test_dry_run_calls_processor_and_returns_json(self, mock_archiver_client, mocker):
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "planned",
            "source_window_start": None,
            "source_window_end": None,
            "seed_vin_count": None,
            "closed_vin_count": None,
            "listing_count": None,
            "artifact_count": None,
            "archive_bytes": None,
            "manifest_key": None,
            "archive_key": None,
            "coverage_failures": [],
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        assert resp.status_code == 200
        assert resp.json() == fake_result
        assert mock_fn.called

    def test_missing_body_defaults_to_empty_payload(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        # dry_run=True: this test is about payload defaulting, not the
        # non-dry-run sync-cohort guard.
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"dry_run": True}
        )
        assert resp.status_code == 200
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.tier is None

    def test_invalid_tier_returns_400(self, mock_archiver_client):
        # dry_run=True: keeps this test scoped to tier validation rather than
        # the non-dry-run sync-cohort guard.
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "bogus", "dry_run": True}
        )
        assert resp.status_code == 400

    def test_non_dry_run_rejected_by_default_sync_cohort_guard(self, mock_archiver_client):
        """Gate D: a real (non-dry-run) export always runs the same heavy
        planning as build_cohort=True, so it must be blocked by the same
        production sync-cohort guard by default."""
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": False}
        )
        assert resp.status_code == 409

    def test_non_dry_run_allowed_when_override_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SYNC_SNAPSHOT_COHORT", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "exported"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": False}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_tier_defaults_flow_through_to_dry_run_result(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "dry_run": True}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["tier"] == "edge"
        assert data["status"] == "planned"

    def test_malformed_source_window_start_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "source_window_start": "not-a-date", "dry_run": True},
        )
        assert resp.status_code == 400

    def test_non_numeric_limit_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "target_vins": "five-thousand", "dry_run": True},
        )
        assert resp.status_code == 400

    def test_audit_sources_forwarded_to_request_and_response(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "audited",
            "source_audit": {"tables": {}, "window": {"start": None, "end": None},
                              "errors": [], "ok": True},
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "audit_sources": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["source_audit"]["ok"] is True
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.audit_sources is True
        assert request_arg.source_base_path == "/tmp/lake"

    def test_audit_sources_defaults_to_false(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.audit_sources is False
        assert request_arg.source_base_path is None

    def test_run_selectors_defaults_to_false(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.run_selectors is False

    def test_run_selectors_forwarded_to_request_and_response(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "planned",
            "selector_diagnostics": {"selectors": {}, "errors": [], "ok": True},
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "run_selectors": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["selector_diagnostics"]["ok"] is True
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.run_selectors is True

    def test_source_base_path_rejected_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "run_selectors": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 400
        assert not mock_fn.called

    def test_source_base_path_allowed_when_flag_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "dry_run": True, "source_base_path": "/tmp/lake"},
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_malformed_payload_with_audit_sources_still_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "bogus", "audit_sources": True},
        )
        assert resp.status_code == 400

    # -----------------------------------------------------------------------
    # build_cohort sync safeguard (Plan 120 Gate C.5)
    # -----------------------------------------------------------------------

    def test_build_cohort_rejected_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True,
            },
        )
        assert resp.status_code == 409
        assert not mock_fn.called
        assert "snapshot-worker" in resp.json()["detail"]

    def test_build_cohort_allowed_when_override_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SYNC_SNAPSHOT_COHORT", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True,
            },
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_dry_run_without_build_cohort_still_allowed_by_default(
        self, mock_archiver_client, mocker
    ):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "dry_run": True}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_audit_sources_still_allowed_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "audited"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "audit_sources": True}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_build_cohort_false_not_blocked_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "edge", "dry_run": True, "build_cohort": False},
        )
        assert resp.status_code == 200
        assert mock_fn.called

    # -----------------------------------------------------------------------
    # Planning cache fields (Plan 120 Gate C.75)
    # -----------------------------------------------------------------------

    def test_planning_cache_fields_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.reuse_planning_cache is False
        assert request_arg.refresh_planning_cache is False
        assert request_arg.planning_cache_bucket_grain == "week"
        assert request_arg.planning_cache_prefix == "snapshot_planning_cache"

    def test_planning_cache_fields_forwarded(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True,
                "reuse_planning_cache": True,
                "planning_cache_bucket_grain": "day",
                "planning_cache_prefix": "custom_prefix",
            },
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.reuse_planning_cache is True
        assert request_arg.refresh_planning_cache is False
        assert request_arg.planning_cache_bucket_grain == "day"
        assert request_arg.planning_cache_prefix == "custom_prefix"

    def test_build_cohort_guard_still_intact_with_planning_cache_fields(
        self, mock_archiver_client, mocker
    ):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True, "reuse_planning_cache": True,
            },
        )
        assert resp.status_code == 409
        assert not mock_fn.called


# ---------------------------------------------------------------------------
# GET /ready
# ---------------------------------------------------------------------------

class TestReady:
    def test_ready_true_when_idle(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app.job_snapshot",
            return_value={"active_jobs": 0, "oldest_started_at": None},
        )
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 200
        assert resp.json() == {
            "ready": True,
            "active_jobs": 0,
            "oldest_started_at": None,
        }

    def test_ready_false_when_busy(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app.job_snapshot",
            return_value={
                "active_jobs": 3,
                "oldest_started_at": "2026-08-25T01:00:00+00:00",
            },
        )
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["detail"]["ready"] is False
        assert resp.json()["detail"]["active_jobs"] == 3
        assert "reason" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /disk-usage/run  (Plan 135 Stage 4)
# ---------------------------------------------------------------------------

class TestDiskUsageEndpoint:
    def test_refused_without_the_host_mounts(self, mock_archiver_client, mocker):
        """The regular archiver cannot see the disks. Without this guard it
        would measure nothing, write an empty .prom, and return 200 -- which
        reads as "the disks are empty" rather than as a misconfiguration."""
        import archiver.app as archiver_app
        from shared.job_counter import is_idle

        mocker.patch.object(archiver_app, "_disk_usage_textfile_dir", return_value=None)
        processor = mocker.patch.object(archiver_app, "_run_disk_usage")

        resp = mock_archiver_client.post("/disk-usage/run", json={})

        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert "pack-worker" in detail
        assert "http://pack-worker:8001" in detail
        assert "DISK_USAGE_TEXTFILE_DIR" in detail
        processor.assert_not_called()
        assert is_idle(), "the guard must run before active_job()"

    def test_daily_run_defaults_to_skipping_minio(self, mock_archiver_client, mocker):
        """The expensive walk must be opt-in: defaulting it on would put a
        20-minute disk thrash on every daily run."""
        import archiver.app as archiver_app

        mocker.patch.object(
            archiver_app, "_disk_usage_textfile_dir", return_value="/textfile"
        )
        processor = mocker.patch.object(
            archiver_app, "_run_disk_usage",
            return_value={"failed": 0, "measured": 10, "unpublished": []},
        )

        resp = mock_archiver_client.post("/disk-usage/run", json={})

        assert resp.status_code == 200
        processor.assert_called_once_with(include_slow=False)

    def test_weekly_run_passes_the_flag_through(self, mock_archiver_client, mocker):
        import archiver.app as archiver_app

        mocker.patch.object(
            archiver_app, "_disk_usage_textfile_dir", return_value="/textfile"
        )
        processor = mocker.patch.object(
            archiver_app, "_run_disk_usage",
            return_value={"failed": 0, "measured": 11, "unpublished": []},
        )

        resp = mock_archiver_client.post("/disk-usage/run", json={"include_slow": True})

        assert resp.status_code == 200
        processor.assert_called_once_with(include_slow=True)

    def test_failed_measurement_is_a_500_not_a_quiet_200(
        self, mock_archiver_client, mocker
    ):
        """Carried-forward values look identical to fresh ones in the gauge, so
        the run is the only place a failure can surface."""
        import archiver.app as archiver_app

        mocker.patch.object(
            archiver_app, "_disk_usage_textfile_dir", return_value="/textfile"
        )
        mocker.patch.object(
            archiver_app, "_run_disk_usage",
            return_value={"failed": 2, "measured": 8, "unpublished": ["/usr", "/tmp"]},
        )

        resp = mock_archiver_client.post("/disk-usage/run", json={})

        assert resp.status_code == 500
        assert resp.json()["detail"]["unpublished"] == ["/usr", "/tmp"]
