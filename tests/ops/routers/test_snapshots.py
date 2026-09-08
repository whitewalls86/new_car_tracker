"""Unit tests for ops/routers/snapshots.py — Plan 120 Gate F download API."""
import pytest

from ops.routers import snapshots

BASE = "/admin/snapshots/adaptive-refresh"
AUTH = {"Authorization": "Bearer test-token"}


@pytest.fixture(autouse=True)
def _token(mocker):
    """Configure a known token set for every test in this module."""
    mocker.patch.object(
        snapshots, "SNAPSHOT_TOKENS",
        (snapshots.SnapshotToken(name="ci", scope="read", token="test-token"),),
    )


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TestAuth:
    def test_missing_authorization_header_is_401(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest")
        assert resp.status_code == 401

    def test_wrong_token_is_403(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest", headers={"Authorization": "Bearer nope"})
        assert resp.status_code == 403

    def test_malformed_header_is_401(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest", headers={"Authorization": "test-token"})
        assert resp.status_code == 401

    def test_unconfigured_token_is_503(self, mock_client, mocker):
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", ())
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 503


class TestNamedTokens:
    """Plan 162: one entry per caller, so revocation and attribution are per-caller."""

    def test_any_entry_in_the_set_authenticates(self, mock_client, mocker):
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (
            snapshots.SnapshotToken("ci", "read", "ci-token"),
            snapshots.SnapshotToken("mlflow", "read", "mlflow-token"),
        ))
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})
        for token in ("ci-token", "mlflow-token"):
            resp = mock_client.get(
                f"{BASE}/latest", headers={"Authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 200, token

    def test_removing_one_entry_revokes_only_that_caller(self, mock_client, mocker):
        """The property the whole design exists for. A shared string cannot do
        this: revoking CI would take the laptop and the MLflow rehearsal with it."""
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (
            snapshots.SnapshotToken("mlflow", "read", "mlflow-token"),
        ))
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})

        revoked = mock_client.get(
            f"{BASE}/latest", headers={"Authorization": "Bearer ci-token"},
        )
        survivor = mock_client.get(
            f"{BASE}/latest", headers={"Authorization": "Bearer mlflow-token"},
        )
        assert revoked.status_code == 403
        assert survivor.status_code == 200

    def test_the_caller_name_is_logged_and_the_token_is_not(self, mock_client, mocker, caplog):
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})
        with caplog.at_level("INFO", logger="pipeline_ops"):
            mock_client.get(f"{BASE}/latest", headers=AUTH)
        logged = caplog.text
        assert "caller=ci" in logged
        assert "test-token" not in logged

    def test_the_legacy_unnamed_token_still_works(self, mock_client, mocker):
        """Deploying this change must not lock out an existing .env, or the
        upgrade needs a container restart and a config edit in lockstep."""
        parsed = snapshots._parse_token_set("", "old-single-token")
        assert parsed == (snapshots.SnapshotToken("legacy", "read", "old-single-token"),)


class TestScopes:
    """`write` is reserved and unused today. It is enforced now so the route
    that needs it — Plan 108's deploy trigger, which mounts the Docker socket —
    cannot be reached by a credential issued for downloads."""

    def test_a_read_token_is_refused_by_a_write_route(self, mocker):
        from fastapi import HTTPException

        dependency = snapshots.require_snapshot_token("write")
        with pytest.raises(HTTPException) as excinfo:
            dependency(authorization="Bearer test-token")
        assert excinfo.value.status_code == 403
        assert "requires 'write'" in excinfo.value.detail

    def test_a_write_token_may_also_read(self, mocker):
        """Otherwise one caller needs two credentials, which is the arrangement
        people work around rather than follow."""
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (
            snapshots.SnapshotToken("deployer", "write", "w-token"),
        ))
        assert snapshots.require_snapshot_token("read")(
            authorization="Bearer w-token",
        ) is None
        assert snapshots.require_snapshot_token("write")(
            authorization="Bearer w-token",
        ) is None

    def test_the_refusal_names_scopes_not_the_token(self, mocker):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as excinfo:
            snapshots.require_snapshot_token("write")(authorization="Bearer test-token")
        assert "test-token" not in excinfo.value.detail

    def test_an_unknown_scope_is_a_programming_error_not_a_403(self):
        """Caught when the route is declared, at import, rather than on the
        first request that happens to exercise it."""
        with pytest.raises(ValueError):
            snapshots.require_snapshot_token("delete")


class TestStorageSeam:
    """`_resolve_token` and `_tokens_configured` are the only two functions that
    know where credentials live. Everything above them reads the returned entry.

    These tests are what a later move to table-backed tokens lands against: swap
    the two bodies, and if the auth path still passes, the swap did not change
    behaviour. They assert against the seam rather than through a route, which
    is the point — the route should not be able to tell.
    """

    def test_resolve_returns_the_matching_entry(self, mocker):
        entry = snapshots.SnapshotToken("mlflow", "read", "m-token")
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (entry,))
        assert snapshots._resolve_token("m-token") is entry

    def test_resolve_returns_none_for_an_unknown_token(self):
        assert snapshots._resolve_token("nope") is None

    def test_resolve_compares_every_entry_without_stopping_early(self, mocker):
        """The timing property, asserted rather than left to a comment. An early
        exit leaks nothing about a token's value but does leak which caller
        presented it, through how long the response took."""
        calls: list[str] = []

        def counting_compare(presented, stored):
            calls.append(stored)
            return presented == stored

        mocker.patch.object(snapshots.secrets, "compare_digest", counting_compare)
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (
            snapshots.SnapshotToken("first", "read", "a"),
            snapshots.SnapshotToken("second", "read", "b"),
            snapshots.SnapshotToken("third", "read", "c"),
        ))

        # Matching the *first* entry must still compare the other two.
        snapshots._resolve_token("a")
        assert calls == ["a", "b", "c"]

    def test_configured_is_a_separate_question_from_resolution(self, mocker):
        """503 and 403 answer different questions and must not collapse into
        one: "this deployment has no tokens" is an operator's problem, "your
        token is wrong" is the caller's."""
        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", ())
        assert snapshots._tokens_configured() is False
        assert snapshots._resolve_token("anything") is None

        mocker.patch.object(snapshots, "SNAPSHOT_TOKENS", (
            snapshots.SnapshotToken("ci", "read", "t"),
        ))
        assert snapshots._tokens_configured() is True


class TestTokenSetParsing:
    def test_parses_named_scoped_entries(self):
        parsed = snapshots._parse_token_set("ci:read:abc,mlflow:write:def", "")
        assert parsed == (
            snapshots.SnapshotToken("ci", "read", "abc"),
            snapshots.SnapshotToken("mlflow", "write", "def"),
        )

    def test_a_token_may_contain_colons(self):
        """Split on the first two colons only — otherwise a passphrase-style
        token silently becomes a truncated one that never matches."""
        parsed = snapshots._parse_token_set("ci:read:a:b:c", "")
        assert parsed == (snapshots.SnapshotToken("ci", "read", "a:b:c"),)

    @pytest.mark.parametrize("raw", ["notoken", "ci:read", "ci::abc", ":read:abc", "ci:read:"])
    def test_malformed_entries_are_dropped_not_raised(self, raw, caplog):
        """A typo in one entry must not take the router down at import and lock
        every caller out — the blast radius of a config error is that one
        caller's 403, not a fleet-wide 503."""
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert snapshots._parse_token_set(raw, "") == ()
        assert "malformed" in caplog.text

    def test_an_unknown_scope_entry_is_dropped_and_named(self, caplog):
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert snapshots._parse_token_set("ci:admin:abc", "") == ()
        assert "unknown scope" in caplog.text
        assert "abc" not in caplog.text

    def test_a_malformed_entry_never_logs_its_value(self, caplog):
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            snapshots._parse_token_set("ci:read", "")
        assert "ci:read" not in caplog.text

    def test_whitespace_and_empty_entries_are_tolerated(self):
        parsed = snapshots._parse_token_set(" ci:read:abc , , mlflow:read:def ", "")
        assert [entry.name for entry in parsed] == ["ci", "mlflow"]


# ---------------------------------------------------------------------------
# GET /latest
# ---------------------------------------------------------------------------

class TestLatest:
    def test_returns_pointer_json(self, mock_client, mocker):
        pointer = {
            "snapshot_id": "adaptive-refresh-2026-07-07-174500",
            "export_fingerprint": "abc123",
            "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
            "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
            "archive_bytes": 1024,
            "archive_sha256": "deadbeef",
        }
        mocker.patch.object(snapshots, "read_json", return_value=pointer)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == pointer

    def test_missing_latest_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 404

    def test_read_error_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", side_effect=RuntimeError("boom"))
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /{snapshot_id}
# ---------------------------------------------------------------------------

ALIAS = {
    "snapshot_id": "adaptive-refresh-2026-07-07-174500",
    "export_fingerprint": "abc123",
    "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
    "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
    "archive_bytes": 1024,
    "archive_sha256": "deadbeef",
}
MANIFEST = {
    "snapshot_id": ALIAS["snapshot_id"],
    "tier": "edge",
    "archive": {
        "path": ALIAS["archive_key"],
        "bytes": 1024,
        "sha256": "deadbeef",
        "file_count": 3,
    },
}


class TestSnapshotManifest:
    def test_resolves_through_alias(self, mock_client, mocker):
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if key == alias_key else MANIFEST,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == MANIFEST

    def test_missing_alias_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/nonexistent-snapshot", headers=AUTH)
        assert resp.status_code == 404

    def test_alias_snapshot_id_mismatch_is_404(self, mock_client, mocker):
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        mismatched_alias = dict(ALIAS, snapshot_id="some-other-snapshot")
        read_json_mock = mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: mismatched_alias if key == alias_key else MANIFEST,
        )

        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)

        assert resp.status_code == 404
        read_json_mock.assert_called_once_with(alias_key)

    def test_reused_archive_manifest_snapshot_id_is_overlaid(self, mock_client, mocker):
        reused_manifest = dict(MANIFEST, snapshot_id="original-packaging-snapshot")
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else reused_manifest,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json()["snapshot_id"] == ALIAS["snapshot_id"]

    @pytest.mark.parametrize(("archive_field", "bad_value"), [
        ("path", "snapshot_archives/fingerprints/other/snapshot.tar.zst"),
        ("bytes", 2048),
        ("sha256", "bad-sha"),
    ])
    def test_manifest_archive_mismatch_is_404(
        self, mock_client, mocker, archive_field, bad_value,
    ):
        bad_manifest = {
            **MANIFEST,
            "archive": {**MANIFEST["archive"], archive_field: bad_value},
        }
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else bad_manifest,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 404

    def test_missing_manifest_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 404

    def test_invalid_snapshot_id_rejected(self, mock_client):
        resp = mock_client.get(f"{BASE}/..%2f..%2fetc%2fpasswd", headers=AUTH)
        assert resp.status_code in (400, 404)

    def test_invalid_snapshot_id_with_dots_rejected(self, mock_client, mocker):
        read_json_mock = mocker.patch.object(snapshots, "read_json")
        resp = mock_client.get(f"{BASE}/adaptive..refresh", headers=AUTH)
        assert resp.status_code == 400
        read_json_mock.assert_not_called()

    @pytest.mark.parametrize("bad_manifest_key", [
        "s3://other-bucket/snapshot_archives/fingerprints/abc123/archive_manifest.json",
        "/etc/passwd",
        "snapshot_archives/fingerprints/../../../etc/passwd",
        "snapshot_archives/fingerprints/abc123/../../secret.json",
        "some/other/prefix/archive_manifest.json",
        "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",  # wrong object name
        "snapshot_planning_cache/fingerprints/abc123/planning.json",
    ])
    def test_tampered_archive_manifest_key_rejected(self, mock_client, mocker, bad_manifest_key):
        tampered_alias = dict(ALIAS, archive_manifest_key=bad_manifest_key)
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        read_json_mock = mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: tampered_alias if key == alias_key else MANIFEST,
        )

        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)

        assert resp.status_code == 404
        # Only the alias lookup should ever happen — the bad key must never
        # be passed through to a second read_json call.
        read_json_mock.assert_called_once_with(alias_key)


# ---------------------------------------------------------------------------
# GET /{snapshot_id}/download
# ---------------------------------------------------------------------------

class TestDownload:
    def test_streams_archive_bytes(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=1024)
        mocker.patch.object(snapshots, "open_stream", return_value=iter([b"chunk-1", b"chunk-2"]))

        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)

        assert resp.status_code == 200
        assert resp.content == b"chunk-1chunk-2"
        assert resp.headers["content-type"] == "application/zstd"
        assert resp.headers["content-length"] == "1024"
        assert resp.headers["x-archive-sha256"] == "deadbeef"
        assert "adaptive-refresh-2026-07-07-174500" in resp.headers["content-disposition"]

    def test_missing_alias_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/nonexistent-snapshot/download", headers=AUTH)
        assert resp.status_code == 404

    def test_alias_snapshot_id_mismatch_is_404(self, mock_client, mocker):
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        mismatched_alias = dict(ALIAS, snapshot_id="some-other-snapshot")
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: mismatched_alias if key == alias_key else None,
        )
        object_size_mock = mocker.patch.object(snapshots, "object_size")
        open_stream_mock = mocker.patch.object(snapshots, "open_stream")

        resp = mock_client.get(
            f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH,
        )

        assert resp.status_code == 404
        object_size_mock.assert_not_called()
        open_stream_mock.assert_not_called()

    def test_missing_archive_object_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=None)
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)
        assert resp.status_code == 404

    def test_open_stream_error_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=1024)
        mocker.patch.object(snapshots, "open_stream", side_effect=RuntimeError("boom"))
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)
        assert resp.status_code == 404

    def test_invalid_snapshot_id_rejected(self, mock_client, mocker):
        object_size_mock = mocker.patch.object(snapshots, "object_size")
        resp = mock_client.get(f"{BASE}/../etc/download", headers=AUTH)
        assert resp.status_code in (400, 404)
        object_size_mock.assert_not_called()

    @pytest.mark.parametrize("bad_archive_key", [
        "s3://other-bucket/snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
        "/etc/passwd",
        "snapshot_archives/fingerprints/../../../etc/passwd",
        "snapshot_archives/fingerprints/abc123/../../secret.tar.zst",
        "some/other/prefix/snapshot.tar.zst",
        "snapshot_archives/fingerprints/abc123/archive_manifest.json",  # wrong object name
        "html/year=2026/month=1/artifact_type=detail_page/x.html.zst",
    ])
    def test_tampered_archive_key_rejected(self, mock_client, mocker, bad_archive_key):
        tampered_alias = dict(ALIAS, archive_key=bad_archive_key)
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: tampered_alias if key == alias_key else None,
        )
        object_size_mock = mocker.patch.object(snapshots, "object_size")
        open_stream_mock = mocker.patch.object(snapshots, "open_stream")

        resp = mock_client.get(
            f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH,
        )

        assert resp.status_code == 404
        # The bad key must never reach object_size or open_stream.
        object_size_mock.assert_not_called()
        open_stream_mock.assert_not_called()
