"""Unit tests for ops/routers/snapshots.py — Plan 120 Gate F download API."""
import hashlib
from contextlib import contextmanager

import pytest

from ops.queries import SELECT_MACHINE_TOKEN, TOUCH_MACHINE_TOKEN_LAST_USED
from ops.routers import snapshots

BASE = "/admin/snapshots/adaptive-refresh"
AUTH = {"Authorization": "Bearer test-token"}

# Captured before the autouse fixture below replaces them. The lifecycle tests
# assert on the real lookup, and calling these directly is how they reach past
# the stub the route tests need — clearer than a fixture that undoes another
# fixture, and it keeps those tests honest about testing a function rather than
# a route.
REAL_RESOLVE_MACHINE_TOKEN = snapshots._resolve_machine_token
REAL_MACHINE_TOKENS_EXIST = snapshots._machine_tokens_exist


def _row(name="ci", scope="read", is_revoked=False, is_expired=False):
    """A row shaped like select_machine_token.sql's result."""
    return {
        "name": name, "scope": scope,
        "is_revoked": is_revoked, "is_expired": is_expired,
    }


@pytest.fixture
def machine_tokens(mocker):
    """Stand in for `ops.machine_tokens` and hand back the cursor it was asked.

    Patches the router's `db_cursor` rather than psycopg2 so a test states the
    row the table returns and nothing else has to be true. The returned mock is
    where a test reads back what was executed and with which parameters.
    """
    cursor = mocker.MagicMock()

    @contextmanager
    def fake_cursor(*args, **kwargs):
        yield cursor

    mocker.patch.object(snapshots, "db_cursor", fake_cursor)
    return cursor


@pytest.fixture(autouse=True)
def _token(mocker):
    """Authenticate `test-token` as the `ci` caller, through the table.

    The table is the only storage there is, since Plan 173's second deploy
    retired the environment set.

    Stubbing the seam is also what keeps them honest: `mock_client` mocks
    `psycopg2.connect`, so an unstubbed lookup gets a `MagicMock` back, and a
    `MagicMock` is truthy — every string would authenticate as a caller whose
    name is a mock.
    """
    mocker.patch.object(
        snapshots, "_resolve_machine_token",
        side_effect=lambda presented: (
            snapshots.SnapshotToken("ci", "read") if presented == "test-token" else None
        ),
    )
    mocker.patch.object(snapshots, "_machine_tokens_exist", return_value=True)


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
        mocker.patch.object(snapshots, "_machine_tokens_exist", return_value=False)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 503

    def test_an_unreachable_table_is_a_503_not_a_403(self, mock_client, mocker, machine_tokens):
        """The caller presented a perfectly good token. Blaming it for the
        database being down points the person debugging at the one component
        that was healthy."""
        machine_tokens.execute.side_effect = RuntimeError("connection refused")
        assert REAL_MACHINE_TOKENS_EXIST() is False

        mocker.patch.object(snapshots, "_machine_tokens_exist", REAL_MACHINE_TOKENS_EXIST)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 503


class TestNamedTokens:
    """Plan 162: one entry per caller, so revocation and attribution are
    per-caller. Plan 173 moved those entries into rows."""

    def test_any_caller_in_the_table_authenticates(self, mock_client, mocker):
        live = {"ci-token": "ci", "mlflow-token": "mlflow"}
        mocker.patch.object(
            snapshots, "_resolve_machine_token",
            side_effect=lambda presented: (
                snapshots.SnapshotToken(live[presented], "read")
                if presented in live else None
            ),
        )
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})
        for token in live:
            resp = mock_client.get(
                f"{BASE}/latest", headers={"Authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 200, token

    def test_revoking_one_caller_leaves_the_others_alone(self, mock_client, mocker):
        """The property the whole design exists for. A shared string cannot do
        this: revoking CI would take the laptop and the MLflow rehearsal with
        it."""
        mocker.patch.object(
            snapshots, "_resolve_machine_token",
            side_effect=lambda presented: (
                snapshots.SnapshotToken("mlflow", "read")
                if presented == "mlflow-token" else None
            ),
        )
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})

        revoked = mock_client.get(
            f"{BASE}/latest", headers={"Authorization": "Bearer ci-token"},
        )
        survivor = mock_client.get(
            f"{BASE}/latest", headers={"Authorization": "Bearer mlflow-token"},
        )
        assert revoked.status_code == 403
        assert survivor.status_code == 200

    def test_revocation_takes_effect_without_a_restart(self, mock_client, mocker):
        """Plan 173's exit condition, in miniature. The environment form could
        not do this at all: `.env` is read at import, so revoking meant an SSH
        and a container restart. Here the same process serves a 200 and then a
        403 with nothing reloaded between them.

        The production demonstration is an `UPDATE ... SET revoked_at = now()`
        against a live token — this asserts the mechanism that makes it work,
        which is that nothing about the credential is cached in the module."""
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})
        assert mock_client.get(f"{BASE}/latest", headers=AUTH).status_code == 200

        mocker.patch.object(snapshots, "_resolve_machine_token", return_value=None)
        assert mock_client.get(f"{BASE}/latest", headers=AUTH).status_code == 403

    def test_the_caller_name_is_logged_and_the_token_is_not(self, mock_client, mocker, caplog):
        mocker.patch.object(snapshots, "read_json", return_value={"snapshot_id": "s1"})
        with caplog.at_level("INFO", logger="pipeline_ops"):
            mock_client.get(f"{BASE}/latest", headers=AUTH)
        logged = caplog.text
        assert "caller=ci" in logged
        assert "test-token" not in logged


class TestScopes:
    """`write` is reserved and unused today. It is enforced now so the route
    that needs it — Plan 108's deploy trigger, which mounts the Docker socket —
    cannot be reached by a credential issued for downloads."""

    def test_a_read_token_is_refused_by_a_write_route(self):
        from fastapi import HTTPException

        dependency = snapshots.require_snapshot_token("write")
        with pytest.raises(HTTPException) as excinfo:
            dependency(authorization="Bearer test-token")
        assert excinfo.value.status_code == 403
        assert "requires 'write'" in excinfo.value.detail

    def test_a_write_token_may_also_read(self, mocker):
        """Otherwise one caller needs two credentials, which is the arrangement
        people work around rather than follow."""
        mocker.patch.object(
            snapshots, "_resolve_machine_token",
            return_value=snapshots.SnapshotToken("deployer", "write"),
        )
        assert snapshots.require_snapshot_token("read")(
            authorization="Bearer w-token",
        ) is None
        assert snapshots.require_snapshot_token("write")(
            authorization="Bearer w-token",
        ) is None

    def test_the_refusal_names_scopes_not_the_token(self):
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

    These are what Plan 173's move landed against: the two bodies were swapped
    and the auth path did not change, which is what the seam was extracted for.
    They assert against the seam rather than through a route, which is the point
    — the route should not be able to tell.
    """

    def test_resolve_returns_the_matching_caller(self, mocker):
        entry = snapshots.SnapshotToken("mlflow", "read")
        mocker.patch.object(snapshots, "_resolve_machine_token", return_value=entry)
        assert snapshots._resolve_token("m-token") is entry

    def test_resolve_returns_none_for_an_unknown_token(self, mocker):
        mocker.patch.object(snapshots, "_resolve_machine_token", return_value=None)
        assert snapshots._resolve_token("nope") is None

    def test_a_resolved_caller_carries_no_token(self):
        """The field was dropped with the environment form. A credential kept on
        the object handed to every route is one a later change logs by
        accident."""
        assert snapshots.SnapshotToken._fields == ("name", "scope")

    def test_configured_is_a_separate_question_from_resolution(self, mocker):
        """503 and 403 answer different questions and must not collapse into
        one: "this deployment has no tokens" is an operator's problem, "your
        token is wrong" is the caller's."""
        mocker.patch.object(snapshots, "_machine_tokens_exist", return_value=False)
        mocker.patch.object(snapshots, "_resolve_machine_token", return_value=None)
        assert snapshots._tokens_configured() is False
        assert snapshots._resolve_token("anything") is None

        mocker.patch.object(snapshots, "_machine_tokens_exist", return_value=True)
        assert snapshots._tokens_configured() is True


# ---------------------------------------------------------------------------
# The credential table — Plan 173
# ---------------------------------------------------------------------------

class TestTokenDigest:
    def test_the_stored_form_is_sha256_hex(self):
        assert snapshots.token_digest("abc") == hashlib.sha256(b"abc").hexdigest()

    def test_the_plaintext_is_not_recoverable_from_it(self):
        """Stating the property the column exists for: a pg_dump, a screenshot
        of pgAdmin or a stray query result hands over nothing usable."""
        assert snapshots.token_digest("secret") != "secret"
        assert len(snapshots.token_digest("secret")) == 64


class TestMachineTokenLifecycle:
    """`expires_at`, `revoked_at` and `last_used_at` — what a row carries that
    an environment variable never could."""

    def test_a_live_row_authenticates_as_its_caller(self, machine_tokens):
        machine_tokens.fetchone.return_value = _row(name="ci", scope="read")
        assert REAL_RESOLVE_MACHINE_TOKEN("t") == snapshots.SnapshotToken("ci", "read")

    def test_an_unknown_digest_resolves_to_nothing(self, machine_tokens):
        machine_tokens.fetchone.return_value = None
        assert REAL_RESOLVE_MACHINE_TOKEN("t") is None

    def test_a_revoked_row_is_refused(self, machine_tokens, caplog):
        machine_tokens.fetchone.return_value = _row(is_revoked=True)
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert REAL_RESOLVE_MACHINE_TOKEN("t") is None
        assert "revoked" in caplog.text

    def test_an_expired_row_is_refused(self, machine_tokens, caplog):
        machine_tokens.fetchone.return_value = _row(is_expired=True)
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert REAL_RESOLVE_MACHINE_TOKEN("t") is None
        assert "expired" in caplog.text

    def test_the_refusal_reason_is_logged_but_the_caller_is_told_nothing(
        self, mock_client, mocker, machine_tokens,
    ):
        """A revoked credential and a token that was never issued are different
        events in the log and the same 403 on the wire. The distinction is for
        whoever reads the log later, not for whoever is holding the token."""
        machine_tokens.fetchone.return_value = _row(name="ci", is_revoked=True)
        mocker.patch.object(snapshots, "_resolve_machine_token", REAL_RESOLVE_MACHINE_TOKEN)

        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 403
        assert "revoked" not in resp.json()["detail"]

    def test_the_lookup_never_sends_the_plaintext_to_the_database(self, machine_tokens):
        """The whole point of the column being a digest. A parameter carrying
        the token would put it in the statement log of anything watching."""
        machine_tokens.fetchone.return_value = _row()
        REAL_RESOLVE_MACHINE_TOKEN("plaintext-token")

        statement, params = machine_tokens.execute.call_args_list[0].args
        assert statement is SELECT_MACHINE_TOKEN
        assert params == (snapshots.token_digest("plaintext-token"),)
        assert "plaintext-token" not in params

    def test_a_lookup_failure_falls_through_rather_than_raising(self, machine_tokens, caplog):
        """A 500 from the auth path would take the route down for a caller whose
        credential is fine. The 503 for an unreachable database is
        _tokens_configured's to raise, and it runs first."""
        machine_tokens.execute.side_effect = RuntimeError("boom")
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert REAL_RESOLVE_MACHINE_TOKEN("t") is None
        assert "lookup failed" in caplog.text


class TestLastUsedAt:
    """Most of why the table is worth building. Revocation is trivial; knowing
    whether anything still depends on a credential is not."""

    def test_a_successful_resolution_records_the_use(self, machine_tokens):
        machine_tokens.fetchone.return_value = _row()
        REAL_RESOLVE_MACHINE_TOKEN("t")

        statements = [call.args[0] for call in machine_tokens.execute.call_args_list]
        # Identity against the imported constant, not a substring of it: a
        # substring match would go on passing after the statement's WHERE
        # clause — which is the throttle — were dropped.
        assert statements == [SELECT_MACHINE_TOKEN, TOUCH_MACHINE_TOKEN_LAST_USED]

    def test_a_refused_credential_records_nothing(self, machine_tokens):
        """`last_used_at` answers "is anything still *authenticating* with
        this". A revoked row that kept moving would say a dead credential is
        live, which is the one answer that would make revoking it look unsafe."""
        machine_tokens.fetchone.return_value = _row(is_revoked=True)
        REAL_RESOLVE_MACHINE_TOKEN("t")

        assert len(machine_tokens.execute.call_args_list) == 1

    def test_the_throttle_window_is_bound_not_interpolated(self, machine_tokens):
        """It reaches the statement as a parameter, so the SQL file names no
        window of its own and the two cannot drift."""
        machine_tokens.fetchone.return_value = _row()
        REAL_RESOLVE_MACHINE_TOKEN("t")

        statement, params = machine_tokens.execute.call_args_list[1].args
        assert statement is TOUCH_MACHINE_TOKEN_LAST_USED
        assert params == (snapshots.token_digest("t"), snapshots.LAST_USED_THROTTLE)

    def test_a_failed_bookkeeping_write_does_not_refuse_the_caller(
        self, machine_tokens, caplog,
    ):
        """A stale answer to "is anything still using this" is a far better
        outcome than a download that fails because a timestamp would not
        update."""
        machine_tokens.fetchone.return_value = _row(name="ci", scope="read")
        machine_tokens.execute.side_effect = [None, RuntimeError("boom")]

        with caplog.at_level("WARNING", logger="pipeline_ops"):
            resolved = REAL_RESOLVE_MACHINE_TOKEN("t")
        assert resolved == snapshots.SnapshotToken("ci", "read")
        assert "last_used_at update failed" in caplog.text


class TestConfiguredFromTheTable:
    def test_a_live_row_makes_the_deployment_configured(self, machine_tokens):
        machine_tokens.fetchone.return_value = (True,)
        assert REAL_MACHINE_TOKENS_EXIST() is True

    def test_no_live_row_makes_it_unconfigured(self, machine_tokens):
        """A deployment whose only credential expired overnight reports itself
        unconfigured, which is true, rather than refusing every caller as though
        each had presented a bad token."""
        machine_tokens.fetchone.return_value = (False,)
        assert REAL_MACHINE_TOKENS_EXIST() is False

    def test_an_unreadable_table_is_unconfigured_not_an_exception(
        self, machine_tokens, caplog,
    ):
        machine_tokens.execute.side_effect = RuntimeError("connection refused")
        with caplog.at_level("WARNING", logger="pipeline_ops"):
            assert REAL_MACHINE_TOKENS_EXIST() is False
        assert "configuration check failed" in caplog.text


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
