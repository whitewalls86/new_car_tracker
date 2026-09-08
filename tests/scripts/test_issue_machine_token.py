"""Unit tests for scripts/issue_machine_token.py (Plan 173, Stage A).

The script's whole job is to bring a credential into existence and then be the
only thing that ever saw it, so the tests concentrate on the ways it could
quietly fail to do that:

  A - what reaches the database is a digest, never the token
  B - the token is shown exactly once, on stdout, and never through the logger
  C - the digest it stores is the one the router looks up
  D - expiry is not optional, and defaults rather than being forgotten
"""
from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ops.routers.snapshots import token_digest
from scripts.issue_machine_token import (
    DEFAULT_EXPIRY_DAYS,
    issue_token,
    main,
)


def _fake_db(mocker, row=None):
    """Patch the script's db_cursor and hand back the cursor it was given."""
    cursor = MagicMock()
    cursor.fetchone.return_value = row or {
        "id": 1,
        "name": "ci",
        "scope": "read",
        "created_at": datetime(2026, 9, 8, tzinfo=timezone.utc),
        "expires_at": datetime(2027, 9, 8, tzinfo=timezone.utc),
    }

    @contextmanager
    def db_cursor(*_args, **_kwargs):
        yield cursor

    mocker.patch("scripts.issue_machine_token.db_cursor", db_cursor)
    return cursor


# ---------------------------------------------------------------------------
# A - what reaches the database
# ---------------------------------------------------------------------------

class TestOnlyTheDigestIsStored:
    def test_the_insert_carries_the_digest_and_not_the_token(self, mocker):
        cursor = _fake_db(mocker)
        token, _ = issue_token("ci", "read", 365)

        params = cursor.execute.call_args.args[1]
        assert token_digest(token) in params
        assert token not in params

    def test_the_digest_is_sha256_of_the_token(self, mocker):
        _fake_db(mocker)
        token, _ = issue_token("ci", "read", 365)
        assert token_digest(token) == hashlib.sha256(token.encode("utf-8")).hexdigest()

    def test_the_router_would_find_what_this_stored(self, mocker):
        """The one thing a duplicated hash definition would break, and it would
        break silently: every token issued here would simply never
        authenticate, with nothing in any log to say why. Asserted by importing
        the router's own function rather than restating it."""
        cursor = _fake_db(mocker)
        token, _ = issue_token("ci", "read", 365)

        stored = cursor.execute.call_args.args[1][2]
        assert stored == token_digest(token)

    def test_the_token_is_high_entropy_hex(self, mocker):
        """256 bits, which is why SHA-256 rather than a password KDF is the
        right hash: there is nothing here for an attacker to guess at any
        speed. Hex also contains no comma or colon, so a token issued during
        the migration is still pasteable into the environment fallback."""
        _fake_db(mocker)
        token, _ = issue_token("ci", "read", 365)

        assert len(token) == 64
        int(token, 16)


# ---------------------------------------------------------------------------
# B - the token is shown once and never logged
# ---------------------------------------------------------------------------

class TestTheTokenIsShownOnce:
    def test_main_prints_the_token_to_stdout(self, mocker, capsys):
        _fake_db(mocker)
        mocker.patch(
            "scripts.issue_machine_token.secrets.token_hex", return_value="a" * 64,
        )

        assert main(["--name", "ci"]) == 0
        assert "a" * 64 in capsys.readouterr().out

    def test_the_token_never_reaches_a_logger(self, mocker, capsys, caplog):
        """A token in a log line is the leak this plan exists to prevent, and
        the ops logger ships to Loki — where it would then also be searchable."""
        _fake_db(mocker)
        mocker.patch(
            "scripts.issue_machine_token.secrets.token_hex", return_value="b" * 64,
        )

        with caplog.at_level(logging.DEBUG):
            main(["--name", "ci"])
        capsys.readouterr()
        assert "b" * 64 not in caplog.text

    def test_the_operator_is_told_there_is_no_second_chance(self, mocker, capsys):
        """Nothing stores the plaintext, so a caller who closes the terminal has
        to issue a new credential. That is a property worth stating on the
        screen rather than only in the docstring."""
        _fake_db(mocker)
        main(["--name", "ci"])
        assert "once" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# C / D - the arguments that make a credential what it is
# ---------------------------------------------------------------------------

class TestIssuedAttributes:
    def test_name_and_scope_are_stored_as_given(self, mocker):
        cursor = _fake_db(mocker)
        issue_token("mlflow", "write", 30)

        params = cursor.execute.call_args.args[1]
        assert params[0] == "mlflow"
        assert params[1] == "write"

    def test_created_by_is_recorded_when_given(self, mocker):
        cursor = _fake_db(mocker)
        issue_token("ci", "read", 365, created_by="andrew, laptop")
        assert "andrew, laptop" in cursor.execute.call_args.args[1]

    def test_the_expiry_is_the_requested_number_of_days_out(self, mocker):
        cursor = _fake_db(mocker)
        before = datetime.now(timezone.utc)
        issue_token("ci", "read", 30)

        expires_at = cursor.execute.call_args.args[1][4]
        assert timedelta(days=29) < expires_at - before < timedelta(days=31)

    def test_the_default_expiry_is_a_year_not_never(self, mocker, capsys):
        """A credential that expires on its own beats one somebody must remember
        to kill, so the default is a date rather than NULL and there is no flag
        that reaches NULL. Making a row perpetual is a deliberate UPDATE."""
        cursor = _fake_db(mocker)
        main(["--name", "ci"])
        capsys.readouterr()

        before = datetime.now(timezone.utc)
        expires_at = cursor.execute.call_args.args[1][4]
        assert expires_at - before > timedelta(days=DEFAULT_EXPIRY_DAYS - 1)

    @pytest.mark.parametrize("days", [0, -1])
    def test_a_non_positive_expiry_is_refused_before_anything_is_written(
        self, mocker, days,
    ):
        cursor = _fake_db(mocker)
        with pytest.raises(ValueError):
            issue_token("ci", "read", days)
        cursor.execute.assert_not_called()

    def test_main_reports_a_bad_expiry_as_an_error_exit(self, mocker, capsys):
        _fake_db(mocker)
        assert main(["--name", "ci", "--expires-in-days", "0"]) == 2
        assert "error" in capsys.readouterr().err

    def test_an_unknown_scope_is_refused_by_the_parser(self, mocker):
        """Before a row exists, rather than by the CHECK constraint after one
        does — the caller gets the list of valid scopes either way, but only
        this way is there nothing to clean up."""
        _fake_db(mocker)
        with pytest.raises(SystemExit):
            main(["--name", "ci", "--scope", "admin"])
