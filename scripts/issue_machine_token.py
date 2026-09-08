"""
Issue a machine credential for the ops API (Plan 173).

    docker compose exec ops python scripts/issue_machine_token.py \\
        --name ci --scope read --created-by "andrew, laptop"

Prints the plaintext token **once**, to stdout, and stores only its SHA-256.
There is no second chance to read it and no recovery path: nothing anywhere
keeps the plaintext, which is the property the table exists to have. Copy it
into the consumer's secret store before closing the terminal.

**A script rather than raw SQL, and it earns its place for one reason:** a token
pasted into a `psql` session is a token in shell history. This never accepts a
token as an argument — it generates one, so the only copy that ever exists is
on the screen. Issuance is also the only operation here that needs code.
Revocation does not, and takes effect on the next request with no restart:

    UPDATE ops.machine_tokens SET revoked_at = now()
    WHERE name = 'ci' AND revoked_at IS NULL;

Nor does the question the table was built to answer:

    SELECT name, scope, created_at, expires_at, revoked_at, last_used_at
    FROM ops.machine_tokens ORDER BY name, created_at;
"""
from __future__ import annotations

import argparse
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ``python scripts/issue_machine_token.py`` puts ``scripts/`` rather than the
# repository root on sys.path. Keep the documented direct invocation working
# while retaining package imports for tests and ``python -m`` usage.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# `token_digest` is imported rather than retyped, and that is the point: it is
# the one definition of what "the digest" means. A second copy that drifted
# would mean every token issued here fails to authenticate, with nothing to say
# why — a worse failure than the one line it saves.
from ops.queries import INSERT_MACHINE_TOKEN
from ops.routers.snapshots import token_digest
from shared.db import db_cursor
from shared.db_vocabularies import MachineTokenScope

# 256 bits, hex — the same shape .env.example documented for the environment
# form, and high-entropy enough that SHA-256 rather than a password KDF is the
# right hash. Hex also happens to contain neither a comma nor a colon, so a
# token issued here is still pasteable into the fallback during the migration.
TOKEN_BYTES = 32

# A credential that expires on its own beats one somebody must remember to kill,
# so expiry is not optional and there is no --no-expiry flag. Making a row
# perpetual is a deliberate UPDATE by hand, not a default anyone can reach for.
DEFAULT_EXPIRY_DAYS = 365


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Issue a machine credential for the ops API (Plan 173)",
    )
    parser.add_argument(
        "--name", required=True,
        help="Caller name, logged on every authenticated request (e.g. ci, local)",
    )
    parser.add_argument(
        # Both the choices and the default come from the column's own
        # vocabulary, so a scope added by migration reaches this flag
        # without anyone remembering to widen a tuple here.
        "--scope", default=MachineTokenScope.READ, choices=tuple(MachineTokenScope),
        help="read (default) or write; write also grants read",
    )
    parser.add_argument(
        "--expires-in-days", dest="expires_in_days", type=int,
        default=DEFAULT_EXPIRY_DAYS,
        help=f"Days until the credential expires (default {DEFAULT_EXPIRY_DAYS})",
    )
    parser.add_argument(
        "--created-by", dest="created_by", default=None,
        help=(
            "Who issued this, for the record. Worth passing: this usually runs "
            "inside the ops container, where the OS user is root and says nothing"
        ),
    )
    return parser.parse_args(argv)


def issue_token(
    name: str,
    scope: str,
    expires_in_days: int,
    created_by: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Generate a credential, store its digest, and return ``(token, row)``.

    The token is returned rather than printed so a test can assert on it
    without reading stdout, and so the one place that prints it is
    :func:`main`.
    """
    if expires_in_days < 1:
        raise ValueError("--expires-in-days must be at least 1")

    token = secrets.token_hex(TOKEN_BYTES)
    expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

    with db_cursor(error_context="Issue machine token", dict_cursor=True) as cur:
        cur.execute(
            INSERT_MACHINE_TOKEN,
            (name, scope, token_digest(token), created_by, expires_at),
        )
        row = cur.fetchone()

    return token, dict(row)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        token, row = issue_token(
            args.name, args.scope, args.expires_in_days, args.created_by,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # stdout, and never the logger: a token in a log line is the leak this whole
    # plan exists to prevent, and the ops logger ships to Loki.
    print(f"Issued machine token id={row['id']} name={row['name']} scope={row['scope']}")
    print(f"Expires {row['expires_at'].isoformat()}")
    print()
    print(token)
    print()
    print("Shown once. Nothing stores the plaintext — copy it now.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
