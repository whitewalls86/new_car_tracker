"""Plan 145 Stage 5 slice 3 -- the V040 live-state proof.

The claim under test: recovery changes **no live state**. Specifically, the
four protected hot tables

    ops.price_observations   ops.vin_to_listing
    ops.blocked_cooldown     ops.detail_scrape_claims

and the two time-dependent V040 views

    ops.ops_vehicle_staleness   ops.ops_detail_scrape_queue
    (db/migrations/V040__detail_scrape_circuit_breaker.sql)

are byte-equivalent before and after the write canary runs.

Run against live production the claim is meaningless: production legitimately
writes those same tables while the canary runs, so a before/after difference
proves nothing and an equality is luck. So this script **refuses to run
without a named maintenance window** (``--window``), and it assumes every
service with write access to the protected tables is **already quiesced**.

    IT DOES NOT PAUSE OR RESUME ANY SERVICE. Pausing and resuming the writers
    is a manual, separately approved production action performed by the
    maintainer -- before and after this script, respectively.

The sequence:

  1. refuse without ``--window``; record the window name in the report;
  2. open one verifier connection, one ``READ COMMITTED`` transaction. ``now()``
     is ``transaction_timestamp()`` and is fixed for the whole transaction at
     every isolation level, so both view snapshots see one ``now()``. It must
     **not** be ``REPEATABLE READ``: that freezes the transaction's data
     snapshot at the first statement, so the second snapshot could never see
     the canary's committed writes and the equality check could never fail;
  3. snapshot the four tables and two views (an order-independent content
     digest + a row count), and ``txid_current()``;
  4. run the canary on a **separate connection** (``--canary-cmd`` subprocess,
     or an injected callable in tests, or just ``--settle-seconds``);
  5. snapshot again **in the same transaction**; assert the two snapshots
     share a ``txid_current()``;
  6. require every digest byte-equivalent;
  7. the maintainer restarts the writers.

Exit code: 0 pass, 1 fail (a relation changed, or the two snapshots were not
one transaction, or the canary command failed), 2 refused (no ``--window``).

``--canary-cmd`` must run the Phase B write canary that commits **exactly** the
objects in ``recovery/plan145/canary/<run>-canary_sample.parquet`` (the
`canary-sample` mode's manifest), within the default ``--max-unapproved-rows``
budget. Do **not** pass ``--maintainer-approval`` here -- that flag lifts the
row budget, and Plan 145 allows nothing beyond the ~500-row canary until this
proof closes. Phase B builds that command; it is not `apply --batch`, whose
unit is a full slice-2 batch of up to 5,000 artifacts.

    python scripts/verify_recovery_live_state.py --window <name> \\
        --canary-cmd "<phase B canary-commit command>" \\
        --report /tmp/p145-v040-<name>.json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

# ``python scripts/verify_recovery_live_state.py`` puts ``scripts/`` rather than
# the repository root on sys.path. Keep the documented direct invocation working
# while retaining package imports for tests and ``python -m`` usage.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

#: Live tables recovery must not mutate.
PROTECTED_TABLES = (
    "ops.price_observations",
    "ops.vin_to_listing",
    "ops.blocked_cooldown",
    "ops.detail_scrape_claims",
)
#: V040 views -- time-dependent, which is why both snapshots must share one
#: transaction (one fixed ``now()``).
V040_VIEWS = (
    "ops.ops_vehicle_staleness",
    "ops.ops_detail_scrape_queue",
)
RELATIONS = PROTECTED_TABLES + V040_VIEWS

_BANNER = (
    "Plan 145 Stage 5 slice 3 -- V040 live-state proof\n"
    "This script does NOT quiesce or resume any service. Run it ONLY inside a\n"
    "named maintenance window with every writer to the protected tables already\n"
    "stopped by the maintainer."
)


def _digest_sql(relation: str) -> str:
    """An order-independent content digest and a row count for one relation.

    ``bit_xor`` (PostgreSQL 14+) over ``hashtextextended(t::text, 0)`` -- a
    built-in 64-bit row hash -- is independent of physical row order and builds
    no intermediate. ``md5(string_agg(...))`` would: on a wide 1M-row view its
    single text value can exceed PostgreSQL's 1 GiB varlena limit and error out
    mid-window. ``coalesce`` handles the empty relation (``bit_xor`` of nothing
    is NULL).
    """
    return (
        "SELECT count(*)::bigint AS n, "
        "coalesce(bit_xor(hashtextextended(t::text, 0)), 0) AS digest "
        f"FROM {relation} t"
    )


def _snapshot(cur) -> tuple[int, dict[str, dict[str, Any]]]:
    """``(txid, {relation: {rows, digest}})`` -- everything from one cursor, so
    the two snapshots a run takes are provably in one transaction."""
    cur.execute("SELECT txid_current()")
    txid = int(cur.fetchone()[0])
    out: dict[str, dict[str, Any]] = {}
    for relation in RELATIONS:
        cur.execute(_digest_sql(relation))
        n, digest = cur.fetchone()
        out[relation] = {"rows": int(n), "digest": str(digest)}
    return txid, out


def _run_canary(args: argparse.Namespace,
                canary: Optional[Callable[[], Any]]) -> dict[str, Any]:
    """Run the canary between the two snapshots, on its own connection.

    A subprocess (``--canary-cmd``) is a separate process and therefore a
    separate database connection by construction. ``canary`` is the in-process
    seam the unit tests use to inject a mutation.
    """
    if canary is not None:
        value = canary()
        return {"mode": "callable", "returncode": 0, "value": repr(value)}
    if args.canary_cmd:
        completed = subprocess.run(args.canary_cmd, shell=True, check=False)
        return {"mode": "subprocess", "command": args.canary_cmd,
                "returncode": completed.returncode}
    if args.settle_seconds:
        time.sleep(args.settle_seconds)
        return {"mode": "settle_only", "returncode": 0,
                "seconds": args.settle_seconds}
    return {"mode": "none", "returncode": 0,
            "note": "no canary supplied -- before/after with nothing between "
                    "is a mechanism check only"}


def _parse(argv: Optional[list[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="verify_recovery_live_state",
        description="Plan 145 Stage 5 slice 3: prove the write canary mutates "
                    "no live state. Refuses to run without --window; does not "
                    "pause or resume any service.",
    )
    p.add_argument("--window", default=None,
                   help="REQUIRED. The name of the maintenance window this runs "
                        "in. Recorded in the report. The run is refused without "
                        "it -- a verifier that can be run casually will be.")
    p.add_argument("--canary-cmd", default=None,
                   help="Shell command that runs the write canary, executed on "
                        "its own connection between the two snapshots.")
    p.add_argument("--settle-seconds", type=float, default=0.0,
                   help="If no --canary-cmd is given, pause this long between "
                        "snapshots instead (default 0).")
    p.add_argument("--report", default=None,
                   help="Write the JSON report here (default: stdout only).")
    p.add_argument("--json", action="store_true",
                   help="Also print the full JSON report to stdout.")
    return p.parse_args(argv)


def run(argv: Optional[list[str]] = None, *,
        connect: Optional[Callable[[], Any]] = None,
        canary: Optional[Callable[[], Any]] = None) -> int:
    args = _parse(argv)
    print(_BANNER, file=sys.stderr)

    if not args.window:
        print("REFUSED: --window <name> is required. This proof is only valid "
              "inside a named maintenance window with production writers "
              "quiesced.", file=sys.stderr)
        return 2

    if connect is None:                       # pragma: no cover - real DB path
        from shared.db import get_conn
        connect = get_conn

    report: dict[str, Any] = {
        "plan": 145, "stage": 5, "slice": 3, "phase": "B-gate",
        "check": "v040_live_state",
        "window": args.window,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "relations": list(RELATIONS),
        "note": "this script does not pause or resume any service; the "
                "maintainer quiesces the writers before it and restarts them "
                "after it",
    }

    conn = connect()
    try:
        try:
            conn.autocommit = False
        except Exception:                     # pragma: no cover - fake conns
            pass
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            txid_before, snap_before = _snapshot(cur)
            canary_result = _run_canary(args, canary)
            txid_after, snap_after = _snapshot(cur)
        conn.rollback()
    finally:
        conn.close()

    single_transaction = txid_before == txid_after
    changed = {
        relation: {"before": snap_before[relation], "after": snap_after[relation]}
        for relation in RELATIONS
        if snap_before[relation] != snap_after[relation]
    }
    canary_ok = canary_result.get("returncode", 0) == 0
    passed = single_transaction and not changed and canary_ok

    report.update({
        "txid": {"before": txid_before, "after": txid_after,
                 "single_transaction": single_transaction},
        "canary": canary_result,
        "snapshot_before": snap_before,
        "snapshot_after": snap_after,
        "changed_relations": changed,
        "passed": passed,
    })

    if args.report:
        Path(args.report).write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))

    print()
    print(f"window               {args.window}")
    print(f"single transaction   {single_transaction}  "
          f"(txid {txid_before} -> {txid_after})")
    print(f"canary               {canary_result.get('mode')} "
          f"rc={canary_result.get('returncode')}")
    for relation in RELATIONS:
        mark = "CHANGED" if relation in changed else "unchanged"
        print(f"  {relation:<30} {snap_before[relation]['rows']:>10,} rows  {mark}")
    print(f"\nresult               {'PASS' if passed else 'FAIL'}")
    if not single_transaction:
        print("FAIL: the two V040 snapshots were not in one transaction; the "
              "time-dependent views make that proof invalid.", file=sys.stderr)
    if changed:
        print(f"FAIL: {len(changed)} protected relation(s) changed across the "
              f"canary: {sorted(changed)}", file=sys.stderr)
    if not canary_ok:
        print(f"FAIL: the canary command exited "
              f"{canary_result.get('returncode')}.", file=sys.stderr)
    print()
    return 0 if passed else 1


def main(argv: Optional[list[str]] = None) -> int:
    return run(argv)


if __name__ == "__main__":                    # pragma: no cover
    sys.exit(main())
