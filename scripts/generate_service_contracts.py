"""Generate each service's OpenAPI contract, and hold a change that does not move it.

Plan 162 Stage Z, gap G22.

**A contract nobody generates is a document.** This plan exists because
`ARCHITECTURE.md:179` was accurate in April 2026 and quietly false by August,
and a hand-written contract certified by review is that same failure wearing a
suit. So the artifact under ``contracts/`` is generated from the running app
and committed, and CI regenerates and diffs it. You cannot forget to update a
generated file; you can only fail to notice it changed, and the diff is what
makes noticing mandatory.

**Named ``generate`` rather than ``verify``.** The three ``verify_*`` scripts
beside this one check a recording of somebody *else's* surface -- the Docker
API, Promtail's config -- where regenerating is not a thing an author does.
This one's primary job is to write the artifact, and ``--check`` is the same
work with the write withheld. A ``verify_`` name would hide the fact that this
is also how you update the file the gate is complaining about.

**Nothing is dropped, and that is the whole design.** An earlier draft of this
stage proposed a "normalised projection" that discarded operation IDs and
volatile ordering, with a written record of what it dropped and why. That is a
maintained document whose drift is invisible by construction -- the fields it
drops are exactly the ones the gate can no longer see -- which is the defect
this plan is named after, reintroduced inside its own remedy.

The reason anyone reaches for dropping is that the generator is not
deterministic. Here it has exactly one cause, and ``constraints.txt`` fixes it
at the root: pin ``fastapi``, ``starlette``, ``pydantic`` and
``prometheus-fastapi-instrumentator`` and the schema becomes a pure function of
the source tree. So canonicalisation is ``sort_keys`` and an indent -- no
decisions, no list to maintain, no blind spot. If a field ever does move under
a pinned stack, that is a finding to record with the field named, not a
category to budget for in advance.

A moved operation ID in a diff costs a reviewer one second. A dropped field
that hides a real change costs an outage.
"""
from __future__ import annotations

import argparse
import difflib
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = REPO_ROOT / "contracts"

# Run in a subprocess, one service at a time, for the reason `_ROUTE_PROBE` in
# tests/test_testing_contract.py records: importing six FastAPI apps into one
# interpreter registers six sets of Prometheus collectors in a single registry
# and leaves `scraper/` on `sys.path` for everything after it.
#
# The enumerator is `app.openapi()` rather than a walk of `app.routes`, and
# that is not cosmetic. Up to FastAPI 0.128 `include_router` flattened a
# router's routes into `app.routes` with the prefix applied; by 0.141 it
# appends one wrapper resolved at match time, and a shallow walk returns four
# routes for `ops` instead of 54.
_SCHEMA_PROBE = """
import importlib, json, os, sys, tempfile
repo, service = sys.argv[1], sys.argv[2]
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "contract.log"))
if len(sys.argv) > 3:
    sys.path.insert(0, os.path.join(repo, service))
sys.path.insert(0, repo)
app = importlib.import_module(service + ".app").app
sys.stdout.write(json.dumps(app.openapi()))
"""


def service_apps() -> list[str]:
    """Every top-level package whose ``app.py`` constructs a ``FastAPI()``.

    Derived, never listed. A new service arrives with no artifact and fails the
    gate, which is the conversation this stage wants to force; a hand-kept list
    would let it arrive silently instead.

    ``dashboard`` has an ``app.py`` and no ``FastAPI(``: Streamlit owns its
    URLs and it serves no schema, so there is nothing here for it to own. That
    is a finding of this test rather than an exemption granted to it -- what
    ``dashboard`` owes instead is the "enough" table, as G7.
    """
    found = []
    for init in sorted(REPO_ROOT.glob("*/__init__.py")):
        package = init.parent
        if package.name == "tests":
            continue
        entrypoint = package / "app.py"
        if not entrypoint.is_file():
            continue
        if "FastAPI(" not in entrypoint.read_text(encoding="utf-8"):
            continue
        found.append(package.name)
    assert found, (
        "no service exposes a FastAPI app, so this gate would pass by having "
        "nothing to check. That is the failure it exists to prevent, not a "
        "repository with no services."
    )
    return found


def generate(service: str) -> str:
    """The canonical contract text for *service*.

    Two import recipes, tried in order, because production has two: most
    services import as a package from the repo root, and ``scraper`` runs with
    its own directory as the root. Trying the plain recipe first matters --
    putting ``ops/`` on ``sys.path`` shadows the standard library's ``email``
    with ``ops/email.py`` and the app never imports at all.
    """
    failures = []
    for extra in ([], ["--service-dir"]):
        result = subprocess.run(
            [sys.executable, "-c", _SCHEMA_PROBE, str(REPO_ROOT), service, *extra],
            capture_output=True,
            text=True,
            encoding="utf-8",
            # PYTHONHASHSEED is load-bearing, and this stage found out the hard
            # way: the first `--check` after the first write failed against
            # code nobody had touched. Six `ops` handlers are registered as
            # `api_route(..., methods=["GET", "HEAD"])`, and FastAPI builds the
            # operation ID from the route's *method set*. Set iteration order
            # for strings follows the per-process hash seed, so `info_page__get`
            # and `info_page__head` alternate between runs of identical source.
            #
            # This is not the version drift `constraints.txt` fixes -- pinning
            # every package would not have touched it -- and it is the one thing
            # that would have forced this file to start dropping fields. It does
            # not: a fixed seed makes set order reproducible, and nothing is
            # discarded.
            #
            # The stabilised value is arbitrary rather than correct. Both halves
            # of the pair are, which is why FastAPI emits `Duplicate Operation
            # ID` for all six -- a real defect this artifact made undeniable,
            # recorded for the plan rather than silently normalised away here.
            env={
                **os.environ,
                "PYTHONIOENCODING": "utf-8",
                "PYTHONHASHSEED": "0",
            },
        )
        if result.returncode == 0:
            schema = json.loads(result.stdout)
            # `sort_keys` and an indent, and deliberately nothing else. See the
            # module docstring: every further transformation would be a decision
            # someone has to maintain, and a field this file drops is a change
            # the gate can no longer see.
            return json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        failures.append(result.stderr.strip()[-600:])
    raise SystemExit(
        f"{service}'s app could not be imported, so its contract cannot be "
        f"generated. This is a failure, not a skip: an ungenerated contract "
        f"is indistinguishable from a service that serves nothing.\n\n"
        + "\n\n---\n\n".join(failures)
    )


def committed_path(service: str) -> Path:
    return CONTRACTS_DIR / f"{service}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate and diff without writing; exit 1 on any difference",
    )
    args = parser.parse_args()

    CONTRACTS_DIR.mkdir(exist_ok=True)
    drifted: list[str] = []

    for service in service_apps():
        generated = generate(service)
        path = committed_path(service)
        committed = path.read_text(encoding="utf-8") if path.is_file() else None

        if not args.check:
            if committed != generated:
                path.write_text(generated, encoding="utf-8", newline="\n")
                print(f"wrote {path.relative_to(REPO_ROOT).as_posix()}")
            continue

        if committed is None:
            drifted.append(
                f"{service}: no committed contract at "
                f"{path.relative_to(REPO_ROOT).as_posix()}"
            )
        elif committed != generated:
            diff = difflib.unified_diff(
                committed.splitlines(keepends=True),
                generated.splitlines(keepends=True),
                fromfile=f"{path.relative_to(REPO_ROOT).as_posix()} (committed)",
                tofile=f"{service} (as the app serves it)",
            )
            drifted.append(f"{service}:\n" + "".join(diff))

    if drifted:
        print(
            "These services do not serve what their committed contract says "
            "they serve:\n\n" + "\n\n".join(drifted) + "\n\n"
            "Regenerate with `python scripts/generate_service_contracts.py` "
            "and review the diff. A change here is a change to what other "
            "services and the DAGs can rely on, so the diff is the point: say "
            "'yes, I meant that' in review rather than letting it land unread.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
