"""CI's services are production's, in definition and in contents.

Plan 162 Stage Q / CAR-78.

Five heavy jobs -- ``dbt-models``, ``snapshot-dbt``, ``schema-contracts``,
``service-integration`` and ``lake-integration`` -- used to hand-declare their
own ``services:`` blocks and their own Flyway step: five copies of
``postgres:16``, four of ``minio/minio:latest`` and six transcriptions of
Flyway's argument list. **The drift was measurable and nothing measured it.**
CI's Postgres had neither production's ``shared_buffers``/``max_connections``
nor its ``shm_size``; CI's MinIO had no console, no OIDC configuration and no
``MINIO_PROMETHEUS_AUTH_TYPE``; CI's Flyway omitted ``-baselineOnMigrate=true``.

**The guard is a resolved-config diff, not a field-by-field parity test**, and
that is the whole design. ``tests/test_lakehouse_compose_config.py`` is the
precedent this file departs from: it ``yaml.safe_load``s single files and
asserts only what it names, so a field nobody thought of is a field nobody
checks. ``docker compose config`` instead resolves the entire merge chain --
override files, ``${VAR}`` interpolation, ``extends``, ``include`` -- and
normalises as it goes, so this file diffs the two resolved *documents* and
requires the difference to equal :data:`DECLARED_DIVERGENCES`. A drift cannot
be missed for not having been anticipated, because nothing here enumerates
what to look at.

**The honest claim is not "CI's services are byte-identical to production's."**
``cartracker-net`` and ``cartracker_pgdata`` are both ``external: true``, and
production's MinIO authenticates operators against Google. The end state is
production's definition plus a declared and asserted override set -- which is
what turns the residue from accidental into visible.

**Both directions fail**, on the discipline ``DORMANT_SUITES`` and
``DECLARED_SKIPS`` already use here: a difference with no entry fails, and an
entry describing a difference that no longer exists fails too. The second is
the one that keeps this file honest as the base compose file changes.

**On the skip, and why it is deliberately not declared.** These tests shell out
to ``docker``, so they skip where the CLI is absent -- a developer's machine
without Docker installed. They must not appear in
``tests/plugins/declared_skips.py``, and the reasoning is Stage U's mechanism
read forwards: ``ubuntu-latest`` ships the Docker CLI, so in CI these never
skip, and a declared skip that *stops* skipping fails the run. Leaving them
undeclared is what makes them **required** in CI rather than optional: if
Docker ever vanished from a runner, the skip would fire undeclared and
``REQUIRE_DECLARED_SKIPS`` would fail the run rather than let five jobs quietly
stop being checked. The dependency Stage Q was told to hand forward is
discharged by declaring nothing.

``docker compose config`` needs no daemon -- it is client-side resolution --
so these run on a runner with no Docker service and in a container with no
socket. Verified 2026-09-09 against ``DOCKER_HOST`` pointed at a dead port.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yml"

#: Pinned rather than inherited. Compose derives the project name from the
#: working directory, and a non-external network's resolved ``name`` carries
#: that prefix -- so an unpinned run makes this suite's verdict depend on what
#: the checkout directory happens to be called, which is how it behaved in a
#: worktree named after its branch before this line existed.
_PROJECT = "cartracker"

#: Both chains resolve through the same env file, so the diff below is caused
#: by ``docker-compose.ci.yml`` and never by an unset variable.
_ENV_FILE = ".env.ci"

_BASE = "docker-compose.yml"
_OVERRIDE = "docker-compose.ci.yml"

#: The five jobs that used to declare services of their own.
HEAVY_JOBS = (
    "dbt-models",
    "snapshot-dbt",
    "schema-contracts",
    "service-integration",
    "lake-integration",
)

#: Distinguishes "this key holds a different value" from "this key is not in
#: the other document at all", which are different kinds of divergence and
#: read differently in a failure.
ABSENT = "<absent>"


@dataclass(frozen=True)
class Divergence:
    """One resolved-config path on which CI is allowed to differ.

    ``why`` is the half a reader needs and the half no mechanism can supply.
    An entry whose ``why`` is "because it is" is an entry that should not be
    here -- the point of the allowlist is that every difference between CI and
    production has an argument attached to it.
    """

    path: str
    base: Any
    ci: Any
    why: str


DECLARED_DIVERGENCES = (
    Divergence(
        "networks.cartracker-net.external",
        True,
        ABSENT,
        "Production joins a network the VM created out of band, so that a "
        "`docker compose down` cannot take it with it. A CI runner has no "
        "such network and nothing to protect, so Compose creates it per job.",
    ),
    Divergence(
        "networks.cartracker-net.name",
        "cartracker-net",
        "cartracker_cartracker-net",
        "A consequence of the line above rather than a separate decision: "
        "Compose prefixes a network it owns with the project name and leaves "
        "an external one's name alone.",
    ),
    Divergence(
        "volumes.cartracker_pgdata.external",
        True,
        ABSENT,
        "The same move for the database volume. Note `postgres` itself has no "
        "override at all -- it still mounts `cartracker_pgdata` by name, so "
        "the service definition CI runs is production's exactly.",
    ),
    Divergence(
        "volumes.cartracker_pgdata.name",
        "cartracker_pgdata",
        "cartracker_pgdata_ci",
        "A safety rename rather than a style choice. An explicit `name:` is "
        "not project-scoped, so dropping `external` alone would have Compose "
        "create and destroy a volume called exactly `cartracker_pgdata` -- "
        "harmless on a runner, but on a developer machine running the real "
        "stack a `down -v` against this chain would delete their database.",
    ),
    Divergence(
        "services.minio.environment.MINIO_IDENTITY_OPENID_CONFIG_URL",
        "https://accounts.google.com/.well-known/openid-configuration",
        "",
        "Production's MinIO authenticates operators against Google; CI has no "
        "OAuth client. Verified 2026-09-09 that MinIO starts and reports "
        "healthy with this empty -- an empty provider URL disables the "
        "provider rather than half-configuring it.",
    ),
    Divergence(
        "services.minio.environment.MINIO_IDENTITY_OPENID_REDIRECT_URI",
        "https://cartracker.info/minio/oauth_callback",
        "",
        "Part of the OIDC set above; names a host that does not exist in CI.",
    ),
    Divergence(
        "services.minio.environment.MINIO_IDENTITY_OPENID_CLAIM_NAME",
        "email",
        "",
        "Part of the OIDC set above.",
    ),
    Divergence(
        "services.minio.environment.MINIO_IDENTITY_OPENID_SCOPES",
        "openid,email,profile",
        "",
        "Part of the OIDC set above.",
    ),
    Divergence(
        "services.minio.environment.MINIO_BROWSER_REDIRECT_URL",
        "https://cartracker.info/minio",
        "",
        "Points at a public host that does not exist on a runner. The console "
        "itself is kept -- `--console-address :9001` is production's command "
        "and CI runs it unmodified.",
    ),
    # `MINIO_IDENTITY_OPENID_CLIENT_ID` and `..._CLIENT_SECRET` are blanked by
    # the override too and are deliberately *not* entries here: they resolve
    # to '' on both sides, because their `${GOOGLE_CLIENT_*}` are secrets no
    # env file in the repository holds. Declaring them would fail this suite's
    # own staleness direction, which is the mechanism working rather than a
    # gap -- an entry may only exist for a difference that is really there.
)


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _resolve(*files: str) -> Dict[str, Any]:
    """The fully resolved Compose document for one chain of files."""
    command = ["docker", "compose", "-p", _PROJECT]
    for name in files:
        command += ["-f", name]
    command += ["--env-file", _ENV_FILE, "config", "--format", "json"]
    completed = subprocess.run(
        command,
        cwd=_REPO_ROOT,
        capture_output=True,
        # `text=True` decodes with the locale encoding, which is cp1252 on the
        # Windows half of this repository's development and utf-8 on the CI
        # runner. Plan 162 Stage J made that class of difference a rule.
        encoding="utf-8",
    )
    assert completed.returncode == 0, (
        f"`{' '.join(command)}` failed, so this suite cannot say anything "
        f"about parity:\n{completed.stderr}"
    )
    return json.loads(completed.stdout)


def _flatten(document: Any, prefix: str = "") -> Dict[str, Any]:
    """Every leaf of a resolved document, keyed by its dotted path.

    Lists are indexed rather than compared whole so that a failure names the
    element that moved instead of printing two long sequences and leaving the
    reader to find the difference.
    """
    flat: Dict[str, Any] = {}
    if isinstance(document, dict):
        for key, value in document.items():
            flat.update(_flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(document, list):
        for index, value in enumerate(document):
            flat.update(_flatten(value, f"{prefix}[{index}]"))
    else:
        flat[prefix] = document
    return flat


def _observed_differences() -> Dict[str, tuple]:
    base = _flatten(_resolve(_BASE))
    ci = _flatten(_resolve(_BASE, _OVERRIDE))
    return {
        path: (base.get(path, ABSENT), ci.get(path, ABSENT))
        for path in sorted(set(base) | set(ci))
        if base.get(path, ABSENT) != ci.get(path, ABSENT)
    }


requires_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="the docker CLI is not installed",
)


@requires_docker
def test_the_ci_override_is_the_whole_difference():
    """Every way CI's services differ from production's is declared here."""
    observed = _observed_differences()
    declared = {entry.path: (entry.base, entry.ci) for entry in DECLARED_DIVERGENCES}

    undeclared = sorted(path for path in observed if path not in declared)
    assert not undeclared, (
        "CI's Compose chain differs from production's at paths no entry in "
        "DECLARED_DIVERGENCES names. Either the override grew a difference "
        "nobody argued for, or the base file changed under it:\n"
        + "\n".join(
            f"    {path}\n        production: {observed[path][0]!r}"
            f"\n        ci:         {observed[path][1]!r}"
            for path in undeclared
        )
    )

    stale = sorted(path for path in declared if path not in observed)
    assert not stale, (
        "these paths are declared as differences and no longer differ. A "
        "declaration that has stopped being true is the drift this suite "
        "exists against, so delete the entries rather than the assertion:\n"
        + "\n".join(f"    {path}" for path in stale)
    )

    misdescribed = sorted(
        f"    {path}\n        declared:  {declared[path]!r}"
        f"\n        observed:  {observed[path]!r}"
        for path in observed
        if path in declared and observed[path] != declared[path]
    )
    assert not misdescribed, (
        "these paths still differ, but not in the way their entry describes. "
        "The same path diverging for a new reason is a new decision wearing "
        "an old declaration:\n" + "\n".join(misdescribed)
    )


@requires_docker
def test_ci_runs_productions_flyway_command():
    """The migration CI applies is the one production applies.

    Six hand-maintained argument lists is what this stage found, and the one
    they all omitted was `-baselineOnMigrate=true`. Asserting the resolved
    command rather than the file text is what makes this survive a future
    edit to how the base file writes it.
    """
    resolved = _resolve(_BASE, _OVERRIDE)
    command = resolved["services"]["flyway"]["command"]
    rendered = " ".join(command) if isinstance(command, list) else str(command)
    assert "-baselineOnMigrate=true" in rendered
    assert "-locations=filesystem:/flyway/sql" in rendered
    assert "-defaultSchema=public" in rendered


def _workflow() -> Dict[str, Any]:
    return yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))


def test_no_heavy_job_declares_its_own_services():
    """A bare `services:` image cannot come back unnoticed.

    This is the cheap half of the guard and the one that runs everywhere: it
    needs no Docker, because the defect it catches is visible in the workflow
    file. A job that declares its own `postgres:16` has left the Compose
    definition behind whatever the diff above says.
    """
    jobs = _workflow()["jobs"]
    offenders = sorted(job for job in HEAVY_JOBS if "services" in jobs.get(job, {}))
    assert not offenders, (
        f"these jobs declare their own `services:` block: {offenders}. CI's "
        f"services come from {_BASE} plus {_OVERRIDE}, started with `docker "
        "compose up`, so that the drift this file measures cannot reopen. "
        "Add the service to the Compose definitions instead."
    )


def test_every_heavy_job_starts_the_compose_services():
    """...and one that declares nothing actually starts something.

    Without this, `test_no_heavy_job_declares_its_own_services` passes on a
    job that deleted its services and starts nothing at all -- green because
    it asserts less, which is the failure mode this plan is named after.

    **Read per step, not per job, and that was a real bug rather than a
    tidy-up.** This first joined every `run:` in the job into one string and
    looked for the override file in it -- which the *Flyway* step also names,
    so deleting the `up` step entirely left this rule green. It was found by
    writing the mutation that was supposed to prove it, which is the argument
    the mutation harness exists to make.
    """
    jobs = _workflow()["jobs"]
    missing = []
    for job in HEAVY_JOBS:
        steps = jobs.get(job, {}).get("steps", [])
        starts = [
            step.get("run", "") or ""
            for step in steps
            if f"-f {_OVERRIDE}" in (step.get("run", "") or "")
            and "up -d" in (step.get("run", "") or "")
        ]
        if not starts:
            missing.append(job)
    assert not missing, (
        f"these jobs start no Compose services: {missing}. Every heavy job "
        f"brings up what it needs from {_BASE} plus {_OVERRIDE} in a step of "
        "its own -- naming the override in some other step is not starting "
        "anything."
    )
