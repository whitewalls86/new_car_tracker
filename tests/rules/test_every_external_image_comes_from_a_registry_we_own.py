"""Every image this repository does not build is one it owns, by digest.

Plan 183 Stage D.

**Pinning would not have saved MinIO; only owning a copy would have.** On
2026-09-11 Docker Hub deleted ``minio/minio`` -- the repository, not a tag --
and six heavy CI jobs died at image pull. A digest reference into that
repository returned 404 exactly as ``:latest`` did, because a digest names
bytes and says nothing about who keeps them. Plan 183 copied the build
production runs into ``ghcr.io/whitewalls86/minio`` and pinned every reference
to it; this rule is that lesson applied to every image the repository pulls
rather than builds.

**Owned means ``ghcr.io/whitewalls86/<name>`` and a digest.** The namespace is
the only one this repository can push to and nobody else can delete from, and
the digest is what stops the tag moving under it. Either alone is the bet that
failed: a digest into someone else's registry, or our registry under a tag.
``.github/workflows/vendor-image.yml`` makes the copy and fails unless the
copy's digest equals the source's, so each conversion is one dispatch and one
edited line.

**External means a service with ``image:`` and no ``build:``.** A service that
builds is this repository's code, whatever its image is named. A
``${X_IMAGE:-...}`` reference is read at its default, because the default is
what every host runs -- none of those variables is set in ``.env`` -- and it is
the same reading ``test_image_keep_set.py`` makes, through the same reader.

**A Dockerfile's base image is external too.** Every build pulls it, so a
deleted base image breaks every build of that service the way a deleted
``image:`` breaks every start. Each ``FROM`` line is read as written, skipping
``--platform=`` style flags and references to an earlier stage of the same
file. A ``FROM`` this reader cannot read -- split across lines, or interpolated
from an ``ARG`` -- is refused by the floor rather than guessed at.

**The ledger is keyed on the reference as written, and it can only shrink.**
17 compose references and 5 base images, measured 2026-09-12 once MinIO was
converted. Editing an
image line changes its key, so any stage that touches an image drains its entry
on the way through and the new reference must be owned. What nobody touches is
Plan 180 Stage K's to drain, and this rule files under that plan's seam 7 --
Compose as the whole truth about configuration.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path

import yaml

from tests.rules.test_image_keep_set import (
    _INTERPOLATED,
    _REPO_ROOT,
    _compose_files,
    _services,
)

OWNED = re.compile(
    r"^ghcr\.io/whitewalls86/[a-z0-9._/-]+"
    r"(:[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?"
    r"@sha256:[0-9a-f]{64}$"
)

_IMAGE_LINE = re.compile(r"^\s+image:\s*\S", re.MULTILINE)

# Instructions are case-insensitive. The group is optional so that a bare
# `FROM` still matches, and reaches the floor as unreadable.
_FROM = re.compile(r"^\s*FROM(?:\s+(?P<rest>.*))?$", re.IGNORECASE)

# Dot directories are skipped wholesale: `.claude/worktrees` holds copies of
# this repository whose Dockerfiles are not this checkout's.
_SKIPPED_DIRECTORIES = {"node_modules"}

_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yml"
_RECORDER = "scripts/record_ci_images.py"
_GATE = "scripts/check_ci_image_provenance.py"
_IMAGE_RECORD_ENV = "CI_IMAGE_RECORDS"
_IMAGE_RECORD_ARTIFACT = "ci-image-records-"

# Pulled, not built, and not yet copied into ghcr.io/whitewalls86/. Each drains
# the same way: dispatch vendor-image.yml with the source by digest, then point
# the reference at the copy and delete the entry here.
#
# `trawl` is already pinned by digest, and is here anyway: its repository
# belongs to another account, which is the MinIO failure with a digest attached.
UNOWNED_IMAGE_LEDGER: tuple[str, ...] = (
    "caddy:latest",
    "dpage/pgadmin4",
    "flyway/flyway:10-alpine",
    "ghcr.io/flaresolverr/flaresolverr:v3.4.6",
    "ghcr.io/germondai/trawl@sha256:86b1fdf26cfeebd996eeedaeb774002434f4cf8d02e5486f8a8173373c4d6e9b",
    "grafana/grafana:11.6.1",
    "grafana/loki:2.9.8",
    "grafana/promtail:3.5.8",
    "postgres:16",
    "prom/node-exporter:latest",
    "prom/prometheus:latest",
    "prom/statsd-exporter:latest",
    "prometheuscommunity/postgres-exporter:latest",
    "quay.io/lakekeeper/catalog:v0.13.1",
    "quay.io/oauth2-proxy/oauth2-proxy:latest",
    "redis:7-alpine",
    "tecnativa/docker-socket-proxy:0.3.0",
    # Dockerfile base images.
    "apache/airflow:3.2.0",
    "python:3.11-slim",
    "python:3.12-slim-bookworm",
    "python:3.13-slim",
    "python:3.13-slim-bookworm",
)


def external_images() -> dict[str, set[str]]:
    """``{reference: {file:service, ...}}`` for every service that pulls rather
    than builds, with interpolation read at its default."""
    found: dict[str, set[str]] = {}
    for path in _compose_files():
        for service, spec in _services(path).items():
            spec = spec or {}
            if not spec.get("image") or spec.get("build"):
                continue
            reference = spec["image"]
            interpolated = _INTERPOLATED.match(reference)
            if interpolated:
                reference = interpolated.group("default")
            found.setdefault(reference, set()).add(f"{path.name}:{service}")
    return found


def _dockerfiles() -> list[Path]:
    found: list[Path] = []
    for directory, subdirectories, files in os.walk(_REPO_ROOT):
        subdirectories[:] = [
            name
            for name in subdirectories
            if not name.startswith(".") and name not in _SKIPPED_DIRECTORIES
        ]
        found.extend(Path(directory) / name for name in files if name.startswith("Dockerfile"))
    return sorted(found)


def _from_lines(path: Path) -> Iterator[tuple[int, str | None]]:
    """``(line number, reference)`` for every ``FROM`` that pulls, where the
    reference is ``None`` if this line-by-line reader cannot read it."""
    stages: set[str] = set()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        match = _FROM.match(line)
        if not match:
            continue
        words = [word for word in (match.group("rest") or "").split() if not word.startswith("--")]
        reference = words[0] if words else ""
        is_stage = reference.lower() in stages
        if len(words) >= 3 and words[1].lower() == "as":
            stages.add(words[2].lower())
        if is_stage:
            continue
        readable = reference and reference != "\\" and "$" not in reference
        yield number, reference if readable else None


def base_images() -> dict[str, set[str]]:
    """``{reference: {Dockerfile:line, ...}}`` for every readable ``FROM`` that
    names an image rather than an earlier stage."""
    found: dict[str, set[str]] = {}
    for path in _dockerfiles():
        where = path.relative_to(_REPO_ROOT).as_posix()
        for number, reference in _from_lines(path):
            if reference is not None:
                found.setdefault(reference, set()).add(f"{where}:{number}")
    return found


def test_every_external_image_comes_from_a_registry_we_own():
    """An image someone else keeps is an image someone else can take away.

    Both directions: an unowned reference outside the ledger fails, and a
    ledger entry no compose file or Dockerfile names any more fails until it is
    deleted -- so the list can only shrink, and a conversion cannot leave its
    entry behind.
    """
    found = external_images()
    for reference, where in base_images().items():
        found.setdefault(reference, set()).update(where)
    unowned = {ref: where for ref, where in found.items() if not OWNED.match(ref)}
    ledgered = set(UNOWNED_IMAGE_LEDGER)

    unwaived = sorted(set(unowned) - ledgered)
    assert not unwaived, (
        "these images are pulled from a registry this repository does not own, "
        "and are not in the ledger:\n  "
        + "\n  ".join(f"{ref}  ({', '.join(sorted(unowned[ref]))})" for ref in unwaived)
        + "\n\nCopy the image with .github/workflows/vendor-image.yml (source by "
        "digest), then reference ghcr.io/whitewalls86/<name>:<tag>@sha256:<digest>. "
        "Adding a ledger entry instead is a decision, not a convenience."
    )

    stale = sorted(ledgered - set(unowned))
    assert not stale, (
        "these ledger entries no longer match an unowned image in any compose "
        "file or Dockerfile and must be deleted:\n  " + "\n  ".join(stale)
    )


def test_the_external_image_reader_is_not_blind():
    """A reader that misses a compose file passes the rule above on it.

    Derived, not counted: every compose file whose text carries an ``image:``
    line must be one the reader found an image in. A file whose services moved
    somewhere the reader does not look -- a renamed key, an ``include:``, an
    extension block -- keeps its ``image:`` lines and loses its place here.
    """
    with_image_lines = {
        path.name
        for path in _compose_files()
        if _IMAGE_LINE.search(path.read_text(encoding="utf-8"))
    }
    read = {
        path.name
        for path in _compose_files()
        if any((spec or {}).get("image") for spec in _services(path).values())
    }
    assert with_image_lines, "no compose file carries an image: line at all"
    unread = sorted(with_image_lines - read)
    assert not unread, (
        "these compose files carry image: lines the reader did not find in any "
        "service:\n  " + "\n  ".join(unread)
    )


def test_the_base_image_reader_is_not_blind():
    """A ``FROM`` the reader cannot read is a base image the rule never sees.

    The reader takes one line at a time and resolves no ``ARG``, so a ``FROM``
    split with a trailing backslash, or naming its image through ``$``, would
    drop out of the rule above in silence. Refused here instead: every ``FROM``
    that is not an earlier stage must carry its reference, whole, on its own
    line.
    """
    dockerfiles = _dockerfiles()
    assert dockerfiles, "no Dockerfile found under the repository"
    instructions = {
        f"{path.relative_to(_REPO_ROOT).as_posix()}:{number}": reference
        for path in dockerfiles
        for number, reference in _from_lines(path)
    }
    assert instructions, "no Dockerfile carries a FROM instruction at all"
    blind = sorted(where for where, reference in instructions.items() if reference is None)
    assert not blind, (
        "these FROM instructions carry no reference the reader can read -- "
        "split across lines, or interpolated from an ARG:\n  "
        + "\n  ".join(blind)
        + "\n\nWrite the image reference whole, on the FROM line itself."
    )


def test_every_job_leaves_an_image_record_the_gate_reads():
    """The rules above read what the repository says; the gate reads what the
    runners held. This holds the gate's inputs in place.

    **Every job owes a record, not every job that uses Docker.** Three jobs
    reach Docker only from inside a Python script, so a derivation of "uses
    Docker" from the workflow text would be blind to exactly the shape that
    pulled an untagged ``alpine``. The owing set is therefore every job but the
    gate, which is derived from the file and cannot go stale -- the same
    reasoning that makes ``test_every_job_that_runs_pytest_has_its_record_read_
    by_the_gate`` derive its set rather than list it.

    Both steps are ``if: always()``, because a failed job is the one most worth
    reading: it may have failed at a pull. The gate ``needs`` exactly the owing
    set, so it cannot start before a record is written and cannot wait on a job
    that is not there; it runs on ``always()`` so a red job does not skip it;
    and it is handed ``toJSON(needs)``, which is how it tells a job that ran
    and recorded nothing from one the path filter skipped.
    """
    document = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    assert _IMAGE_RECORD_ENV in (document.get("env") or {}), (
        f"{_WORKFLOW.name} no longer sets {_IMAGE_RECORD_ENV} at workflow level."
    )
    jobs = document["jobs"]

    gates = sorted(
        key
        for key, job in jobs.items()
        if any(_GATE in str(step.get("run", "")) for step in job.get("steps") or [])
    )
    assert len(gates) == 1, f"expected one job running {_GATE}, found {gates or 'none'}"
    gate_key = gates[0]
    gate = jobs[gate_key]
    owing = sorted(set(jobs) - {gate_key})
    assert owing, f"{_WORKFLOW.name} has no job but the gate"

    unrecorded = []
    for key in owing:
        steps = jobs[key].get("steps") or []
        recorded = [
            index
            for index, step in enumerate(steps)
            if _RECORDER in str(step.get("run", "")) and step.get("if") == "always()"
        ]
        uploaded = [
            index
            for index, step in enumerate(steps)
            if "upload-artifact" in str(step.get("uses", ""))
            and (step.get("with") or {}).get("name") == f"{_IMAGE_RECORD_ARTIFACT}{key}"
            and step.get("if") == "always()"
        ]
        if not (recorded and uploaded and recorded[0] < uploaded[-1]):
            unrecorded.append(key)
    assert not unrecorded, (
        f"these jobs do not leave an image record for {gate_key}: {unrecorded}. "
        f"Each job ends with an `if: always()` step running {_RECORDER}, then an "
        f"`if: always()` upload named {_IMAGE_RECORD_ARTIFACT}<job>. Without "
        "one, whatever that runner pulled is read by nobody."
    )

    needs = gate.get("needs") or []
    needs = [needs] if isinstance(needs, str) else needs
    unread = sorted(set(owing) - set(needs))
    phantom = sorted(set(needs) - set(owing))
    assert not unread and not phantom, (
        f"{gate_key} must need exactly every other job. Not waited on: {unread}. "
        f"Waited on but not a job: {phantom}."
    )
    assert str(gate.get("if", "")).startswith("always()"), (
        f"{gate_key} does not run on always(), so a failed job -- possibly one "
        "that failed at a pull -- skips the gate that would read its record."
    )
    gate_steps = gate.get("steps") or []
    assert any(
        _GATE in str(step.get("run", ""))
        and "toJSON(needs)" in str((step.get("env") or {}).get("NEEDS", ""))
        for step in gate_steps
    ), f"{gate_key} does not hand {_GATE} `NEEDS: ${{{{ toJSON(needs) }}}}`."
    assert any(
        "download-artifact" in str(step.get("uses", ""))
        and (step.get("with") or {}).get("pattern") == f"{_IMAGE_RECORD_ARTIFACT}*"
        for step in gate_steps
    ), f"{gate_key} does not download {_IMAGE_RECORD_ARTIFACT}*."
