"""Layer 0. Plan 170 Stage C: the image keep-set, derived and rendered.

``docker image prune -a`` deletes every image no container references. On this
host four of those are deliberately not running -- Plan 125's paused lakekeeper
catalog, Plan 112 Gate B's MLflow server, and the profile-gated ``dbt`` tools
images -- and ``maintenance-running-set.txt`` says so in as many words. Docker
cannot read it. That gap is Plan 170's whole defect: **the safety information
exists and the tool doing the deleting cannot see it.**

So the keep-set is computed here, statically, and rendered into
``docs/runbooks/runbook_storage_maintenance.md`` for the operator holding the
prune command. Stage A measured that the join needs no Docker and no socket:
Compose derives every built image's name from ``project`` and ``service``, and
the manifest is keyed on exactly ``project/service``.

**The join runs image to services, never service to image.**
``cartracker-archiver`` is built by three services and ``cartracker-airflow`` by
four, so an image is reclaimable only when *no* service holding it needs it --
one running service protects the whole image, and one paused service protects it
against every running sibling.

**What lands in the block is what a plain ``docker compose up -d`` does not
materialise a container for**: another project, or profile-gated. Everything
else already has a container, which is the protection Docker itself can see and
does not need telling about. ``oneshot`` services are deliberately not in the
block for the same reason -- ``flyway`` runs to completion and leaves an exited
container behind, and an exited container is still a reference.

The rendered block is asserted against the derivation because an unchecked
rendering is the second copy this plan's design section forbids, and the Plan
138 projection is the precedent for one going stale in silence. There is no
generator script: when the assertion fails it prints the block to paste.
"""
import re
from pathlib import Path

import yaml

from tests.rules.test_maintenance_running_set import (
    DEFAULT_PROJECT,
    _split,
    load_registry,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNBOOK = _REPO_ROOT / "docs" / "runbooks" / "runbook_storage_maintenance.md"

# Which project each Compose file contributes services to. Overrides add to a
# project rather than declaring one, so several files share a value. Named
# rather than discovered because a project name is not recoverable from a
# filename, and `test_every_compose_file_is_attributed_to_a_project` is what
# stops a new file being skipped in silence.
COMPOSE_PROJECTS = {
    "docker-compose.yml": DEFAULT_PROJECT,
    "docker-compose.override.yml": DEFAULT_PROJECT,
    # Plan 162 Stage Q. An override of the default project's file rather than
    # a project of its own -- the same attribution `docker-compose.lakehouse
    # .ci.yml` gets below, for the same reason. It declares no `image:` and no
    # `build:`, so it contributes nothing to the index; it is here because
    # every `docker-compose*.yml` must be attributed to something.
    "docker-compose.ci.yml": DEFAULT_PROJECT,
    "docker-compose.test.yml": "cartracker-test",
    "docker-compose.lakehouse.yml": "cartracker-lakehouse",
    "docker-compose.lakehouse.a3.yml": "cartracker-lakehouse",
    "docker-compose.lakehouse.ci.yml": "cartracker-lakehouse",
    "docker-compose.lakehouse.local.yml": "cartracker-lakehouse",
    "docker-compose.mlflow.yml": "cartracker-mlflow",
}

# The classes whose services hold an image that no container will be holding.
# Plan 170's design: the keep-set is every container-referenced image plus every
# image belonging to a service in one of these. `profile-running` is in the list
# even though such a service is expected running -- `up -d` without its profile
# flag leaves it down, which is the 2026-08-14 solver outage, and a prune run in
# that window would take the image too.
PROTECTED_CLASSES = {"aux-paused", "on-demand", "profile-running", "aux-foreign"}

_SENTINEL = "# Derived from docker-compose*.yml and maintenance-running-set.txt"
_BLOCK_HEADER = (
    f"{_SENTINEL}\n"
    "# by tests/rules/test_image_keep_set.py, which asserts this block. Do not edit.\n"
)

# `${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}` -- the default is
# what this host runs, because none of these variables is set in `.env`.
_INTERPOLATED = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*:-(?P<default>[^}]*)\}$")


def _compose_files() -> list[Path]:
    return sorted(_REPO_ROOT.glob("docker-compose*.yml"))


def _services(path: Path) -> dict:
    return (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("services") or {}


def resolve_image_reference(reference: str) -> str:
    """The tag Docker stores an ``image:`` value under, as ``docker image ls``
    prints it: interpolation defaulted, and an implicit ``:latest`` made
    explicit so the rendered block can be matched by eye against the host."""
    interpolated = _INTERPOLATED.match(reference)
    if interpolated:
        reference = interpolated.group("default")
    if "@" in reference:
        return reference
    _, separator, tail = reference.rpartition(":")
    return reference if separator and "/" not in tail else f"{reference}:latest"


def image_index() -> dict[str, set[str]]:
    """``{image: {project/service, ...}}`` across every Compose file.

    A service with neither ``image:`` nor ``build:`` contributes nothing -- it
    is an override adding ports or volumes to a service the base file already
    named, and the base file is where its image comes from.
    """
    index: dict[str, set[str]] = {}
    for path in _compose_files():
        project = COMPOSE_PROJECTS[path.name]
        for service, spec in _services(path).items():
            spec = spec or {}
            if spec.get("image"):
                image = resolve_image_reference(spec["image"])
            elif spec.get("build"):
                # Compose names a built image `<project>-<service>` when the
                # service does not name one itself.
                image = f"{project}-{service}:latest"
            else:
                continue
            index.setdefault(image, set()).add(f"{project}/{service}")
    return index


def materialised_by_up() -> set[str]:
    """The ``project/service`` keys a plain ``docker compose up -d`` leaves a
    container behind for.

    Which is the default project, minus anything gated behind a ``profiles:``
    key. A container -- running or exited -- is a reference Docker can see, and
    the keep-set exists only for the images it cannot.
    """
    return {
        f"{DEFAULT_PROJECT}/{service}"
        for path in _compose_files()
        if COMPOSE_PROJECTS[path.name] == DEFAULT_PROJECT
        for service, spec in _services(path).items()
        if not (spec or {}).get("profiles")
    }


def keep_set() -> dict[str, set[str]]:
    """``{image: {project/service, ...}}`` for images no container will hold."""
    held = materialised_by_up()
    return {
        image: keys
        for image, keys in image_index().items()
        if not keys & held
    }


def classes_of(keys: set[str]) -> list[str]:
    registry = load_registry()
    found = set()
    for key in keys:
        project, service = _split(key)
        entry = registry.get(key) or (
            registry.get(service) if project == DEFAULT_PROJECT else None
        )
        found.add(entry[0] if entry else "UNCLASSIFIED")
    return sorted(found)


def render_block() -> str:
    """The fenced block's contents, exactly as the runbook must carry them."""
    lines = [_BLOCK_HEADER.rstrip("\n")]
    for image, keys in sorted(keep_set().items()):
        lines.append(f"{'/'.join(classes_of(keys)):<16}{image}")
    return "\n".join(lines) + "\n"


class TestTheDerivationHoldsItsShape:
    def test_every_compose_file_is_attributed_to_a_project(self):
        unattributed = {
            path.name for path in _compose_files() if path.name not in COMPOSE_PROJECTS
        }
        assert not unattributed, (
            f"{sorted(unattributed)} is a Compose file no project claims. Its "
            "services' images are therefore invisible to the keep-set, and a "
            "prune deletes them. Add it to COMPOSE_PROJECTS."
        )
        # The floor, and it is the assertion above run backwards. `unattributed`
        # is a set difference, so a glob that stopped matching passes it 0 of 0.
        # `>= 2` was what stood here, and it would have caught the glob dying
        # while missing it losing seven of nine. The exact answer is not a
        # number at all: COMPOSE_PROJECTS names every file, so every name in it
        # must be on disk. That also fails on the stale half nothing else asks
        # about -- an entry left behind for a Compose file that was deleted.
        missing = set(COMPOSE_PROJECTS) - {path.name for path in _compose_files()}
        assert not missing, (
            f"{sorted(missing)} is named by COMPOSE_PROJECTS and is not on "
            "disk. Either docker-compose*.yml has stopped matching -- in which "
            "case every rule in this file is deriving from nothing -- or the "
            "file was deleted and its entry should go with it."
        )

    def test_a_shared_image_is_joined_to_every_service_that_builds_it(self):
        """The hazard Stage A named: the Compose label records whichever service
        built the image last, so a label-to-class join would classify
        `cartracker-archiver` by `snapshot-worker` alone and reclaim an image
        two running services need."""
        index = image_index()
        assert index["cartracker-archiver:latest"] == {
            "cartracker/archiver",
            "cartracker/pack-worker",
            "cartracker/snapshot-worker",
        }
        # Five, not the four the plan named -- `airflow-init` declares the same
        # build as the four long-running services, and a join that counted only
        # the ones that stay up would still have got this image right for the
        # wrong reason.
        assert len(index["cartracker-airflow:latest"]) == 5

    def test_one_running_service_protects_a_shared_image(self):
        """`snapshot-worker` is `on-demand` and `archiver` is not, and the image
        they share stays out of the block -- a container holds it."""
        assert "cartracker-archiver:latest" not in keep_set()

    def test_interpolated_and_digest_references_resolve_to_what_the_host_shows(self):
        assert (
            resolve_image_reference("${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}")
            == "quay.io/lakekeeper/catalog:v0.13.1"
        )
        assert resolve_image_reference("cartracker-ops") == "cartracker-ops:latest"
        assert resolve_image_reference("postgres:16") == "postgres:16"
        assert (
            resolve_image_reference("ghcr.io/germondai/trawl@sha256:86b1fd")
            == "ghcr.io/germondai/trawl@sha256:86b1fd"
        )


class TestTheKeepSetProtectsWhatTheManifestNames:
    def test_every_entry_is_classified_by_the_manifest(self):
        unclassified = {
            image: sorted(keys)
            for image, keys in keep_set().items()
            if "UNCLASSIFIED" in classes_of(keys)
        }
        assert not unclassified, (
            f"{unclassified} is held only by services no manifest entry names. "
            "The keep-set can say the image is unreferenced but not why it is "
            "kept, so the operator holding the prune command has nothing to "
            "check it against. Name the service in "
            "maintenance-running-set.txt."
        )

    def test_every_entry_carries_a_protected_class(self):
        for image, keys in keep_set().items():
            for klass in classes_of(keys):
                assert klass in PROTECTED_CLASSES, (
                    f"{image} is held only by {sorted(keys)}, classed {klass!r}, "
                    f"which is not one of {sorted(PROTECTED_CLASSES)}. Either "
                    "the class is wrong or this image is reclaimable and the "
                    "keep-set is over-protecting."
                )

    def test_the_four_paused_and_on_demand_images_are_all_in_it(self):
        """The four Plan 170 measured as unreferenced-but-deliberate on the host
        on 2026-09-08. These are what `docker image prune -a` would take."""
        for image in (
            "cartracker-lakehouse:latest",
            "cartracker-mlflow:latest",
            "cartracker-dbt_test:latest",
            "quay.io/lakekeeper/catalog:v0.13.1",
        ):
            assert image in keep_set(), (
                f"{image} left the keep-set. It has no container on this host "
                "by decision -- Plan 125's pause, Plan 112 Gate B, or a "
                "profile-gated `compose run` target -- so nothing else stops a "
                "prune deleting it."
            )

    def test_the_third_party_catalog_is_carried_by_name(self):
        """`quay.io/lakekeeper/catalog` carries no Compose label and cannot be
        given one. It is in the block only because the manifest names its two
        services, which is the whole reason the derivation reads the manifest
        rather than the host's labels."""
        assert keep_set()["quay.io/lakekeeper/catalog:v0.13.1"] == {
            "cartracker-lakehouse/lakekeeper",
            "cartracker-lakehouse/lakekeeper-migrate",
        }

    def test_the_solver_and_its_redis_are_both_kept(self):
        """`profile-running`, so `up -d` alone leaves them down. A prune in that
        window takes the solver image and the 2026-08-14 outage comes back with
        a pull in front of it."""
        assert "redis:7-alpine" in keep_set()
        assert any(keys == {"cartracker/trawl"} for keys in keep_set().values())


class TestTheRunbookCarriesTheDerivation:
    def test_the_rendered_block_matches_the_derivation(self):
        runbook = _RUNBOOK.read_text(encoding="utf-8")
        expected = render_block()
        assert expected in runbook, (
            "The keep-set block in docs/runbooks/runbook_storage_maintenance.md "
            "no longer matches what docker-compose*.yml and "
            "maintenance-running-set.txt derive. A Compose service or a "
            "manifest class changed and the runbook still tells the operator "
            "the old answer. Replace the fenced block with:\n\n"
            f"{expected}"
        )

    def test_the_block_appears_exactly_once(self):
        """An edit that appends a fresh block instead of replacing the stale one
        satisfies the assertion above and leaves two answers in the runbook."""
        runbook = _RUNBOOK.read_text(encoding="utf-8")
        assert runbook.count(_SENTINEL) == 1, (
            f"the derived keep-set block appears {runbook.count(_SENTINEL)} "
            "times. The stale one is still there."
        )
