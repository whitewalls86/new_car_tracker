"""Every image a CI runner held was built here, or is one this repository names.

Plan 183 Stage D. ``test_every_external_image_comes_from_a_registry_we_own``
reads what Compose and the Dockerfiles *say*. This reads what the runners
actually *held*, because a script can pull an image no compose file names: the
container_health contract pulled an untagged ``alpine`` from Docker Hub for as
long as it existed, and no rule reading Compose could have seen it. Each job
records its runner's images with ``scripts/record_ci_images.py``; this job
downloads every record and reads them together.

**Built here means no ``RepoDigests``.** A registry pull always records the
digest it came from, and an image ``docker compose build`` produced has none --
on the runner's classic image store, which the build log shows: ``writing image
sha256:... naming to docker.io/library/cartracker-processing``, and no
``unpacking to`` (checked 2026-09-12, run 34669832374). If the runner moves to
the containerd image store, built images gain digests and this fails on the
first of them, naming a ``cartracker-*`` image: loud, and the safe direction to
be wrong in. ``docker load`` and ``docker commit`` also make digest-less images;
CI uses neither.

**Pulled means one of the references this repository defines**, read through
the rule's own readers rather than a second parser -- the same way
``check_sql_execution_coverage.py`` reads ``production_sql_files()``. That
rule holds each of those references owned or ledgered, so a pulled image
outside them is both unowned and named nowhere in Compose, and this one check
answers both halves of the stage's exit. Matched after normalising both sides:
``docker.io/library/`` and ``:latest`` made explicit, and a tag dropped where a
digest is present, because Docker pulls by the digest and ignores the tag.

**Every job that ran owes a record.** Which ran comes from ``toJSON(needs)``,
so a job skipped by the path filter owes nothing and a job that ran and left
no record fails here, rather than reading as a runner that pulled nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tests.rules.test_every_external_image_comes_from_a_registry_we_own import (  # noqa: E402
    base_images,
    external_images,
)

_DOCKER_HUB = "docker.io"


def normalise(reference: str) -> str:
    """The reference as its registry knows it."""
    name, _, digest = reference.partition("@")
    slash, colon = name.rfind("/"), name.rfind(":")
    repository, tag = (name[:colon], name[colon + 1 :]) if colon > slash else (name, "latest")
    first, _, rest = repository.partition("/")
    if not rest:
        repository = f"{_DOCKER_HUB}/library/{repository}"
    elif "." not in first and ":" not in first and first != "localhost":
        repository = f"{_DOCKER_HUB}/{repository}"
    return f"{repository}@{digest}" if digest else f"{repository}:{tag}"


def defined_references() -> set[str]:
    """Every image Compose pulls and every Dockerfile base image, normalised."""
    return {normalise(reference) for reference in (*external_images(), *base_images())}


def unaccounted(images: list[dict], defined: set[str]) -> list[dict]:
    """Images pulled from a registry under no reference this repository defines."""
    return [
        image
        for image in images
        if image.get("RepoDigests")
        and not {
            normalise(reference)
            for reference in (*(image.get("RepoTags") or []), *image["RepoDigests"])
        }
        & defined
    ]


def missing_records(needs: dict, recorded: set[str]) -> list[str]:
    """Jobs that ran and left no record. A skipped job pulled nothing."""
    return sorted(
        job
        for job, outcome in needs.items()
        if outcome.get("result") != "skipped" and job not in recorded
    )


def load_records(directory: Path) -> dict[str, list[dict]]:
    """``{job: images}``. ``download-artifact`` puts each artifact in its own
    subdirectory, so the files are found wherever they landed."""
    records: dict[str, list[dict]] = {}
    for path in sorted(directory.rglob("images-*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        records[record["job"]] = record["images"]
    return records


def _label(image: dict) -> str:
    return ", ".join(image.get("RepoTags") or image.get("RepoDigests") or [image["Id"]])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--records", type=Path, default=REPO_ROOT / "ci-image-records")
    arguments = parser.parse_args(argv)

    raw_needs = os.environ.get("NEEDS")
    if not raw_needs:
        print(
            "FAIL: NEEDS is not set. The gate job passes `toJSON(needs)` so that a "
            "job which ran and recorded nothing can be told apart from one that "
            "was skipped; without it that question has no answer."
        )
        return 1
    needs = json.loads(raw_needs)
    records = load_records(arguments.records)
    defined = defined_references()

    failed = False
    for job in sorted(records):
        images = records[job]
        built = sum(1 for image in images if not image.get("RepoDigests"))
        print(f"{job}: {len(images)} image(s), {built} built here, {len(images) - built} pulled")
        for image in images:
            if image.get("RepoDigests"):
                print(f"    pulled  {_label(image)}")

    missing = missing_records(needs, set(records))
    if missing:
        print(
            f"\nFAIL: {len(missing)} job(s) ran and left no image record: "
            f"{', '.join(missing)}. Each job runs scripts/record_ci_images.py with "
            "`if: always()` and uploads ci-image-records-<job>; a job that did not "
            "is a runner whose pulls nobody read."
        )
        failed = True

    for job in sorted(records):
        stray = unaccounted(records[job], defined)
        if stray:
            print(
                f"\nFAIL: {job} held image(s) pulled from a registry under no "
                "reference this repository defines:"
            )
            for image in stray:
                print(f"    {_label(image)}")
            failed = True
    if failed and any(unaccounted(images, defined) for images in records.values()):
        print(
            "\nCompose is the one place an image is defined. Read the image from its "
            "Compose service rather than naming it in a script or a workflow step, "
            "and if it is a new dependency, add it to Compose -- where "
            "test_every_external_image_comes_from_a_registry_we_own asks whether "
            "it is owned."
        )

    if not failed:
        print(f"\nok: {len(records)} record(s), every pulled image is one this repository names")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
