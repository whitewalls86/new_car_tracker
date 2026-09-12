"""Record the images on this CI runner, for the image provenance gate.

Plan 183 Stage D. Every job in ``.github/workflows/ci.yml`` runs this twice:
with ``--baseline`` straight after checkout, before anything can touch Docker,
and without it as an ``if: always()`` step at the end, which is uploaded as
``ci-image-records-<job>``. ``scripts/check_ci_image_provenance.py`` reads them
all.

**Why a baseline.** GitHub's runner image carries images of its own -- six on
``ubuntu-24.04`` 20260907.300.1, under ``ghcr.io/github`` and
``ghcr.io/dependabot`` -- and the gate's first run failed every job on them.
They are GitHub's, not this repository's dependencies, and they change when
GitHub says so. So the end record carries the ids of the images the runner
held before the job began, and the gate judges only what appeared since. An
end record with no baseline carries ``"baseline": null``, which the gate fails
rather than reading every image as new.

**Every job rather than the ones that "use Docker".** Three of them reach
Docker only from inside a Python script, so any reading of the workflow file
that decided which jobs owe a record would be blind to those. A job that never
touched Docker records only what was already there, which costs a second.

    python3 scripts/record_ci_images.py --baseline   # $CI_IMAGE_RECORDS/baseline-$GITHUB_JOB.json
    python3 scripts/record_ci_images.py              # $CI_IMAGE_RECORDS/images-$GITHUB_JOB.json

Standard library only, because it runs in jobs that install nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def _docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True, encoding="utf-8"
    ).stdout


def runner_images() -> list[dict]:
    """``{Id, RepoTags, RepoDigests}`` for every image the daemon holds.

    ``--all`` so that an intermediate image is not left out. It carries no
    digest, so the gate reads it as built here, which it was.
    """
    ids = sorted(set(_docker("image", "ls", "--all", "--quiet", "--no-trunc").split()))
    if not ids:
        return []
    return [
        {
            "Id": image["Id"],
            "RepoTags": image.get("RepoTags") or [],
            "RepoDigests": image.get("RepoDigests") or [],
        }
        for image in json.loads(_docker("image", "inspect", *ids))
    ]


def _write(path: Path, record: dict) -> None:
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="record what the runner held before the job began",
    )
    arguments = parser.parse_args(argv)

    job = os.environ["GITHUB_JOB"]
    directory = Path(os.environ.get("CI_IMAGE_RECORDS", "ci-image-records"))
    directory.mkdir(parents=True, exist_ok=True)
    images = runner_images()
    baseline_path = directory / f"baseline-{job}.json"

    if arguments.baseline:
        _write(baseline_path, {"job": job, "images": images})
        print(f"record_ci_images: {len(images)} image(s) before the job -> {baseline_path}")
    else:
        baseline = None
        if baseline_path.exists():
            before = json.loads(baseline_path.read_text(encoding="utf-8"))["images"]
            baseline = [image["Id"] for image in before]
        path = directory / f"images-{job}.json"
        _write(path, {"job": job, "baseline": baseline, "images": images})
        print(f"record_ci_images: {len(images)} image(s) on this runner -> {path}")
        if baseline is None:
            print("record_ci_images: no baseline was recorded; the gate will fail this job")
    for image in images:
        print(f"  {', '.join(image['RepoTags'] or image['RepoDigests']) or image['Id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
