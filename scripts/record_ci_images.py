"""Record every image on this CI runner, for the image provenance gate.

Plan 183 Stage D. Every job in ``.github/workflows/ci.yml`` runs this as an
``if: always()`` step, whether or not it touched Docker, and uploads the result
as ``ci-image-records-<job>``; ``scripts/check_ci_image_provenance.py`` reads
them all. Every job rather than the ones that "use Docker" is deliberate: three
of them reach Docker only from inside a Python script, so any reading of the
workflow file that decided which jobs owe a record would be blind to those. A
job that never touched Docker records an empty list, which costs a second.

    python3 scripts/record_ci_images.py   # $CI_IMAGE_RECORDS/images-$GITHUB_JOB.json

Standard library only, because it runs in jobs that install nothing.
"""

from __future__ import annotations

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


def main() -> int:
    job = os.environ["GITHUB_JOB"]
    directory = Path(os.environ.get("CI_IMAGE_RECORDS", "ci-image-records"))
    directory.mkdir(parents=True, exist_ok=True)
    images = runner_images()
    path = directory / f"images-{job}.json"
    path.write_text(json.dumps({"job": job, "images": images}, indent=2) + "\n", encoding="utf-8")
    print(f"record_ci_images: {len(images)} image(s) on this runner -> {path}")
    for image in images:
        print(f"  {', '.join(image['RepoTags'] or image['RepoDigests']) or image['Id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
