"""Replay the Plan 141 fixture corpus through the real Promtail image.

``shared/log_ingestion_policy.classify_line`` exists to keep the fixture
tests and Promtail from drifting apart, but it is a Python model of a Go
pipeline and the unit tests only ever ask the model. Two divergences were
found by reading rather than by running: the model dropped an application
record with no ``logger`` that production retains, and it let an OAuth access
status win over a lifecycle severity that Promtail's stage order gives to the
lifecycle. Both were invisible because nothing executed the real pipeline.

This runs each fixture line through ``grafana/promtail:<compose tag>`` with
``-dry-run -stdin`` and asserts the labels Go produced are the labels the
corpus claims. Needs a Docker daemon, so it is a CI step rather than a test:
see the ``promtail-config`` job.

    python scripts/verify_promtail_contract.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROMTAIL_CONFIG = _REPO_ROOT / "promtail" / "promtail.yml"
_COMPOSE = _REPO_ROOT / "docker-compose.yml"
_FIXTURES = (
    _REPO_ROOT / "tests" / "fixtures" / "observability" / "plan_141_log_contract.json"
)

# Promtail's dry-run writer separates the label set from the line with a tab,
# but not reliably, so anchor on the label block itself.
_ENTRY = re.compile(r"^\S+\s+(\{[^}]*\})\t?(.*)$")
_LABEL = re.compile(r'(\w+)="([^"]*)"')

# Labels the corpus pins. `service` and `source` are inputs here because
# dry-run has no Docker metadata to relabel from.
_CHECKED_LABELS = ("level", "logger", "status")


def _promtail_image() -> str:
    compose = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    return compose["services"]["promtail"]["image"]


def _stages_for(config: dict, service: str, source: str) -> list:
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    if source == "application_file":
        return jobs[service]["pipeline_stages"]
    # `docker: {}` unwraps the container JSON envelope that stdin does not have.
    return [
        stage
        for stage in jobs["docker-operations"]["pipeline_stages"]
        if "docker" not in stage
    ]


def _run(image: str, config: dict, service: str, source: str, lines: list[str]) -> dict:
    document = {
        "server": {"http_listen_port": 0, "grpc_listen_port": 0},
        "positions": {"filename": "/tmp/positions.yaml"},
        "clients": [{"url": "http://127.0.0.1:3100/loki/api/v1/push"}],
        "scrape_configs": [
            {
                "job_name": "stdin",
                "static_configs": [
                    {
                        "targets": ["localhost"],
                        "labels": {"service": service, "source": source},
                    }
                ],
                "pipeline_stages": _stages_for(config, service, source),
            }
        ],
    }
    with tempfile.TemporaryDirectory() as workdir:
        path = Path(workdir) / "promtail.yml"
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        completed = subprocess.run(
            [
                "docker", "run", "--rm", "-i",
                "-v", f"{path.as_posix()}:/etc/promtail/stdin.yml:ro",
                "--entrypoint", "/usr/bin/promtail",
                image,
                "-config.file=/etc/promtail/stdin.yml",
                "-dry-run",
                "-stdin",
            ],
            input="\n".join(lines) + "\n",
            capture_output=True,
            text=True,
            check=True,
        )

    retained = {}
    for output_line in completed.stdout.splitlines():
        entry = _ENTRY.match(output_line)
        if entry is None:
            continue
        labels = dict(_LABEL.findall(entry.group(1)))
        if labels.get("service") != service:
            continue
        retained[entry.group(2).strip()] = labels
    return retained


def main() -> int:
    image = _promtail_image()
    config = yaml.safe_load(_PROMTAIL_CONFIG.read_text(encoding="utf-8"))
    cases = json.loads(_FIXTURES.read_text(encoding="utf-8"))["cases"]

    groups: dict[tuple[str, str], list[dict]] = {}
    for case in cases:
        groups.setdefault((case["service"], case["source_type"]), []).append(case)

    failures: list[str] = []
    for (service, source), group in sorted(groups.items()):
        retained = _run(image, config, service, source, [c["line"] for c in group])
        for case in group:
            expected = case["expected"]
            labels = retained.get(case["line"].strip())
            if expected["retained"] and labels is None:
                failures.append(
                    f"{case['name']}: corpus says retained, Promtail dropped it"
                )
                continue
            if not expected["retained"]:
                if labels is not None:
                    failures.append(
                        f"{case['name']}: corpus says dropped "
                        f"({expected.get('drop_reason')}), Promtail kept {labels}"
                    )
                continue
            for label in _CHECKED_LABELS:
                if labels.get(label) != expected.get(label):
                    failures.append(
                        f"{case['name']}: {label} is {labels.get(label)!r} in "
                        f"Promtail, {expected.get(label)!r} in the corpus"
                    )
        print(f"  {service:24s} {source:18s} {len(group):2d} lines replayed")

    if failures:
        print(f"\n{len(failures)} contract mismatch(es):", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print(f"\n{len(cases)} fixtures agree with {image}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
