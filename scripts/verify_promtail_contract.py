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

Plan 160: the checker has three states in reality -- retained, dropped, and
*not observed* -- and used to encode only two, so an entry lost to Promtail's
shutdown race read as a log-contract violation. Two things fix that. The
replay holds stdin open until the entries it expects have arrived, because
``-stdin`` treats EOF as "shut down now" and the old ``input=`` handed
Promtail its work and told it to exit in the same breath. And an observation
that is still short after three attempts reports **inconclusive** rather than
"Promtail dropped it": a real regression loses the same line every time, a
race does not.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, NamedTuple

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROMTAIL_CONFIG = _REPO_ROOT / "promtail" / "promtail.yml"
_COMPOSE = _REPO_ROOT / "docker-compose.yml"
_FIXTURES = (
    _REPO_ROOT / "tests" / "fixtures" / "observability" / "plan_141_log_contract.json"
)

# Promtail's dry-run writer separates the label set from the line with a tab,
# but not reliably, so anchor on the label block itself. This also skips the
# 15-line client-config banner Promtail prints before any entry.
_ENTRY = re.compile(r"^\S+\s+(\{[^}]*\})\t?(.*)$")
_LABEL = re.compile(r'(\w+)="([^"]*)"')

# Labels the corpus pins. `service` and `source` are inputs here because
# dry-run has no Docker metadata to relabel from.
_CHECKED_LABELS = ("level", "logger", "status")

# How long to hold stdin open waiting for the entries a batch must produce.
# Generous because it is only ever paid in full when something is wrong.
_DEADLINE = 30.0
# A batch that expects *zero* retained entries has no positive signal to wait
# for, so it is the one case that must ride a deadline instead of a count.
_QUIET_GRACE = 3.0
# A beat after the expected count arrives, in case a line the corpus says is
# dropped is in fact being retained just behind it.
_SETTLE = 0.25
_POLL = 0.05
_EXIT_TIMEOUT = 30.0

_MAX_ATTEMPTS = 3


class Observation(NamedTuple):
    """What one replay of a batch saw, and whether it saw all of it."""

    retained: dict[str, dict[str, str]]
    complete: bool


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


def _parse_entries(output_lines, service: str) -> dict[str, dict[str, str]]:
    """Dry-run stdout to ``{line: labels}``, ignoring the config banner."""
    retained: dict[str, dict[str, str]] = {}
    for output_line in output_lines:
        entry = _ENTRY.match(output_line.rstrip("\n"))
        if entry is None:
            continue
        labels = dict(_LABEL.findall(entry.group(1)))
        if labels.get("service") != service:
            continue
        retained[entry.group(2).strip()] = labels
    return retained


def _config_document(config: dict, service: str, source: str) -> dict:
    return {
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


def _run(
    image: str,
    config: dict,
    service: str,
    source: str,
    lines: list[str],
    expected_count: int,
) -> Observation:
    """Replay one batch, holding stdin open until the entries arrive.

    The old ``subprocess.run(input=...)`` wrote the lines and immediately
    closed stdin, and ``-stdin`` mode reads EOF as "shut down now" -- so the
    entries still moving through Promtail's pipeline when shutdown won were
    never written to stdout at all. Waiting *after* the call cannot help:
    ``run`` already blocks until the process exits and drains the pipe. The
    waiting has to happen before EOF, which is what this does.
    """
    document = _config_document(config, service, source)
    with tempfile.TemporaryDirectory() as workdir:
        path = Path(workdir) / "promtail.yml"
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        process = subprocess.Popen(
            [
                "docker", "run", "--rm", "-i",
                "-v", f"{path.as_posix()}:/etc/promtail/stdin.yml:ro",
                "--entrypoint", "/usr/bin/promtail",
                image,
                "-config.file=/etc/promtail/stdin.yml",
                "-dry-run",
                "-stdin",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            encoding="utf-8",
        )
        collected: list[str] = []
        lock = threading.Lock()

        def _drain() -> None:
            for output_line in process.stdout:
                with lock:
                    collected.append(output_line)

        reader = threading.Thread(target=_drain, daemon=True)
        reader.start()

        def _seen() -> int:
            with lock:
                snapshot = list(collected)
            return len(_parse_entries(snapshot, service))

        try:
            process.stdin.write("\n".join(lines) + "\n")
            process.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass

        complete = expected_count == 0
        deadline = time.monotonic() + (
            _QUIET_GRACE if expected_count == 0 else _DEADLINE
        )
        while time.monotonic() < deadline:
            if expected_count and _seen() >= expected_count:
                complete = True
                time.sleep(_SETTLE)
                break
            if process.poll() is not None:
                break
            time.sleep(_POLL)

        if not process.stdin.closed:
            process.stdin.close()
        killed = False
        try:
            process.wait(timeout=_EXIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            killed = True
            complete = False
        reader.join(timeout=_EXIT_TIMEOUT)
        stderr = process.stderr.read()
        process.stderr.close()

    # A container we had to kill is an incomplete observation, which is a
    # verdict the checker can now express -- so it reports one rather than
    # dying. A non-zero exit we did *not* cause is a broken harness (a config
    # Promtail will not load, no daemon), and that must never be laundered
    # into "inconclusive".
    if process.returncode and not killed:
        raise RuntimeError(
            f"promtail exited {process.returncode} for {service}/{source}:\n{stderr}"
        )

    with lock:
        snapshot = list(collected)
    return Observation(_parse_entries(snapshot, service), complete)


def _check_group(
    service: str,
    group: list[dict],
    runner: Callable[[list[str], int], Observation],
) -> tuple[list[str], list[str], int]:
    """Verdict for one batch: ``(failures, inconclusive, attempts_used)``.

    Retries a short observation because that is the whole disambiguation: a
    genuine contract regression is deterministic and loses the same line every
    attempt, a race is not.
    """
    expected_retained = [case for case in group if case["expected"]["retained"]]
    expected_count = len(expected_retained)
    lines = [case["line"] for case in group]

    attempts: list[dict[str, dict[str, str]]] = []
    missing_per_attempt: list[set[str]] = []
    for _ in range(_MAX_ATTEMPTS):
        observation = runner(lines, expected_count)
        attempts.append(observation.retained)
        missing_per_attempt.append(
            {
                case["name"]
                for case in expected_retained
                if case["line"].strip() not in observation.retained
            }
        )
        if observation.complete and not missing_per_attempt[-1]:
            break

    # The race only ever loses entries, so the best available observation of
    # the batch is the union of what every attempt saw.
    merged: dict[str, dict[str, str]] = {}
    for retained in attempts:
        merged.update(retained)

    always_missing = set.intersection(*missing_per_attempt)
    sometimes_missing = set.union(*missing_per_attempt) - always_missing

    failures: list[str] = []
    inconclusive: list[str] = []
    for case in group:
        expected = case["expected"]
        labels = merged.get(case["line"].strip())

        if not expected["retained"]:
            # Retention is positive evidence and the race cannot invent it,
            # so this direction is never ambiguous.
            if labels is not None:
                failures.append(
                    f"{case['name']}: corpus says dropped "
                    f"({expected.get('drop_reason')}), Promtail kept {labels}"
                )
            continue

        if case["name"] in always_missing:
            failures.append(
                f"{case['name']}: corpus says retained, Promtail dropped it "
                f"on all {len(attempts)} attempts"
            )
            continue

        if case["name"] in sometimes_missing:
            seen_on = sum(
                1 for missing in missing_per_attempt if case["name"] not in missing
            )
            inconclusive.append(
                f"{service}/{case['name']}: not observed on "
                f"{len(attempts) - seen_on} of {len(attempts)} attempts, retained "
                f"on {seen_on} -- the checker lost it, the pipeline did not drop it"
            )

        if labels is None:
            continue
        for label in _CHECKED_LABELS:
            if labels.get(label) != expected.get(label):
                failures.append(
                    f"{case['name']}: {label} is {labels.get(label)!r} in "
                    f"Promtail, {expected.get(label)!r} in the corpus"
                )

    return failures, inconclusive, len(attempts)


def main() -> int:
    image = _promtail_image()
    config = yaml.safe_load(_PROMTAIL_CONFIG.read_text(encoding="utf-8"))
    cases = json.loads(_FIXTURES.read_text(encoding="utf-8"))["cases"]

    groups: dict[tuple[str, str], list[dict]] = {}
    for case in cases:
        groups.setdefault((case["service"], case["source_type"]), []).append(case)

    failures: list[str] = []
    inconclusive: list[str] = []
    for (service, source), group in sorted(groups.items()):

        def runner(
            lines: list[str],
            expected_count: int,
            _service: str = service,
            _source: str = source,
        ) -> Observation:
            return _run(image, config, _service, _source, lines, expected_count)

        group_failures, group_inconclusive, attempts = _check_group(
            service, group, runner
        )
        failures.extend(group_failures)
        inconclusive.extend(group_inconclusive)
        retried = f"  ({attempts} attempts -- the replay came up short)" if attempts > 1 else ""
        print(f"  {service:24s} {source:18s} {len(group):2d} lines replayed{retried}")

    if inconclusive:
        # Not a contract violation, and deliberately not an exit code. It is
        # printed so the race stops being invisible and its real frequency
        # starts being recorded.
        print(
            f"\nINCONCLUSIVE: {len(inconclusive)} entr"
            f"{'y' if len(inconclusive) == 1 else 'ies'} the checker could not "
            "observe. This is an incomplete observation, not a contract "
            "violation:",
            file=sys.stderr,
        )
        for note in inconclusive:
            print(f"  - {note}", file=sys.stderr)

    if failures:
        print(f"\n{len(failures)} contract mismatch(es):", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    verdict = f"\n{len(cases)} fixtures agree with {image}."
    if inconclusive:
        verdict += " No mismatch found, but the observation was incomplete."
    print(verdict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
