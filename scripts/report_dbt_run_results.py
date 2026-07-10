"""Print a per-model timing table from a dbt run_results.json artifact.

Plan 123 Phase 5 profiling substrate: dbt writes target/run_results.json after
every invocation with each executed node's status and execution_time. This
script turns that into a readable table sorted by execution_time descending,
so VM verification steps can capture "which models are actually expensive"
evidence without hand-parsing JSON.

Usage examples:
  python scripts/report_dbt_run_results.py
  python scripts/report_dbt_run_results.py --path dbt/target/run_results.json
  python scripts/report_dbt_run_results.py --resource-type model --limit 10
  python scripts/report_dbt_run_results.py --json-out /tmp/model_timings.json
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class NodeTiming:
    unique_id: str
    resource_type: str
    name: str
    status: str
    execution_time: float


def load_run_results(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def extract_node_timings(run_results: dict[str, Any]) -> list[NodeTiming]:
    timings = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id") or ""
        resource_type = unique_id.split(".", 1)[0] if unique_id else ""
        timings.append(
            NodeTiming(
                unique_id=unique_id,
                resource_type=resource_type,
                name=unique_id.rsplit(".", 1)[-1] if unique_id else "",
                status=result.get("status") or "",
                execution_time=float(result.get("execution_time") or 0.0),
            )
        )
    return timings


def filter_and_sort(
    timings: list[NodeTiming],
    resource_type: str | None,
    limit: int | None,
) -> list[NodeTiming]:
    filtered = timings
    if resource_type:
        filtered = [t for t in filtered if t.resource_type == resource_type]
    filtered = sorted(filtered, key=lambda t: t.execution_time, reverse=True)
    if limit is not None:
        filtered = filtered[:limit]
    return filtered


def format_table(timings: list[NodeTiming]) -> str:
    if not timings:
        return "(no matching nodes)"
    name_width = max(len("name"), *(len(t.name) for t in timings))
    status_width = max(len("status"), *(len(t.status) for t in timings))
    header = f"{'name':<{name_width}}  {'status':<{status_width}}  execution_time"
    lines = [header, "-" * len(header)]
    for t in timings:
        lines.append(
            f"{t.name:<{name_width}}  {t.status:<{status_width}}  {t.execution_time:.3f}s"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--path",
        type=Path,
        default=Path("target/run_results.json"),
        help="Path to dbt's run_results.json (default: target/run_results.json)",
    )
    parser.add_argument(
        "--resource-type",
        default="model",
        help="Filter to this unique_id resource type (model, test, operation, "
        "seed, ...). Pass an empty string to include every node.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only show the N slowest nodes after filtering",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Also write the filtered/sorted timings as JSON to this path",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.path.exists():
        print(f"run_results.json not found: {args.path}", file=sys.stderr)
        return 1

    run_results = load_run_results(args.path)
    timings = extract_node_timings(run_results)
    resource_type = args.resource_type or None
    filtered = filter_and_sort(timings, resource_type, args.limit)

    print(format_table(filtered))
    elapsed_time = run_results.get("elapsed_time")
    if elapsed_time is not None:
        print(f"\ntotal elapsed_time: {elapsed_time:.3f}s")

    if args.json_out is not None:
        payload = [
            {
                "unique_id": t.unique_id,
                "name": t.name,
                "status": t.status,
                "execution_time": t.execution_time,
            }
            for t in filtered
        ]
        args.json_out.write_text(json.dumps(payload, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
