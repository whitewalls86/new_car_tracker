"""Drift tests for Plan 142 mutation boundaries and drain evidence."""

import ast
from pathlib import Path

import yaml

from ops.coordination_contract import SURFACES
from ops.mutation_contract import (
    DRAIN_SOURCES,
    MUTATION_ROUTES,
    NON_HTTP_WORK,
    required_drain_sources,
)

REPO_ROOT = Path(__file__).parents[2]
ROUTE_ROOTS = ("ops", "archiver", "processing", "scraper", "dbt_runner")
MUTATION_METHODS = {"post", "put", "patch", "delete"}


def _route_functions() -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    found = {}
    for root in ROUTE_ROOTS:
        for path in (REPO_ROOT / root).rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for decorator in node.decorator_list:
                    if not (
                        isinstance(decorator, ast.Call)
                        and isinstance(decorator.func, ast.Attribute)
                        and decorator.func.attr in MUTATION_METHODS
                        and decorator.args
                        and isinstance(decorator.args[0], ast.Constant)
                        and isinstance(decorator.args[0].value, str)
                    ):
                        continue
                    relative = path.relative_to(REPO_ROOT).as_posix()
                    key = f"{relative}:{decorator.func.attr.upper()}:{decorator.args[0].value}"
                    assert key not in found, f"duplicate mutation route declaration: {key}"
                    found[key] = node
    return found


def test_every_fastapi_mutation_route_has_exactly_one_contract_and_none_are_stale():
    discovered = set(_route_functions())
    registered = set(MUTATION_ROUTES)

    assert registered == discovered, (
        f"missing={sorted(discovered - registered)}, "
        f"stale={sorted(registered - discovered)}. Every mutation boundary must "
        "select drain evidence or a reviewed short-operation reason."
    )


def test_every_contract_uses_known_surfaces_and_sufficient_evidence():
    for route, contract in MUTATION_ROUTES.items():
        assert contract.surfaces, route
        assert contract.surfaces <= SURFACES - {"host"}, route
        if contract.execution == "short_transaction":
            assert not contract.drain_sources, route
            assert len(contract.no_persistent_work_reason) >= 60, route
            continue

        assert contract.drain_sources, route
        assert not contract.no_persistent_work_reason, route
        unknown = contract.drain_sources - DRAIN_SOURCES.keys()
        assert not unknown, f"{route}: unknown drain sources {sorted(unknown)}"
        covered = frozenset().union(
            *(DRAIN_SOURCES[name].surfaces for name in contract.drain_sources)
        )
        assert contract.surfaces <= covered, (
            f"{route}: {sorted(contract.surfaces - covered)} have no drain evidence"
        )


def test_in_process_routes_actually_enter_the_shared_job_counter():
    route_functions = _route_functions()
    for route, contract in MUTATION_ROUTES.items():
        if "in_process" not in contract.execution:
            continue
        function = route_functions[route]
        path = REPO_ROOT / route.split(":", 1)[0]
        source = ast.get_source_segment(path.read_text(), function)
        assert "active_job" in source, (
            f"{route} claims in-process evidence but does not enter active_job()"
        )


def test_non_http_work_has_known_covering_sources_and_compose_targets_are_real():
    for name, contract in NON_HTTP_WORK.items():
        surfaces = contract["surfaces"]
        sources = contract["required_sources"]
        assert surfaces and surfaces <= SURFACES - {"host"}, name
        assert sources and sources <= DRAIN_SOURCES.keys(), name
        covered = frozenset().union(*(DRAIN_SOURCES[source].surfaces for source in sources))
        assert surfaces <= covered, name

    services = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())["services"]
    for service in ("trawl", "snapshot-worker", "pack-worker", "dbt", "dbt_test"):
        assert service in services


def test_every_drain_source_names_a_concrete_mechanism_and_nonempty_scope():
    for name, source in DRAIN_SOURCES.items():
        assert source.surfaces, name
        assert source.surfaces <= SURFACES - {"host"}, name
        assert len(source.mechanism) >= 30, name


def test_required_sources_follow_only_mutation_contracts_in_scope():
    processing = required_drain_sources({"processing"})

    assert "processing_artifacts" in processing
    assert "processing_jobs" in processing
    assert "airflow_task_instances" in processing
    assert "scraper_detail_jobs" not in processing
    assert "archiver_jobs" not in processing


def test_synchronous_trawl_work_is_nested_in_scraper_evidence_not_redis_state():
    assert "trawl_jobs" not in DRAIN_SOURCES
    assert "trawl_worker" not in NON_HTTP_WORK
    assert "scraper_detail_jobs" in required_drain_sources({"detail_fetch"})
