"""No call site retypes a status the envelope declares.

Plan 162 Stage AL, gap G33 — the pair's other half.

**``_codes_at_call_sites`` now resolves a raised declared refusal through
``shared/api_envelope.py``; this rule is the reverse direction, without which
the lookup is optional.** A bare literal that is a *member* of the envelope's
declared refusal codes is the same defect
``test_no_module_retypes_a_database_vocabulary_it_could_import`` closes one
seam down: the declaration exists, the call site retypes it, and a change to
the declaration leaves the call site behind — silently, because the old
literal goes on being an int.

**Membership is the scope, exactly as Stage W drew it.** The envelope
declares refusal codes; a ``303`` on a ``RedirectResponse`` call or a ``201``
on a decorator is not a member and must not be touched — Stage W's rule left
``ok`` and ``unknown`` alone for the same reason. Decorator keywords are
declarations, not call sites, and are excluded; so is
``container_health/api_envelope.py``, the one permitted copy of the literals
themselves; so is ``airflow/dags``, where the import that would remove the
copy is impossible and the DAG-vocabulary rules own the copies instead. A
caller-side comparison (``resp.status_code == 503``) counts only in modules
that reach a service this repository owns, because a ``403`` compared
against cars.com's answer belongs to the external-vocabulary census, not to
this envelope.

**An entry is "not yet converted."** The 2026-09-10 measurement recorded 128
literals; the reconciliation to this ledger's corpus is in plan_162 §Record
— the delta is decorator declarations, non-member success codes, and
``shared/minio.py``'s four S3 *string* codes, each excluded here on the
scope argument above, plus ``processing``'s three sites the recorded split
missed.
"""
from __future__ import annotations

import ast

from tests.rules.test_testing_contract import (
    REPO_ROOT,
    _envelope_codes,
    service_packages,
)
from tests.service_contracts import caller_endpoints


def _retyped_in(
    tree: ast.AST, members: set[int], comparisons: bool
) -> set[tuple[str, int]]:
    """``(enclosing function, code)`` for every retyping shape in *tree*.

    One walker for the corpus and the canary floor, so the floor exercises
    the reader it guards rather than a second copy that could stay sharp
    while this one dulled.
    """
    enclosing: dict[int, str] = {}
    decorator_nodes: set[int] = set()
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(function):
                enclosing.setdefault(id(node), function.name)
            for decorator in function.decorator_list:
                for node in ast.walk(decorator):
                    decorator_nodes.add(id(node))
    found: set[tuple[str, int]] = set()
    for node in ast.walk(tree):
        where = enclosing.get(id(node), "<module>")
        if isinstance(node, ast.Call) and id(node) not in decorator_nodes:
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            for keyword in node.keywords:
                if (
                    keyword.arg == "status_code"
                    and isinstance(keyword.value, ast.Constant)
                    and keyword.value.value in members
                ):
                    found.add((where, keyword.value.value))
            if (
                name == "HTTPException"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value in members
            ):
                found.add((where, node.args[0].value))
        if (
            isinstance(node, ast.Compare)
            and comparisons
            and ast.unparse(node.left).endswith(".status_code")
        ):
            for comparator in node.comparators:
                if (
                    isinstance(comparator, ast.Constant)
                    and comparator.value in members
                ):
                    found.add((where, comparator.value))
    return found


def retyped_statuses() -> set[str]:
    """Every ``file::function::code`` where a call site retypes a member."""
    members = set(_envelope_codes().values())
    found: set[str] = set()
    for package in sorted(service_packages()):
        for path in sorted((REPO_ROOT / package).rglob("*.py")):
            if "__pycache__" in path.parts or path.name == "api_envelope.py":
                continue
            relative = path.relative_to(REPO_ROOT).as_posix()
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            found |= {
                f"{relative}::{where}::{code}"
                for where, code in _retyped_in(
                    tree, members, comparisons=bool(caller_endpoints(relative))
                )
            }
    return found


# Keyed on file-plus-function-plus-code, never a line number. Seeded
# 2026-09-10 at 80 keys covering 101 literal sites (several sites in one
# function share a code and a key, which is the grain the repair happens
# at). Draining an entry is conversion: the handler raises the envelope
# member, the caller compares against it, and the literal leaves the call
# site.
RETYPED_STATUS_LEDGER: tuple[str, ...] = (
    "archiver/app.py::_require_disk_usage_host_mounts::409",
    "archiver/app.py::_require_pack_worker::409",
    "archiver/app.py::_single_flight_or_409::409",
    "archiver/app.py::ready::503",
    "archiver/app.py::trigger_compact_silver::500",
    "archiver/app.py::trigger_disk_usage::500",
    # Landed on master (Plan 134 Stage C deploy 2) between this ledger's
    # measurement and its rebase; the same class as its trigger_* siblings.
    "archiver/app.py::trigger_flush_staging::500",
    "archiver/app.py::trigger_pack_bronze_html::400",
    "archiver/app.py::trigger_pack_bronze_html::500",
    "archiver/app.py::trigger_prune_packed_source_html::400",
    "archiver/app.py::trigger_prune_packed_source_html::500",
    "archiver/app.py::trigger_snapshot_export::400",
    "archiver/app.py::trigger_snapshot_export::409",
    "archiver/app.py::trigger_verify_pack_read_path::400",
    "archiver/app.py::trigger_verify_pack_read_path::500",
    "dbt_runner/app.py::_validate_tokens::400",
    "dbt_runner/app.py::dbt_build::400",
    "dbt_runner/app.py::dbt_build::409",
    "dbt_runner/app.py::dbt_build::500",
    "dbt_runner/app.py::dbt_docs_generate::500",
    "dbt_runner/app.py::ready::503",
    "ops/app.py::observer_readonly::403",
    "ops/routers/admin.py::_db_error_response::503",
    "ops/routers/admin.py::_deploy_refusal::409",
    "ops/routers/admin.py::_deploy_refusal::500",
    "ops/routers/admin.py::_deploy_refusal::503",
    "ops/routers/admin.py::_fetch_dbt_context::503",
    "ops/routers/admin.py::_not_found_response::404",
    "ops/routers/admin.py::create_search::422",
    "ops/routers/admin.py::update_search::422",
    "ops/routers/auth.py::auth_check::403",
    "ops/routers/auth.py::auth_check::503",
    "ops/routers/coordination.py::_status::503",
    "ops/routers/coordination.py::authorize_coordination::409",
    "ops/routers/coordination.py::authorize_coordination::503",
    "ops/routers/coordination.py::begin_coordination_drain::409",
    "ops/routers/coordination.py::begin_coordination_drain::503",
    "ops/routers/coordination.py::begin_coordination_validation::409",
    "ops/routers/coordination.py::begin_coordination_validation::503",
    "ops/routers/coordination.py::cancel_coordination::409",
    "ops/routers/coordination.py::cancel_coordination::503",
    "ops/routers/coordination.py::complete_coordination::409",
    "ops/routers/coordination.py::complete_coordination::503",
    "ops/routers/coordination.py::request_coordination::409",
    "ops/routers/coordination.py::request_coordination::422",
    "ops/routers/coordination.py::request_coordination::500",
    "ops/routers/coordination.py::request_coordination::503",
    "ops/routers/coordination.py::submit_host_evidence::409",
    "ops/routers/coordination.py::submit_host_evidence::422",
    "ops/routers/coordination.py::submit_host_evidence::503",
    "ops/routers/deploy.py::complete_deployment::409",
    "ops/routers/deploy.py::complete_deployment::500",
    "ops/routers/deploy.py::complete_deployment::503",
    "ops/routers/deploy.py::start_deploy_intent::409",
    "ops/routers/deploy.py::start_deploy_intent::422",
    "ops/routers/deploy.py::start_deploy_intent::500",
    "ops/routers/deploy.py::start_deploy_intent::503",
    "ops/routers/public.py::recap_index::404",
    "ops/routers/public.py::recap_page::404",
    "ops/routers/snapshots.py::_manifest_for_alias::404",
    "ops/routers/snapshots.py::_manifest_for_alias::409",
    "ops/routers/snapshots.py::_resolve_alias::404",
    "ops/routers/snapshots.py::_validate_snapshot_id::400",
    "ops/routers/snapshots.py::_validated_prefixed_key::404",
    "ops/routers/snapshots.py::download_snapshot_archive::404",
    "ops/routers/snapshots.py::get_latest_snapshot::404",
    "ops/routers/snapshots.py::get_snapshot_manifest::404",
    "ops/routers/snapshots.py::require_snapshot_token::401",
    "ops/routers/snapshots.py::require_snapshot_token::403",
    "ops/routers/snapshots.py::require_snapshot_token::503",
    "ops/routers/users.py::_db_error_response::503",
    "ops/routers/users.py::_not_found_response::404",
    "ops/routers/users.py::change_user_role::400",
    "ops/routers/users.py::submit_access_request::400",
    "ops/routers/users.py::submit_access_request::503",
    "processing/app.py::ready::503",
    "processing/routers/artifact.py::process_single_artifact::404",
    "processing/routers/batch.py::process_batch::503",
    "scraper/app.py::mark_job_fetched::404",
    "scraper/app.py::ready::503",
    "scraper/app.py::scrape_detail_batch_endpoint::400",
)


def test_the_retyping_reader_sees_every_shape():
    """The floor: a canary carrying all three shapes the reader must find.

    The predicate is string-and-membership comparison, which can narrow
    without anything going red — the same reason the SQL detector and the
    mock-object rule carry canaries. The membership guard comes first,
    because a canary using codes the envelope no longer declares would fail
    for the wrong reason.
    """
    members = set(_envelope_codes().values())
    assert {404, 409, 503} <= members, (
        "the envelope no longer declares the codes this canary uses; move "
        "the canary with the vocabulary rather than deleting it."
    )
    canary = ast.parse(
        "def handler(x):\n"
        "    if x.status_code == 503:\n"
        "        raise HTTPException(404, detail='gone')\n"
        "    return JSONResponse({}, status_code=409)\n"
    )
    found = {
        code for _where, code in _retyped_in(canary, members, comparisons=True)
    }
    assert found == {404, 409, 503}, (
        f"the retyping shapes found {sorted(found)} of the canary's three; "
        f"a shape this file has stopped seeing goes unread everywhere else "
        f"too, silently."
    )


def test_no_call_site_retypes_a_status_the_envelope_declares():
    """The declaration exists; a bare member literal beside it is a copy.

    Both directions: a new retyped literal fails on arrival, and a
    converted one fails until its entry is deleted — so the ledger is a
    work list that can only shrink, and a reader gone blind strands all 80
    entries loudly.
    """
    found = retyped_statuses()
    ledgered = set(RETYPED_STATUS_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these call sites retype a status code the envelope declares:\n  "
        + "\n  ".join(unwaived)
        + "\n\nRaise the declared refusal from shared/api_envelope.py (or "
        "compare against its member) instead of retyping the literal. "
        "Adding a ledger entry is a decision, not a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries no longer describe a retyped literal and "
        "must be deleted:\n  " + "\n  ".join(stale)
    )
