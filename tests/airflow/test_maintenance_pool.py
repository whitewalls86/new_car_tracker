"""
Plan 142 Stage 0 item 3, Phase A: the maintenance pool assignment.

Phase A ships the pool attribute *inert* — the pool is created with more slots
than there are pooled tasks, so nothing about scheduling changes until an
operator shrinks it. That makes it a normal deploy rather than a window, but it
also makes it invisible: nothing observable would notice if the assignment
drifted off a mutating task, or crept onto a sensor, until a window depended on
it. These tests are what notices.

They read the DAG source with `ast` rather than building a DagBag, so they run
in the ordinary suite without Airflow installed. The DagBag equivalent lives in
tests/integration/airflow/test_dag_integrity.py and checks that the attribute
survives into the real operator.
"""
import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# Every task that can mutate production state, and nothing else. Deriving this
# from the source would make the test agree with whatever the source says,
# which is the one thing it must not do.
EXPECTED_POOLED_TASKS = {
    ("results_processing.py", "process_batch"),
    ("orphan_checker.py", "expire_orphan_detail_claims"),
    ("orphan_checker.py", "reap_stuck_processing"),
    ("orphan_checker.py", "evict_delisted_cooldowns"),
    # `scrape_detail_pages.claim_batch` was here until Plan 147 Stage 3
    # (2026-08-30). It was held only because pausing `results_processing` used
    # to leave the detail scraper re-claiming the same listings every 15
    # minutes; Plan 147 moved that guard next to the fetch, and production
    # verification showed 2,000 fetches over five batches with zero repeats
    # while processing was paused. See TestScrapeDetailPagesIsNotPooled below,
    # which pins the removal rather than letting it look like drift.
}

# The sensor factories in sensors.py. Both build `mode="reschedule"` sensors,
# which must never hold a pool slot while they wait.
SENSOR_FACTORIES = {"deploy_intent_sensor", "http_health_sensor"}


def _dag_files():
    return sorted(p for p in DAGS_DIR.glob("*.py") if not p.name.startswith("_"))


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _kwarg(call: ast.Call, name: str):
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _operator_calls(tree: ast.Module):
    """Yield (task_id, call) for every call site that names a task_id literal."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        task_id = _kwarg(node, "task_id")
        if isinstance(task_id, ast.Constant) and isinstance(task_id.value, str):
            yield task_id.value, node


def _sensor_factory_calls(tree: ast.Module):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in SENSOR_FACTORIES:
                yield node.func.id, node


def _pools_module_constants():
    ns = {}
    exec(compile(_tree(DAGS_DIR / "pools.py"), "pools.py", "exec"), ns)  # noqa: S102
    return ns


class TestTheAssignmentCoversExactlyTheMutatingTasks:
    def test_pooled_tasks_are_the_expected_set(self):
        """Both directions matter. A mutating task that loses its pool is a
        hole in the gate that only shows up mid-window; a task that gains one
        by accident stops running the moment the pool is held."""
        found = set()
        for path in _dag_files():
            for task_id, call in _operator_calls(_tree(path)):
                if _kwarg(call, "pool") is not None:
                    found.add((path.name, task_id))

        assert found == EXPECTED_POOLED_TASKS, (
            "maintenance pool assignment drifted:\n"
            f"  unexpectedly pooled: {sorted(found - EXPECTED_POOLED_TASKS)}\n"
            f"  no longer pooled:    {sorted(EXPECTED_POOLED_TASKS - found)}"
        )

    @pytest.mark.parametrize("filename,task_id", sorted(EXPECTED_POOLED_TASKS))
    def test_pool_is_the_shared_constant_not_a_string_literal(self, filename, task_id):
        """`airflow pools set` takes a name typed by hand under pressure. One
        constant means the DAGs and the operator's command cannot disagree by
        a typo that no import error would catch."""
        tree = _tree(DAGS_DIR / filename)
        call = next(c for tid, c in _operator_calls(tree) if tid == task_id)
        pool = _kwarg(call, "pool")

        assert isinstance(pool, ast.Name) and pool.id == "MAINTENANCE_POOL", (
            f"{filename}:{task_id} sets pool to something other than the "
            "MAINTENANCE_POOL constant from pools.py"
        )

    def test_scrape_detail_itself_is_not_pooled(self):
        """Named separately from the set comparison because this one is a
        design decision rather than an omission: `scrape_detail` is the task
        Plan 136 Stage 3d wants in its own `solver` pool, and a task has
        exactly one pool."""
        tree = _tree(DAGS_DIR / "scrape_detail_pages.py")
        call = next(c for tid, c in _operator_calls(tree) if tid == "scrape_detail")
        assert _kwarg(call, "pool") is None


class TestScrapeDetailPagesIsNotPooled:
    """Plan 147 Stage 3 removed this DAG from the maintenance hold.

    Asserted positively, rather than left to the set comparison above, because
    the two failure directions mean opposite things. An absence caught only by
    the set would read as drift — the exact thing these tests exist to catch —
    and someone restoring the pool to "be safe" would silently reintroduce a
    coupling that was measured and removed on evidence.
    """

    def test_claim_batch_is_not_pooled(self):
        """Held only because a processing pause used to loop the scraper.

        `release_claims` now records `last_detail_fetched_at` in the
        transaction that deletes the claim, so the guard survives a processing
        outage on its own. Verified in production 2026-08-30: with
        `results_processing` paused 81 minutes, five batches fetched 2,000
        listings and repeated none. Restoring the pool would mean a processing
        pause once again implies a scraper pause, which is no longer true.
        """
        tree = _tree(DAGS_DIR / "scrape_detail_pages.py")
        call = next(c for tid, c in _operator_calls(tree) if tid == "claim_batch")
        assert _kwarg(call, "pool") is None, (
            "claim_batch is pooled again — see Plan 147 Stage 3 evidence "
            "before restoring it; the coupling that justified the hold is gone"
        )

    def test_no_task_in_this_dag_is_pooled(self):
        """The DAG carries no maintenance hold at all.

        Quiescing detail fetches for a host reboot is a separate concern and
        Plan 142 covers it through the `detail_fetch` surface, not this pool.
        """
        tree = _tree(DAGS_DIR / "scrape_detail_pages.py")
        pooled = [
            tid for tid, call in _operator_calls(tree)
            if _kwarg(call, "pool") is not None
        ]
        assert not pooled, f"scrape_detail_pages tasks unexpectedly pooled: {pooled}"


class TestSensorsNeverHoldASlot:
    def test_no_dag_passes_a_pool_to_a_sensor_factory(self):
        """Both factories take **kwargs, so `pool=` would sail straight
        through to the sensor. A pooled sensor in `reschedule` mode is the
        shape this whole mechanism exists to avoid."""
        offenders = [
            f"{path.name}: {factory}"
            for path in _dag_files()
            for factory, call in _sensor_factory_calls(_tree(path))
            if _kwarg(call, "pool") is not None
        ]
        assert not offenders, f"sensors given a pool slot: {offenders}"

    def test_the_factories_do_not_set_a_pool_themselves(self):
        tree = _tree(DAGS_DIR / "sensors.py")
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in SENSOR_FACTORIES:
                src = ast.get_source_segment(
                    (DAGS_DIR / "sensors.py").read_text(encoding="utf-8"), node
                )
                assert "pool" not in src, f"{node.name} sets a pool"


class TestPhaseAIsInert:
    def test_there_are_more_slots_than_pooled_tasks(self):
        """This is what makes Phase A deployable without a window: if every
        pooled task in the repository could run at once and still not exhaust
        the pool, the attribute cannot change scheduling. It only starts
        gating when an operator sets the count to 0."""
        slots = _pools_module_constants()["MAINTENANCE_SLOTS"]
        assert slots >= len(EXPECTED_POOLED_TASKS), (
            f"MAINTENANCE_SLOTS={slots} is not above the {len(EXPECTED_POOLED_TASKS)} "
            "pooled tasks, so the assignment is no longer inert"
        )


class TestTheHoldIsNotDeclarative:
    def test_nothing_in_compose_sets_the_pool(self):
        """`airflow pools set` is an upsert, so a declarative create in
        `airflow-init` would reset the slot count on every `up -d`. The slot
        count *is* the hold state, and a maintenance window recreates the
        stack — so that would silently release a hold mid-window, which is
        exactly what Plan 142's first design principle forbids.

        The pool is therefore created once, by hand, and only an operator ever
        changes it. See the docstring in airflow/dags/pools.py."""
        compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "pools set" not in compose and "pools import" not in compose, (
            "docker-compose.yml creates or resets an Airflow pool. That makes "
            "the maintenance hold auto-releasing."
        )
