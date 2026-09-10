"""The Airflow words this repository types out are words Airflow still has.

Plan 162 Stage AB / CAR-106.

``airflow/dags/notifications.py`` decides which sibling tasks to quote in a
page by comparing a task state against the literal ``"failed"``. That word
belongs to ``airflow.utils.state.TaskInstanceState``, and if Airflow renames
it the comparison does not raise -- it simply stops being true, the pager goes
out naming no tasks, and nothing anywhere says why. This module is what makes
that loud.

**The restatement is deliberate and this test exists so it can stay one.**
``notifications.py`` imports nothing from Airflow on purpose. Its own docstring
records ``c92bd97``, which put ``ti.execution_date`` in a task callback and
silenced every pager in the fleet -- 268 consecutive failures -- because
Airflow 3's task SDK ``RuntimeTaskInstance`` does not carry it. Importing
``airflow.utils.state`` inside a task callback is the same bet on the same
surface. So the DAG keeps the literal, and *this* venv -- the isolated
``apache-airflow==3.2.0`` one CI builds, where the real package is installed --
is what asks whether the literal is still real.

That is the corpus/replay split ``scripts/verify_promtail_contract.py`` and
``scripts/verify_container_health_docker_contract.py`` already run twice over:
one corpus, two consumers, neither importing the other. The corpus is
``tests/external_vocabulary_census.py``.

**The third rule that used to live here moved to
``tests/rules/test_external_vocabularies.py`` in Stage AG.** It reads
``notifications.py`` with ``ast`` and compares against the census, imports
no Airflow at all, and is therefore a repository-wide rule that happened to
be sitting in an integration suite. The two that remain genuinely need the
installed package, which is why they stay here and stay out of the
``Asserted by`` column.

**Neither of the two rules below works alone**, which is Stage W's lesson
repeated one system out. ``test_every_restated_airflow_task_state_is_a_real_member``
alone would pass a census that had drifted away from the DAG -- it checks the
census against Airflow, not against the code.
``test_every_airflow_state_the_dags_compare_against_is_declared`` alone would
pass a census and a DAG that agree with each other and disagree with Airflow.
Together they pin the word at both ends.

**The census is loaded by path, not imported.** pytest leaves the repository
root off ``sys.path`` in this venv -- the job runs with
``PYTHONPATH=airflow/dags`` -- so ``from tests.external_vocabulary_census
import ...`` raises ``ModuleNotFoundError`` here while passing locally. CI run
33444675959 failed exactly that way for ``tests/health_sensor_census.py``.
"""
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[3]
CENSUS_PATH = REPO_ROOT / "tests" / "external_vocabulary_census.py"
NOTIFICATIONS = REPO_ROOT / "airflow" / "dags" / "notifications.py"


def _census():
    spec = importlib.util.spec_from_file_location("external_vocabulary_census", CENSUS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _declared(what: str) -> tuple:
    """The members the census declares for the vocabulary named *what*."""
    census = _census()
    rows = [row for row in census.CENSUS if row["what"] == what]
    assert len(rows) == 1, f"{CENSUS_PATH} declares {len(rows)} rows for {what!r}, expected 1"
    return rows[0]["members"]


@pytest.mark.integration
def test_every_restated_airflow_task_state_is_a_real_member():
    """Every task state the census declares is a member of the real enum.

    This is the check the stage was scoped around, and it is three lines
    because the job that runs it already installs the Airflow production runs.
    """
    from airflow.utils.state import TaskInstanceState

    real = {state.value for state in TaskInstanceState}
    declared = _declared("airflow.utils.state.TaskInstanceState")

    unreal = sorted(set(declared) - real)
    assert not unreal, (
        f"{CENSUS_PATH} declares these as TaskInstanceState members and "
        f"apache-airflow does not have them: {unreal}.\n"
        f"Real members: {sorted(real)}.\n"
        "A DAG comparing a task state against a word Airflow no longer uses "
        "does not raise -- it silently never matches. Fix the comparison in "
        "airflow/dags/notifications.py and the declaration together."
    )


@pytest.mark.integration
def test_every_restated_airflow_trigger_rule_is_a_real_member():
    """Same question for the trigger rules, which four DAGs type out.

    Already loud -- ``BaseOperator`` validates a trigger rule while the DagBag
    is built -- so this asserts something ``test_dag_imports_without_error``
    would also catch. It is here because the census claims that coverage, and
    a claim about an existing check is worth exactly as much as the check
    being where the claim says it is.
    """
    # `airflow.task.trigger_rule`, not `airflow.utils.trigger_rule`. The
    # second still resolves in 3.2.0 and emits a DeprecatedImportWarning
    # saying so -- observed while verifying this test against the real image
    # on 2026-09-09. Importing the deprecated path here would make this rule
    # the thing that breaks on an Airflow upgrade, which is the opposite of
    # what it is for.
    from airflow.task.trigger_rule import TriggerRule

    real = {rule.value for rule in TriggerRule}
    declared = _declared("airflow.task.trigger_rule.TriggerRule")

    unreal = sorted(set(declared) - real)
    assert not unreal, (
        f"{CENSUS_PATH} declares these as TriggerRule members and "
        f"apache-airflow does not have them: {unreal}.\n"
        f"Real members: {sorted(real)}."
    )
