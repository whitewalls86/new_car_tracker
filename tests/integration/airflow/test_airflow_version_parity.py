"""
The Airflow version CI tests must be the version production runs.

Plan 139 Stage F gives `tests/integration/sql/` a real `airflow` schema by
running `airflow db migrate` in CI, so the coordination-drain queries execute
against Airflow's actual tables rather than a stand-in. That guarantee is only
worth anything while CI's Airflow is production's Airflow: bump
`airflow/Dockerfile` alone and CI keeps validating the previous schema, which
turns the whole stage into a false assurance at the one moment it matters most
— an Airflow upgrade.

`ci.yml` already carries a comment saying its pin matches the Dockerfile's base
image tag. A comment is not a check. This is the check.
"""
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[3]
DOCKERFILE = REPO_ROOT / "airflow" / "Dockerfile"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _dockerfile_version() -> str:
    match = re.search(
        r"^FROM\s+apache/airflow:([^\s@]+)",
        DOCKERFILE.read_text(encoding="utf-8"),
        re.M,
    )
    assert match, f"no `FROM apache/airflow:<tag>` in {DOCKERFILE}"
    return match.group(1)


def _workflow_version() -> str:
    matches = re.findall(r'apache-airflow==([0-9][^"\'\s]*)', WORKFLOW.read_text(encoding="utf-8"))
    assert matches, f"no pinned `apache-airflow==<version>` in {WORKFLOW}"
    assert len(set(matches)) == 1, f"{WORKFLOW} pins apache-airflow to {set(matches)}"
    return matches[0]


@pytest.mark.integration
def test_ci_pins_the_airflow_version_production_runs():
    """The venv CI installs and the image prod runs are one version."""
    assert _workflow_version() == _dockerfile_version(), (
        f"ci.yml installs apache-airflow=={_workflow_version()} but "
        f"airflow/Dockerfile runs apache/airflow:{_dockerfile_version()}. "
        "CI would validate a schema production does not have."
    )


@pytest.mark.integration
def test_the_installed_airflow_is_the_pinned_airflow():
    """
    And the venv actually running this test is that version.

    Belt and braces over the two file reads above: a resolver that quietly
    installed something else, or a developer running this suite outside the
    isolated venv, both leave the files agreeing and the schema wrong.
    """
    import airflow

    assert airflow.__version__ == _dockerfile_version(), (
        f"running Airflow {airflow.__version__}, but airflow/Dockerfile runs "
        f"apache/airflow:{_dockerfile_version()}"
    )
