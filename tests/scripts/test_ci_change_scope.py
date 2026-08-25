import subprocess
import sys

import pytest

from scripts.ci_change_scope import docs_only_from_nul


@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        (["docs/PLANS.md"], True),
        (["docs/plans/plan_148.md", "docs/recaps/2026-08-23.md"], True),
        (["docs/PLANS.md", "app.py"], False),
        ([".github/workflows/ci.yml"], False),
        (["tests/test_planning_docs.py"], False),
        (["README.md"], False),
        ([], False),
    ],
)
def test_docs_only_from_nul(paths, expected):
    data = b"".join(path.encode() + b"\0" for path in paths)
    assert docs_only_from_nul(data) is expected


def test_docs_only_supports_spaces_in_paths():
    assert docs_only_from_nul(b"docs/a plan.md\0docs/another plan.md\0") is True


@pytest.mark.parametrize(
    "data",
    [
        b"docs/PLANS.md",
        b"docs/PLANS.md\0\0",
    ],
)
def test_malformed_input_fails_closed(data):
    with pytest.raises(ValueError):
        docs_only_from_nul(data)


def test_command_line_contract():
    completed = subprocess.run(
        [sys.executable, "scripts/ci_change_scope.py"],
        input="docs/PLANS.md\0docs/plans/plan_148.md\0",
        capture_output=True,
        check=True,
        text=True,
    )
    assert completed.stdout.splitlines() == ["true"]
