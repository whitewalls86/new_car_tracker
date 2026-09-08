"""The gate that holds a commit until the attribution skill has run for it.

Written the day five commits went through one invocation of that skill, which
is the failure the stamp is shaped against: the rule has to be per-commit, and
it has to survive a bundled `git add ... && git commit`, because bundling is
how the last gate in this repository was silently defeated.

``_git`` is patched rather than driving a real repository: the gate's logic is
what is under test, not git's.
"""
from __future__ import annotations

import io
import json

import pytest

from scripts import commit_attribution_gate as gate


def _payload(command: str) -> str:
    return json.dumps({"tool_input": {"command": command}})


@pytest.fixture
def harness(mocker, tmp_path):
    state = {"stamp": tmp_path / gate.STAMP_NAME}

    mocker.patch.object(gate, "_git", return_value=str(tmp_path))
    mocker.patch.object(gate, "stamp_path", return_value=state["stamp"])

    def run(command: str) -> int:
        mocker.patch("sys.stdin", io.StringIO(_payload(command)))
        return gate.main()

    state["run"] = run
    state["stamp_it"] = lambda: state["stamp"].write_text("now", encoding="utf-8")
    return state


class TestWhenItStaysOutOfTheWay:
    @pytest.mark.parametrize(
        "command",
        [
            "git status",
            "git add README.md",
            "pytest tests/ -q",
            'echo "git commit -m x"',
            "grep -n 'git commit' docs/PLANS.md",
            "git config commit.gpgsign false",
            "git status; echo commit",
            "git log --format=%s | grep commit",
        ],
    )
    def test_a_command_that_does_not_commit_passes(self, harness, command):
        assert harness["run"](command) == 0

    def test_a_heredoc_body_quoting_a_commit_is_not_a_commit(self, harness):
        """The false positive this repository actually hit, twice in one hour.

        The messages written here quote shell verbatim, so a body containing
        the words `git commit` reaches a gate keyed on a bare substring search
        and stops work that was never a commit.
        """
        command = (
            "cat >> notes.md <<'EOF'\n"
            "A bundled `git add README.md && git commit -m x` showed it an\n"
            "empty index, and it passed.\n"
            "EOF\n"
            "echo done"
        )

        assert harness["run"](command) == 0

    def test_unparseable_input_fails_open(self, mocker):
        mocker.patch("sys.stdin", io.StringIO("not json"))

        assert gate.main() == 0


class TestWhenItFires:
    @pytest.mark.parametrize(
        "command",
        [
            "git commit -m x",
            "git commit -am x",
            "git commit --amend",
            "git -C /some/worktree commit -m x",
            "git --no-pager commit -F -",
            "git commit -F -",
            "git add .; git commit -m x",
            "if true; then git commit -m x; fi",
            "GIT_EDITOR=true git commit --amend",
        ],
    )
    def test_every_shape_of_commit_needs_the_skill(self, harness, command):
        assert harness["run"](command) == 2

    def test_bundling_does_not_defeat_it(self, harness, capsys):
        """The crux, and why this is a stamp and not an index read.

        Plan 175 closed a hole where the public-surface gate matched
        `git commit` and then read an index a bundled `git add` had not
        populated. This gate reads no index, so the bundle changes nothing.
        """
        assert harness["run"]("git add README.md && git commit -m x") == 2
        assert "commit-plan-attribution" in capsys.readouterr().err

    def test_the_message_says_what_to_do(self, harness, capsys):
        harness["run"]("git commit -m x")

        err = capsys.readouterr().err
        assert "commit-plan-attribution" in err
        assert "No plan" in err


class TestTheStamp:
    def test_a_stamp_lets_the_commit_through(self, harness):
        harness["stamp_it"]()

        assert harness["run"]("git commit -m x") == 0

    def test_a_stamp_lets_a_bundled_commit_through_too(self, harness):
        harness["stamp_it"]()

        assert harness["run"]("git add README.md && git commit -m x") == 0
