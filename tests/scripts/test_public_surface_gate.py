"""Plan 138 Stage 10: the commit gate itself, asserted rather than assumed.

The gate has held ``README.md`` and ``ops/templates/info.html`` since Stage 1c
and has never had a test. That is the wrong shape for a mechanism whose whole
value is that it cannot be forgotten: every way it can quietly stop working is
silent. It fails open on unparseable input by design, it only fires on ``git
commit``, and it clears on a stamp -- so a bug in any of those three reads
exactly like a commit with no public surface in it.

The stamp is the part most worth pinning. It exists so the gate is
satisfiable, and it is keyed on the digest of the staged diff precisely so that
passing once buys nothing for content nobody looked at. A stamp that cleared
the gate for a *later* re-stage would leave the mechanism looking healthy while
checking nothing, which is the failure this file exists to make loud.

``_git`` is patched rather than driving a real repository: the gate's logic is
what is under test, not git's.
"""
from __future__ import annotations

import io
import json

import pytest

from scripts import public_surface_gate as gate


def _payload(command: str) -> str:
    return json.dumps({"tool_input": {"command": command}})


@pytest.fixture
def harness(mocker, tmp_path):
    """Drive the gate with a scripted git and a stamp file under tmp_path."""

    state = {"staged": [], "diff": "diff --git a/README.md b/README.md\n+one"}
    stamp = tmp_path / "public-surface-stamp"

    def fake_git(*args: str) -> str:
        if args[:3] == ("diff", "--cached", "--name-only"):
            return "\n".join(state["staged"])
        if args[:2] == ("diff", "--cached"):
            return state["diff"]
        if args[:1] == ("rev-parse",):
            return str(tmp_path)
        return ""

    mocker.patch.object(gate, "_git", side_effect=fake_git)
    mocker.patch.object(gate, "stamp_path", return_value=stamp)

    def run(command: str = "git commit -m x") -> int:
        mocker.patch("sys.stdin", io.StringIO(_payload(command)))
        return gate.main()

    state["run"] = run
    state["stamp"] = stamp
    return state


class TestWhenTheGateStaysOutOfTheWay:
    def test_a_command_that_is_not_a_commit_passes(self, harness):
        harness["staged"] = ["README.md"]

        assert harness["run"]("git status") == 0

    def test_a_commit_touching_no_public_surface_passes(self, harness):
        harness["staged"] = ["ops/routers/info.py", "docs/PLANS.md"]

        assert harness["run"]() == 0

    def test_unparseable_input_fails_open(self, mocker):
        """A hook that cannot read its input must not block every commit.

        Deliberate: the cost of a missed check is a review catch, the cost of
        failing closed is an unclearable gate on all work.
        """
        mocker.patch("sys.stdin", io.StringIO("not json"))

        assert gate.main() == 0


class TestWhenTheGateFires:
    @pytest.mark.parametrize("surface", gate.SURFACES)
    def test_each_named_surface_is_held(self, harness, surface, capsys):
        harness["staged"] = [surface]

        assert harness["run"]() == 2
        assert surface in capsys.readouterr().err

    def test_the_message_names_only_the_surfaces_actually_staged(
        self, harness, capsys
    ):
        harness["staged"] = ["README.md", "ops/routers/info.py"]

        harness["run"]()

        err = capsys.readouterr().err
        assert "README.md" in err
        assert "ops/templates/info.html" not in err


class TestTheStamp:
    def test_a_stamp_for_this_content_clears_the_gate(self, harness):
        harness["staged"] = ["README.md"]
        harness["stamp"].write_text(
            gate.staged_digest(["README.md"]), encoding="utf-8"
        )

        assert harness["run"]() == 0

    def test_restaging_after_the_check_closes_the_gate_again(self, harness):
        """The point of keying the stamp on the digest, and the reason the
        mechanism is worth anything: passing once must not buy passage for
        content nobody read."""
        harness["staged"] = ["README.md"]
        harness["stamp"].write_text(
            gate.staged_digest(["README.md"]), encoding="utf-8"
        )
        assert harness["run"]() == 0

        harness["diff"] = "diff --git a/README.md b/README.md\n+something else"

        assert harness["run"]() == 2

    def test_a_stamp_from_another_surface_does_not_clear_this_one(self, harness):
        harness["staged"] = ["README.md"]
        harness["stamp"].write_text("a digest of something else", encoding="utf-8")

        assert harness["run"]() == 2
