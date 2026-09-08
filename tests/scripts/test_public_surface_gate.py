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
import pathlib
import subprocess

import pytest

from scripts import public_surface_gate as gate


def _payload(command: str) -> str:
    return json.dumps({"tool_input": {"command": command}})


@pytest.fixture
def harness(mocker, tmp_path):
    """Drive the gate with a scripted git and a stamp file under tmp_path."""

    state = {"staged": [], "diff": "diff --git a/README.md b/README.md\n+one"}
    state["hooks_path"] = gate.HOOKS_PATH
    state["top"] = tmp_path
    # The main-clone shape: the common dir sits inside the working tree root,
    # so its parent is that root. A linked worktree is the case where `top`
    # and this disagree, and the tests that need it set both.
    state["common_dir"] = str(tmp_path / ".git")
    stamp = tmp_path / "public-surface-stamp"

    def fake_git(*args: str) -> str:
        if args[:3] == ("diff", "--cached", "--name-only"):
            return "\n".join(state["staged"])
        if args[:2] == ("diff", "--cached"):
            return state["diff"]
        if args == ("rev-parse", "--show-toplevel"):
            return str(state["top"])
        if args[:1] == ("rev-parse",) and "--git-common-dir" in args:
            return state["common_dir"]
        if args[:1] == ("rev-parse",):
            return str(tmp_path)
        if args == ("config", "--get", "core.hooksPath"):
            return state["hooks_path"]
        return ""

    mocker.patch.object(gate, "_git", side_effect=fake_git)
    mocker.patch.object(gate, "stamp_path", return_value=stamp)

    def run(command: str = "git commit -m x", argv: list | None = None) -> int:
        mocker.patch("sys.stdin", io.StringIO(_payload(command)))
        return gate.main(argv or [])

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

        assert gate.main([]) == 0


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


class TestTheInstallIsNotOptional:
    """Plan 175 Stage A.

    Git does not track hooks, so a clone that never ran the install has no
    ``pre-commit`` hook and says nothing about it -- the same silent-absence
    class as the bundling defect itself. The ``PreToolUse`` hook is tracked in
    ``.claude/settings.json`` and is therefore live in every clone from the
    first checkout, so it is the thing that can notice.
    """

    def test_a_clone_without_the_hook_installed_is_refused(self, harness, capsys):
        harness["hooks_path"] = ""
        harness["staged"] = ["README.md"]

        assert harness["run"]() == 2
        assert "git config core.hooksPath .githooks" in capsys.readouterr().err

    def test_the_install_check_fires_even_with_no_surface_staged(self, harness):
        """The one that matters, and the reason this check precedes the index
        read rather than following it.

        A missing install is exactly the case where the index proves nothing:
        the commit may be about to stage a surface itself. Gating this on what
        the index already holds would rebuild the defect Plan 175 closed.
        """
        harness["hooks_path"] = ""
        harness["staged"] = []

        assert harness["run"]() == 2

    def test_a_hooks_path_pointing_somewhere_else_is_not_installed(self, harness):
        harness["hooks_path"] = ".git/hooks"
        harness["staged"] = []

        assert harness["run"]() == 2

    def test_an_absolute_hooks_path_is_installed(self, harness):
        """The spelling that actually broke it, hours after the stage merged.

        `git config` stores whatever it was handed. Something rewrote the value
        to an absolute path pointing at the same directory; git kept running
        the hook, and an exact string match called it uninstalled and refused
        every commit.
        """
        harness["hooks_path"] = str(harness["top"] / gate.HOOKS_PATH)
        harness["staged"] = []

        assert harness["run"]() == 0

    def test_an_absolute_path_from_the_main_clone_is_installed_in_a_worktree(
        self, harness, tmp_path
    ):
        """Plan 163, found 2026-09-08 committing Plan 154 Stage B.

        `core.hooksPath` is shared config, so the absolute value written once
        in the main clone is what every linked worktree reads, and git runs the
        hook from all of them. Resolving it against only the current worktree's
        root called each one uninstalled and refused the commit -- a working
        gate reporting itself broken, which is the failure the absolute-path
        case above already exists to prevent.
        """
        worktree = tmp_path / ".claude" / "worktrees" / "a-branch"
        harness["top"] = worktree
        harness["common_dir"] = str(tmp_path / ".git")
        harness["hooks_path"] = str(tmp_path / gate.HOOKS_PATH)
        harness["staged"] = []

        assert harness["run"]() == 0

    def test_a_worktree_relative_spelling_is_still_installed(
        self, harness, tmp_path
    ):
        """The relative value resolves per worktree, and must keep working.

        This is the spelling the install message asks for, so a fix that only
        taught the check about the main clone's copy would break the one the
        documentation tells people to use.
        """
        worktree = tmp_path / ".claude" / "worktrees" / "a-branch"
        harness["top"] = worktree
        harness["common_dir"] = str(tmp_path / ".git")
        harness["hooks_path"] = gate.HOOKS_PATH
        harness["staged"] = []

        assert harness["run"]() == 0

    def test_an_unrelated_absolute_path_is_still_refused_from_a_worktree(
        self, harness, tmp_path
    ):
        """Widening to two roots must not widen to any root."""
        worktree = tmp_path / ".claude" / "worktrees" / "a-branch"
        harness["top"] = worktree
        harness["common_dir"] = str(tmp_path / ".git")
        harness["hooks_path"] = str(tmp_path / "somewhere" / "else")
        harness["staged"] = []

        assert harness["run"]() == 2

    def test_a_dot_slash_spelling_is_installed(self, harness):
        harness["hooks_path"] = "./" + gate.HOOKS_PATH
        harness["staged"] = []

        assert harness["run"]() == 0

    def test_a_command_that_is_not_a_commit_is_never_asked_to_install(
        self, harness
    ):
        harness["hooks_path"] = ""

        assert harness["run"]("git status") == 0


class TestTheInstallCheckAgainstRealGit:
    """The same question as above, asked of git instead of a fake.

    Every other test here patches ``_git``, which is right for the gate's
    logic and useless for this particular bug: it was a disagreement about
    what git *returns*, and a fake that returns what the author expected
    cannot fail that way. This repository is developed on macOS and Windows
    and CI runs on Linux, and the three spell paths differently -- drive
    letters, separators, and whether ``--show-toplevel`` and
    ``--git-common-dir`` agree on either. So this drives a real repository
    with a real linked worktree, and it is the test that would notice if one
    platform disagreed.
    """

    @staticmethod
    def _run(*args, cwd):
        subprocess.run(
            ["git", *args], cwd=cwd, check=True,
            capture_output=True, text=True, encoding="utf-8",
        )

    @pytest.fixture
    def clone_with_worktree(self, tmp_path):
        """A real clone at `main`, with a real linked worktree at `linked`."""
        main = tmp_path / "clone"
        main.mkdir()
        # `-c init.defaultBranch` rather than `init -b`: the flag needs git
        # 2.28, and an older git ignores the unknown config and calls the
        # branch something else, which this test does not care about. macOS
        # ships an older git than the Windows side of this repository does.
        self._run("-c", "init.defaultBranch=trunk", "init", cwd=main)
        self._run("config", "user.email", "t@example.invalid", cwd=main)
        self._run("config", "user.name", "T", cwd=main)
        (main / gate.HOOKS_PATH).mkdir()
        (main / gate.HOOKS_PATH / "pre-commit").write_text(
            "#!/bin/sh\n", encoding="utf-8"
        )
        (main / "seed").write_text("seed\n", encoding="utf-8")
        self._run("add", "-A", cwd=main)
        self._run("commit", "-m", "seed", cwd=main)

        linked = tmp_path / "linked"
        self._run("worktree", "add", str(linked), "-b", "side", cwd=main)
        return main, linked

    def test_an_absolute_hooks_path_holds_from_the_linked_worktree(
        self, clone_with_worktree, monkeypatch
    ):
        """The exact configuration that refused a commit on 2026-09-08."""
        main, linked = clone_with_worktree
        self._run(
            "config", "core.hooksPath", str(main / gate.HOOKS_PATH), cwd=main
        )

        monkeypatch.chdir(linked)
        assert gate.hooks_installed() is True

        monkeypatch.chdir(main)
        assert gate.hooks_installed() is True

    def test_a_relative_hooks_path_holds_from_both(
        self, clone_with_worktree, monkeypatch
    ):
        """The spelling the install message asks for, which must keep working.

        It resolves per worktree, so the linked tree needs its own checked-out
        copy -- which it has, because `.githooks` is tracked.
        """
        main, linked = clone_with_worktree
        self._run("config", "core.hooksPath", gate.HOOKS_PATH, cwd=main)

        monkeypatch.chdir(linked)
        assert gate.hooks_installed() is True

        monkeypatch.chdir(main)
        assert gate.hooks_installed() is True

    def test_an_unrelated_path_is_still_refused_from_the_worktree(
        self, clone_with_worktree, monkeypatch
    ):
        """Two accepted roots, not any root."""
        main, linked = clone_with_worktree
        elsewhere = main.parent / "elsewhere"
        elsewhere.mkdir()
        self._run("config", "core.hooksPath", str(elsewhere), cwd=main)

        monkeypatch.chdir(linked)
        assert gate.hooks_installed() is False


class TestTheGitHookEntryPoint:
    """``.githooks/pre-commit`` calls ``--pre-commit``.

    Git runs it after ``-a`` has staged, so the index it reads is the index
    being committed -- which is the whole of Plan 175. There is no payload and
    no command string: how the commit was assembled has stopped mattering.
    """

    def test_it_reads_the_index_with_no_payload_at_all(self, harness, mocker):
        harness["staged"] = ["README.md"]
        mocker.patch("sys.stdin", io.StringIO(""))

        assert gate.main(["--pre-commit"]) == 2

    def test_it_clears_on_the_stamp_like_the_other_entry_point(
        self, harness, mocker
    ):
        harness["staged"] = ["README.md"]
        harness["stamp"].write_text(
            gate.staged_digest(["README.md"]), encoding="utf-8"
        )
        mocker.patch("sys.stdin", io.StringIO(""))

        assert gate.main(["--pre-commit"]) == 0

    def test_it_passes_a_commit_with_no_surface_in_it(self, harness, mocker):
        harness["staged"] = ["docs/PLANS.md"]
        mocker.patch("sys.stdin", io.StringIO(""))

        assert gate.main(["--pre-commit"]) == 0

    def test_it_does_not_ask_about_the_install(self, harness, mocker):
        """It is running, so it is installed. Asking would be a second answer
        to a question this entry point has already answered."""
        harness["hooks_path"] = ""
        harness["staged"] = ["docs/PLANS.md"]
        mocker.patch("sys.stdin", io.StringIO(""))

        assert gate.main(["--pre-commit"]) == 0


class TestTheHookIsRunnableWhereItIsCheckedOut:
    def test_the_pre_commit_hook_is_executable_in_the_index(self):
        """Git silently declines to run a hook without the execute bit.

        Found by Plan 175 Stage A's own first commit: `chmod +x` on Windows
        does not reach the index, so the hook landed as 100644 and would have
        been checked out unrunnable on macOS -- installed, configured, and
        never firing. That is the silent absence this plan exists to remove,
        so it is asserted rather than remembered.
        """
        entry = subprocess.run(
            ["git", "ls-files", "--stage", ".githooks/pre-commit"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True,
            cwd=pathlib.Path(gate.__file__).resolve().parent.parent,
        ).stdout

        assert entry.startswith("100755 "), entry or "hook is not tracked at all"
