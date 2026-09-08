"""``.githooks/commit-msg``'s verdict and its stamp consumption (Plan 175 Stage A).

The hook holds two things that fail differently, and only the first had any
coverage: whether a message is *accepted*, and whether accepting it *consumes*
the skill's stamp. The second shipped untested and was wrong.

**The defect, measured 2026-09-04 on a real merge commit.** Stamp consumption
lived inside the attribution branch, below an early `exit 0` for `Merge`,
`Revert` and `fixup!` subjects. Those subjects are rightly exempt from the
attribution check -- git composes them -- but exempting them from consumption
left the stamp on disk, and the next commit then cleared
``scripts/commit_attribution_gate.py`` without the skill running at all. That
is the "invoke once per session" hole the whole mechanism exists to close,
reintroduced through its own exemption.

The hook is `/bin/sh`, so it is driven here as a subprocess against a real
throwaway repository rather than reimplemented in Python. Reimplementing the
predicate would be a paraphrase of production, which is worse than no test.
"""

from __future__ import annotations

import pathlib
import subprocess

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
HOOK = REPO_ROOT / ".githooks" / "commit-msg"

pytestmark = pytest.mark.skipif(
    subprocess.run(["sh", "-c", "exit 0"], capture_output=True).returncode != 0,
    reason="the hook is a POSIX shell script and needs a shell to run at all",
)


@pytest.fixture()
def repo(tmp_path: pathlib.Path) -> pathlib.Path:
    """A throwaway git repository, so `git rev-parse --git-dir` resolves."""
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=master", str(tmp_path)],
        capture_output=True,
        check=True,
    )
    return tmp_path


def run_hook(repo: pathlib.Path, message: str) -> tuple[int, bool]:
    """``(exit code, stamp still exists)`` after running the hook on a message."""
    stamp = repo / ".git" / "commit-attribution-stamp"
    stamp.write_text("2026-09-04T00:00:00Z\n", encoding="utf-8")

    msg_file = repo / "COMMIT_EDITMSG_TEST"
    msg_file.write_text(message, encoding="utf-8")

    completed = subprocess.run(
        ["sh", str(HOOK), str(msg_file)],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.returncode, stamp.exists()


# Every subject the hook exempts from the attribution check. Parametrised from
# the exemption list itself rather than spot-checked: the defect was that one
# branch of this set behaved differently from the other, and a test that
# happened to sample the working branch would have passed.
EXEMPT_SUBJECTS = [
    "Merge origin/master into a-branch",
    "Merge pull request #371 from whitewalls86/some-branch",
    'Revert "docs(plan-164): something"',
    "fixup! docs(plan-164): something",
    "squash! docs(plan-164): something",
    "amend! docs(plan-164): something",
]

ATTRIBUTED_MESSAGES = [
    "docs(plan-164): close the stages\n\nPlan 164.\n",
    "feat: add the thing\n\nPlans 139 and 142.\n",
    "chore: tidy up\n\nNo plan -- unrelated housekeeping.\n",
]


class TestAcceptedMessagesConsumeTheStamp:
    """The half that shipped untested, and the half that was wrong."""

    @pytest.mark.parametrize("subject", EXEMPT_SUBJECTS)
    def test_an_exempt_subject_is_accepted_and_consumes_the_stamp(self, repo, subject):
        code, stamp_survived = run_hook(repo, f"{subject}\n\nbody with no plan named\n")
        assert code == 0, f"{subject!r} should be exempt from the attribution check"
        assert not stamp_survived, (
            f"{subject!r} was accepted but left the stamp on disk. The next commit "
            f"would then clear commit_attribution_gate.py without the skill running."
        )

    @pytest.mark.parametrize("message", ATTRIBUTED_MESSAGES)
    def test_an_attributed_message_is_accepted_and_consumes_the_stamp(self, repo, message):
        code, stamp_survived = run_hook(repo, message)
        assert code == 0
        assert not stamp_survived


class TestRejectedMessagesKeepTheStamp:
    def test_a_message_naming_no_plan_is_rejected(self, repo):
        code, _ = run_hook(repo, "chore: tidy up\n\nnothing about why\n")
        assert code == 1

    def test_and_it_keeps_the_stamp_so_the_wording_can_be_fixed(self, repo):
        """Rejection is not consumption.

        The commit did not happen, so the skill's invocation has not been spent.
        Deleting the stamp here would force a second invocation to fix a typo,
        which trains the author to write the stamp by hand -- the one move the
        skill says makes the whole mechanism worthless.
        """
        _, stamp_survived = run_hook(repo, "chore: tidy up\n\nnothing about why\n")
        assert stamp_survived

    def test_it_says_what_to_do(self, repo):
        msg_file = repo / "m.txt"
        msg_file.write_text("chore: tidy up\n", encoding="utf-8")
        completed = subprocess.run(
            ["sh", str(HOOK), str(msg_file)],
            cwd=str(repo),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert "commit-plan-attribution" in completed.stderr


class TestTheHookIsRunnableWhereItIsCheckedOut:
    def test_the_commit_msg_hook_is_executable_in_the_index(self):
        """The same assertion Plan 175 made for `pre-commit`, for its sibling.

        Git silently declines to run a hook without the execute bit, and
        `chmod +x` on Windows does not reach the index.
        """
        entry = subprocess.run(
            ["git", "ls-files", "--stage", ".githooks/commit-msg"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True,
            cwd=str(REPO_ROOT),
        ).stdout
        assert entry.startswith("100755 "), entry or "hook is not tracked at all"
