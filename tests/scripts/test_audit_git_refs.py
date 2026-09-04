"""Plan 164 Stage 3, success criterion 4: one deliberately constructed case per rule.

Every test below builds a **real** repository with a real remote and runs real
git against it. Nothing here is mocked, because the six findings the auditor
encodes are all facts about git's own behaviour — ``--merged`` under-reporting a
replayed patch, ``-d`` comparing against a configured upstream, a stash being
invisible to every branch command — and a mock of git would assert the
description rather than the behaviour.

Git itself is the one environmental capability these tests need, and they
declare it: the module skips with a reason when git is not on PATH.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.audit_git_refs import (
    DELETABLE,
    OWED,
    PROTECTED,
    UNKNOWN,
    UNPUSHED,
    audit,
    divergence,
    render,
    to_dict,
)

pytestmark = pytest.mark.skipif(
    subprocess.run(["git", "--version"], capture_output=True).returncode != 0,
    reason="these tests drive real git; the auditor's whole subject is git's own behaviour",
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert completed.returncode == 0, f"git {' '.join(args)}: {completed.stderr}"
    return completed.stdout


def _commit(repo: Path, filename: str, message: str) -> str:
    (repo / filename).write_text(f"{message}\n", encoding="utf-8")
    _git(repo, "add", filename)
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD").strip()


def _init_pair(root: Path) -> Path:
    """A bare origin and a clone of it, with one commit on ``master``."""
    origin = root / "origin.git"
    work = root / "work"
    _git(root, "init", "--bare", "--initial-branch=master", str(origin))
    _git(root, "clone", str(origin), str(work))
    _git(work, "config", "user.email", "test@example.com")
    _git(work, "config", "user.name", "Test")
    _git(work, "config", "commit.gpgsign", "false")
    _commit(work, "base.txt", "base")
    _git(work, "push", "-u", "origin", "master")
    return work


@pytest.fixture(scope="module")
def constructed(tmp_path_factory) -> Path:
    """One repository holding a deliberately built case for each of the six findings.

    Laid out so that a *naive* cleanup gets several of them wrong: local
    ``master`` deliberately diverges from ``origin/master``, one branch's patch
    is replayed onto the trunk under a different SHA, and one branch that a
    ``--merged`` check would clear holds the only copy of its content.
    """
    root = tmp_path_factory.mktemp("audit-git-refs")
    work = _init_pair(root)
    base = _git(work, "rev-parse", "HEAD").strip()

    # Finding 1, safe direction — landed on origin/master, invisible to a check
    # taken against a local master that never advanced.
    _git(work, "checkout", "-b", "landed-on-remote-trunk-only", base)
    _commit(work, "landed.txt", "landed")
    _git(work, "push", "origin", "landed-on-remote-trunk-only:master")
    _git(work, "push", "origin", "landed-on-remote-trunk-only")

    # Finding 2 — the same patch replayed onto the trunk under a different SHA.
    # Built from `base`, so the cherry-pick necessarily gets a different parent.
    _git(work, "checkout", "-b", "rewritten-sha", base)
    _commit(work, "rewritten.txt", "rewritten")
    _git(work, "push", "origin", "rewritten-sha")
    _git(work, "fetch", "origin")
    _git(work, "checkout", "--detach", "origin/master")
    _git(work, "cherry-pick", "rewritten-sha")
    _git(work, "push", "origin", "HEAD:master")
    _git(work, "checkout", "master")

    # Finding 1, dangerous direction — merged into *local* master and nowhere else.
    _git(work, "checkout", "-b", "merged-into-local-master-only", base)
    _commit(work, "local-only.txt", "local only")
    _git(work, "checkout", "master")
    _git(work, "merge", "--ff-only", "merged-into-local-master-only")
    _git(work, "push", "origin", "merged-into-local-master-only")

    _git(work, "fetch", "origin")

    # Finding 3 — no remote ref at all: this branch is the only copy.
    _git(work, "checkout", "-b", "only-copy", "origin/master")
    _commit(work, "only-copy.txt", "only copy")

    # Finding 3, second shape — a remote ref exists but does not hold everything.
    _git(work, "checkout", "-b", "ahead-of-remote", "origin/master")
    _commit(work, "pushed.txt", "pushed")
    _git(work, "push", "origin", "ahead-of-remote")
    _commit(work, "not-pushed.txt", "not pushed")

    # Finding 4 — pushed, verifiably complete on the remote, and still not landed.
    _git(work, "checkout", "-b", "owed", "origin/master")
    _commit(work, "owed.txt", "owed")
    _git(work, "push", "origin", "owed")

    # A branch with no commits of its own: deletable, and the control for
    # finding 6 below, which protects an identical branch for a different reason.
    _git(work, "branch", "no-commits-of-its-own", "origin/master")

    # Finding 6 — held by another worktree.
    _git(work, "branch", "held-by-worktree", "origin/master")
    _git(work, "worktree", "add", str(root / "wt"), "held-by-worktree")

    _git(work, "checkout", "master")

    # Finding 5 — a stash, which no `git branch` command can see.
    (work / "base.txt").write_text("dirty\n", encoding="utf-8")
    _git(work, "stash", "push", "-m", "work in progress")

    return work


@pytest.fixture(scope="module")
def report(constructed):
    return audit(constructed, pr_heads=[])


def _verdict(report, name: str) -> str:
    return next(branch.verdict for branch in report.branches if branch.name == name)


def _finding(report, name: str):
    return next(branch for branch in report.branches if branch.name == name)


class TestFinding1TheTrunkIsAlwaysTheRemoteOne:
    """Local ``master`` was 57 commits behind ``origin/master`` on 2026-08-31."""

    def test_local_master_really_does_diverge_in_this_repository(self, constructed):
        # The premise of every assertion in this class. Without it the two
        # directions below would agree and prove nothing.
        ahead, behind = divergence(constructed, "master", "origin/master")
        assert ahead and behind, "the fixture must diverge, or finding 1 is untestable here"

    def test_a_branch_landed_only_on_the_remote_trunk_is_deletable(self, report):
        assert _verdict(report, "landed-on-remote-trunk-only") == DELETABLE

    def test_local_master_would_have_called_that_branch_unmerged(self, constructed):
        # The naive check, run for real. It disagrees with the auditor, and the
        # cost of following it is keeping a branch forever — the harmless
        # direction.
        merged = _git(constructed, "branch", "--merged", "master", "--format=%(refname:short)")
        assert "landed-on-remote-trunk-only" not in merged.split()

    def test_a_branch_merged_into_local_master_only_is_refused(self, report):
        assert _verdict(report, "merged-into-local-master-only") == OWED

    def test_local_master_would_have_cleared_that_branch_for_deletion(self, constructed):
        # The same naive check, now wrong in the direction that destroys work.
        merged = _git(constructed, "branch", "--merged", "master", "--format=%(refname:short)")
        assert "merged-into-local-master-only" in merged.split()


class TestFinding2AncestryIsNotTheTest:
    """Seven byte-identical patches were already on master under different SHAs."""

    def test_a_replayed_patch_counts_as_landed(self, report):
        assert _verdict(report, "rewritten-sha") == DELETABLE

    def test_ancestry_against_the_right_trunk_still_calls_it_unmerged(self, constructed):
        merged = _git(
            constructed, "branch", "--merged", "origin/master", "--format=%(refname:short)"
        )
        assert "rewritten-sha" not in merged.split()

    def test_the_replay_really_carries_a_different_sha(self, constructed):
        branch_tip = _git(constructed, "rev-parse", "rewritten-sha").strip()
        trunk = _git(constructed, "rev-list", "origin/master").split()
        assert branch_tip not in trunk


class TestFinding3AnUnpushedBranchMayHoldTheOnlyCopy:
    def test_a_branch_with_no_remote_ref_is_refused(self, report):
        finding = _finding(report, "only-copy")
        assert finding.verdict == UNPUSHED
        assert finding.remote_ref is None
        assert [commit.subject for commit in finding.unlanded] == ["only copy"]

    def test_a_branch_ahead_of_its_remote_is_refused_and_says_by_how_much(self, report):
        finding = _finding(report, "ahead-of-remote")
        assert finding.verdict == UNPUSHED
        assert finding.ahead_of_remote == 1

    def test_the_push_verification_is_a_count_not_the_absence_of_an_error(self, report):
        # Finding 3's rule: `0 0` is the evidence a push landed. `owed` was
        # pushed and reads it; `ahead-of-remote` was pushed too and does not.
        assert divergence
        assert _finding(report, "owed").ahead_of_remote == 0
        assert _finding(report, "owed").behind_remote == 0


class TestFinding4SafeToDeleteIsNotContentLanded:
    def test_a_fully_pushed_branch_whose_content_has_not_landed_is_owed(self, report):
        assert _verdict(report, "owed") == OWED

    def test_it_lists_the_commits_so_the_decision_can_be_made_per_commit(self, report):
        finding = _finding(report, "owed")
        assert [commit.subject for commit in finding.unlanded] == ["owed"]

    def test_the_report_prints_those_commits(self, report):
        assert "commits not on `origin/master`" in render(report)


class TestFinding5StashesAreNotBranches:
    def test_the_stash_is_reported(self, report):
        assert len(report.stashes) == 1
        assert "work in progress" in report.stashes[0]

    def test_no_stash_is_ever_proposed_for_deletion(self, report):
        assert all("stash" not in branch.name for branch in report.deletable)

    def test_the_report_says_they_are_invisible_to_branch_commands(self, report):
        assert "Invisible to every `git branch` command" in render(report)


class TestFinding6ProtectedRefs:
    def test_the_trunk_and_the_current_branch_are_protected(self, report):
        assert _verdict(report, "master") == PROTECTED

    def test_a_branch_held_by_another_worktree_is_protected(self, report):
        assert _verdict(report, "held-by-worktree") == PROTECTED

    def test_each_protection_says_which_one_applies(self, report):
        # Four protections with one shared message is a report nobody can act
        # on: a worktree can be removed and HEAD can be switched, so which one
        # holds a branch decides what the user does next.
        assert _finding(report, "master").reason == "the trunk"
        assert "worktree" in _finding(report, "held-by-worktree").reason

    def test_an_identical_branch_no_worktree_holds_is_deletable(self, report):
        # The control: `held-by-worktree` and `no-commits-of-its-own` point at
        # the same commit, so only the worktree explains the difference.
        assert _verdict(report, "no-commits-of-its-own") == DELETABLE

    def test_a_branch_with_an_open_pull_request_is_protected(self, constructed):
        pr_report = audit(constructed, pr_heads=["landed-on-remote-trunk-only"])
        assert _verdict(pr_report, "landed-on-remote-trunk-only") == PROTECTED

    def test_unlistable_pull_requests_refuse_every_branch(self, constructed):
        # `None` is not "no open PRs". Deleting the head of an open PR closes
        # it, so a PR state that cannot be read authorises nothing.
        blind = audit(constructed, pr_heads=None)
        assert blind.deletable == []
        assert _verdict(blind, "landed-on-remote-trunk-only") == UNKNOWN


class TestAStaleTrunkAuthorisesNothing:
    def test_skipping_the_fetch_makes_every_verdict_unknown(self, constructed):
        stale = audit(constructed, fetch=False, pr_heads=[])
        assert stale.deletable == []
        assert {branch.verdict for branch in stale.branches} == {UNKNOWN}

    def test_a_failed_fetch_is_not_treated_as_a_successful_one(self, constructed):
        broken = audit(constructed, remote="no-such-remote", pr_heads=[])
        assert broken.fetched is False
        assert broken.deletable == []

    def test_the_report_says_why_no_verdict_was_computed(self, constructed):
        stale = audit(constructed, fetch=False, pr_heads=[])
        assert "No verdict was computed" in render(stale)


class TestTheReportPrintsWhereItIsRun:
    def test_the_rendered_report_is_ascii(self, report):
        # This is not cosmetic. `print()` of a report containing one U+2192
        # raised UnicodeEncodeError the first time the script was run for real,
        # against a repository whose worktree list happened to be non-empty --
        # which the constructed fixture also has. The prose in this repository
        # uses em dashes freely and should; a stream whose encoding is the
        # console's rather than the file's cannot afford them.
        assert render(report).isascii()


class TestTheAuditIsReadOnly:
    def test_no_local_ref_or_stash_changes_across_a_run(self, constructed):
        def snapshot() -> tuple[str, str]:
            return (
                _git(constructed, "for-each-ref", "refs/heads/"),
                _git(constructed, "stash", "list"),
            )

        before = snapshot()
        audit(constructed, pr_heads=[])
        assert snapshot() == before


class TestTheCommandLine:
    def test_json_output_round_trips(self, report):
        payload = json.loads(json.dumps(to_dict(report)))
        assert payload["trunk_ref"] == "origin/master"
        assert {"name", "verdict", "reason"} <= set(payload["branches"][0])

    def test_the_script_runs_as_a_subprocess(self, constructed):
        completed = subprocess.run(
            [
                sys.executable,
                "scripts/audit_git_refs.py",
                "--repo",
                str(constructed),
                "--no-fetch",
                "--assume-no-open-prs",
                "--json",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert completed.returncode == 0, completed.stderr
        payload = json.loads(completed.stdout)
        assert payload["fetched"] is False
        assert all(branch["verdict"] == UNKNOWN for branch in payload["branches"])
