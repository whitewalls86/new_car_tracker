#!/usr/bin/env python3
"""Classify every local branch, worktree and stash in a repository. Delete nothing.

Plan 164 Stage 3. The 2026-08-31 cleanup on this machine measured six ways a
naive ``git branch --merged | xargs git branch -d`` gets this wrong, and each
one is encoded below as a refusal rather than as advice:

1. **Local ``master`` was 57 commits behind ``origin/master``.** Every verdict
   taken against local ``master`` would have been wrong in the dangerous
   direction, so the trunk this compares against is always
   ``<remote>/<trunk>`` after a ``fetch --prune``. A fetch that does not happen
   makes every verdict ``unknown``, and unknown refuses.
2. **``--merged`` under-reports.** Seven commits on one branch were
   byte-identical patches already on master under different SHAs; ancestry
   called the branch unmerged. ``git cherry`` compares patch identity, so it is
   the test used here and ancestry is not consulted at all.
3. **A branch with no remote ref may hold the only copy in existence.** Such a
   branch is ``unpushed`` and is refused; the operator pushes it and verifies
   the push with ``rev-list --left-right --count`` reading ``0 0``, which is
   what this re-checks on the next run.
4. **Safe to delete is not the same as content landed.** A branch whose commits
   are on its remote but not on the trunk is ``owed``: its commits are listed
   individually, because the landed/superseded/owed decision is per commit and
   is a judgement this script does not make.
5. **Stashes are invisible to every ``git branch`` command.** They are reported
   and never proposed for anything.
6. **The current branch, the trunk, a branch checked out in another worktree,
   and a branch with an open PR are protected**, whatever their contents.

A seventh finding is why this script exists at all rather than a shell one-liner:
``git branch -d``'s safety check runs against the branch's configured *upstream*
when it has one and against ``HEAD`` when it does not, so it returns opposite
verdicts for branches in identical states. This script never asks ``-d`` for a
verdict; it computes one.

Usage::

    python scripts/audit_git_refs.py                 # markdown report
    python scripts/audit_git_refs.py --json          # machine-readable
    python scripts/audit_git_refs.py --no-fetch      # every verdict is unknown
    python scripts/audit_git_refs.py --repo <path>   # audit another checkout
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Verdicts. Only DELETABLE authorises anything; every other value refuses, and
# a value this script cannot compute is UNKNOWN rather than an optimistic guess.
DELETABLE = "landed"
PROTECTED = "protected"
UNPUSHED = "unpushed"
OWED = "owed"
UNKNOWN = "unknown"

REFUSAL_REASONS = {
    PROTECTED: "protected: never delete this ref",
    UNPUSHED: "no verified remote copy: push it and verify the push first",
    OWED: "content has not landed on the trunk: decide per commit",
    UNKNOWN: "could not establish the facts this verdict needs",
}


class GitError(RuntimeError):
    """A git invocation this script depends on failed."""


@dataclass
class Commit:
    sha: str
    subject: str
    landed: bool


@dataclass
class BranchFinding:
    name: str
    verdict: str
    reason: str
    upstream: str | None = None
    remote_ref: str | None = None
    ahead_of_remote: int = 0
    behind_remote: int = 0
    unlanded: list[Commit] = field(default_factory=list)

    @property
    def deletable(self) -> bool:
        return self.verdict == DELETABLE


@dataclass
class Report:
    trunk_ref: str
    fetched: bool
    fetch_error: str | None
    pr_heads: list[str] | None
    branches: list[BranchFinding]
    stashes: list[str]
    worktrees: list[dict]

    @property
    def deletable(self) -> list[BranchFinding]:
        return [b for b in self.branches if b.deletable]

    @property
    def refused(self) -> list[BranchFinding]:
        return [b for b in self.branches if not b.deletable]


def git(repo: Path, *args: str, check: bool = True) -> str:
    """Run one git command in ``repo`` and return its stdout."""
    completed = subprocess.run(
        ["git", *args],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and completed.returncode != 0:
        raise GitError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout


def _lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.strip()]


def open_pr_heads_from_gh(repo: Path) -> list[str] | None:
    """Head branch names of open PRs, or ``None`` when that cannot be established.

    ``None`` is not "no open PRs". It propagates into every branch verdict as
    UNKNOWN, because a branch deleted while a PR is open closes the PR.
    """
    if shutil.which("gh") is None:
        return None
    completed = subprocess.run(
        ["gh", "pr", "list", "--state", "open", "--limit", "200", "--json", "headRefName"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError:
        return None
    return [entry["headRefName"] for entry in payload if entry.get("headRefName")]


def current_branch(repo: Path) -> str | None:
    """The checked-out branch, or ``None`` when HEAD is detached."""
    completed = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def worktrees(repo: Path) -> list[dict]:
    """Every worktree of this repository, with the branch each holds."""
    out = []
    entry: dict = {}
    for line in git(repo, "worktree", "list", "--porcelain").splitlines():
        if not line.strip():
            if entry:
                out.append(entry)
                entry = {}
            continue
        key, _, value = line.partition(" ")
        if key == "worktree":
            entry = {"path": value, "branch": None, "prunable": False, "locked": False}
        elif key == "branch":
            entry["branch"] = value.removeprefix("refs/heads/")
        elif key in ("prunable", "locked"):
            entry[key] = True
    if entry:
        out.append(entry)
    return out


def stashes(repo: Path) -> list[str]:
    """Stash entries, which no ``git branch`` command can see. Report only."""
    return _lines(git(repo, "stash", "list", "--format=%gd %h %gs"))


def local_branches(repo: Path) -> list[tuple[str, str | None]]:
    """``(name, configured upstream)`` for every local branch."""
    fmt = "%(refname:short)%09%(upstream:short)"
    out = []
    for line in _lines(git(repo, "for-each-ref", f"--format={fmt}", "refs/heads/")):
        name, _, upstream = line.partition("\t")
        out.append((name, upstream or None))
    return out


def ref_exists(repo: Path, ref: str) -> bool:
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.returncode == 0


def cherry(repo: Path, trunk_ref: str, branch: str) -> list[Commit]:
    """Every commit on ``branch`` and not on ``trunk_ref``, by *patch identity*.

    ``git cherry`` prefixes a commit with ``-`` when an equivalent patch is
    already on the upstream and ``+`` when it is not. Ancestry never enters
    into it, which is finding 2: seven byte-identical patches replayed under new
    SHAs are ``-`` here and invisible to ``--merged``.
    """
    commits = []
    for line in _lines(git(repo, "cherry", "-v", trunk_ref, branch)):
        mark, _, rest = line.partition(" ")
        sha, _, subject = rest.partition(" ")
        commits.append(Commit(sha=sha, subject=subject.strip(), landed=mark == "-"))
    return commits


def divergence(repo: Path, left: str, right: str) -> tuple[int, int]:
    """``(ahead, behind)`` between two refs.

    This is the verification finding 3 demands: the absence of an error from
    ``git push`` is not evidence the push happened, and ``0 0`` is.
    """
    out = git(repo, "rev-list", "--left-right", "--count", f"{left}...{right}").split()
    return int(out[0]), int(out[1])


def classify_branch(
    repo: Path,
    name: str,
    upstream: str | None,
    *,
    trunk_ref: str,
    remote: str,
    protected_names: dict[str, str],
    pr_heads: list[str] | None,
) -> BranchFinding:
    """One branch's verdict. Protection first, then landedness, then push state."""
    if name in protected_names:
        return BranchFinding(name, PROTECTED, protected_names[name], upstream)

    if pr_heads is None:
        return BranchFinding(name, UNKNOWN, "open pull requests could not be listed", upstream)
    if name in pr_heads:
        return BranchFinding(name, PROTECTED, "an open pull request has this head", upstream)

    commits = cherry(repo, trunk_ref, name)
    unlanded = [commit for commit in commits if not commit.landed]
    remote_ref = f"{remote}/{name}"
    has_remote = ref_exists(repo, remote_ref)

    if not unlanded:
        reason = (
            f"every commit is on {trunk_ref} by patch identity"
            if commits
            else f"no commits of its own relative to {trunk_ref}"
        )
        return BranchFinding(name, DELETABLE, reason, upstream, remote_ref if has_remote else None)

    if not has_remote:
        return BranchFinding(
            name,
            UNPUSHED,
            f"{len(unlanded)} unlanded commit(s) and no {remote_ref}: this may be the only copy",
            upstream,
            None,
            unlanded=unlanded,
        )

    ahead, behind = divergence(repo, name, remote_ref)
    if ahead:
        return BranchFinding(
            name,
            UNPUSHED,
            f"{ahead} commit(s) ahead of {remote_ref}: the remote copy is incomplete",
            upstream,
            remote_ref,
            ahead,
            behind,
            unlanded,
        )

    return BranchFinding(
        name,
        OWED,
        f"{len(unlanded)} commit(s) not on {trunk_ref}: decide landed/superseded/owed per commit",
        upstream,
        remote_ref,
        ahead,
        behind,
        unlanded,
    )


def audit(
    repo: Path,
    *,
    remote: str = "origin",
    trunk: str = "master",
    fetch: bool = True,
    pr_heads: list[str] | None | str = "gh",
) -> Report:
    """Classify every ref in ``repo``. Reads only; nothing here writes history.

    ``fetch`` runs ``fetch --prune`` first. When it is off, or when it fails,
    every branch verdict is UNKNOWN — finding 1 is that a stale trunk is wrong
    in the dangerous direction, so a trunk that has not been refreshed
    authorises nothing.
    """
    trunk_ref = f"{remote}/{trunk}"
    fetched = False
    fetch_error: str | None = None

    if fetch:
        completed = subprocess.run(
            ["git", "fetch", "--prune", remote],
            cwd=str(repo),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if completed.returncode == 0:
            fetched = True
        else:
            fetch_error = completed.stderr.strip() or f"git fetch --prune {remote} failed"
    else:
        fetch_error = "--no-fetch: the trunk was not refreshed"

    if fetched and not ref_exists(repo, trunk_ref):
        fetched = False
        fetch_error = f"{trunk_ref} does not exist after fetching"

    if pr_heads == "gh":
        resolved_pr_heads = open_pr_heads_from_gh(repo)
    else:
        resolved_pr_heads = pr_heads

    trees = worktrees(repo)
    # Names a protection applies to, each carrying the reason it applies. A
    # single "protected" line covering seven branches for four different
    # reasons is a report nobody can act on, and the reason is the part the
    # user needs: a worktree can be removed, HEAD can be switched, the trunk
    # never moves.
    protected_names = {}
    for tree in trees:
        if tree["branch"]:
            protected_names[tree["branch"]] = f"checked out by the worktree at {tree['path']}"
    head = current_branch(repo)
    if head:
        protected_names[head] = "the current branch"
    # The trunk is written last so it wins: it is protected for a reason that
    # will not stop being true, unlike a worktree or a checkout.
    protected_names.update({name: "the trunk" for name in (trunk, "main", "master")})

    branches = []
    for name, upstream in local_branches(repo):
        if not fetched:
            branches.append(
                BranchFinding(name, UNKNOWN, fetch_error or "the trunk was not refreshed", upstream)
            )
            continue
        branches.append(
            classify_branch(
                repo,
                name,
                upstream,
                trunk_ref=trunk_ref,
                remote=remote,
                protected_names=protected_names,
                pr_heads=resolved_pr_heads,
            )
        )

    return Report(
        trunk_ref=trunk_ref,
        fetched=fetched,
        fetch_error=fetch_error,
        pr_heads=resolved_pr_heads,
        branches=branches,
        stashes=stashes(repo),
        worktrees=trees,
    )


def to_dict(report: Report) -> dict:
    return {
        "trunk_ref": report.trunk_ref,
        "fetched": report.fetched,
        "fetch_error": report.fetch_error,
        "pr_heads": report.pr_heads,
        "branches": [
            {
                "name": branch.name,
                "verdict": branch.verdict,
                "reason": branch.reason,
                "upstream": branch.upstream,
                "remote_ref": branch.remote_ref,
                "ahead_of_remote": branch.ahead_of_remote,
                "behind_remote": branch.behind_remote,
                "unlanded": [
                    {"sha": commit.sha, "subject": commit.subject} for commit in branch.unlanded
                ],
            }
            for branch in report.branches
        ],
        "stashes": report.stashes,
        "worktrees": report.worktrees,
    }


def render(report: Report) -> str:
    out = ["# Git ref audit", ""]
    if report.fetched:
        out.append(f"Compared against `{report.trunk_ref}`, refreshed by `fetch --prune`.")
    else:
        out.append(
            f"**No verdict was computed.** `{report.trunk_ref}` was not refreshed "
            f"({report.fetch_error}), and a stale trunk is wrong in the dangerous direction."
        )
    if report.pr_heads is None:
        out.append(
            "**Open pull requests could not be listed**, so no branch can be cleared: "
            "deleting the head of an open PR closes it."
        )
    out.append("")

    out.append("## Safe to delete")
    out.append("")
    if report.deletable:
        out.append("| Branch | Why |")
        out.append("|---|---|")
        for branch in report.deletable:
            out.append(f"| `{branch.name}` | {branch.reason} |")
    else:
        out.append("Nothing.")
    out.append("")

    out.append("## Refused")
    out.append("")
    if report.refused:
        out.append("| Branch | Verdict | Why |")
        out.append("|---|---|---|")
        for branch in report.refused:
            out.append(f"| `{branch.name}` | {branch.verdict} | {branch.reason} |")
        out.append("")
        for branch in report.refused:
            if not branch.unlanded:
                continue
            out.append(f"### `{branch.name}` - commits not on `{report.trunk_ref}`")
            out.append("")
            for commit in branch.unlanded:
                out.append(f"- `{commit.sha}` {commit.subject}")
            out.append("")
    else:
        out.append("Nothing.")
    out.append("")

    out.append("## Report only - never touched by this step")
    out.append("")
    out.append(f"**Stashes ({len(report.stashes)}).** Invisible to every `git branch` command.")
    for entry in report.stashes:
        out.append(f"- {entry}")
    out.append("")
    out.append(f"**Worktrees ({len(report.worktrees)}).** Their branches are protected.")
    for tree in report.worktrees:
        flags = [key for key in ("prunable", "locked") if tree.get(key)]
        suffix = f" ({', '.join(flags)})" if flags else ""
        out.append(f"- `{tree['path']}` -> `{tree['branch'] or 'detached'}`{suffix}")
    out.append("")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=str(REPO_ROOT), help="repository to audit")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--trunk", default="master")
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="skip the fetch; every verdict becomes unknown, which refuses",
    )
    parser.add_argument(
        "--assume-no-open-prs",
        action="store_true",
        help="declare that no pull request is open, when `gh` is unavailable",
    )
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    report = audit(
        Path(args.repo),
        remote=args.remote,
        trunk=args.trunk,
        fetch=not args.no_fetch,
        pr_heads=[] if args.assume_no_open_prs else "gh",
    )
    print(json.dumps(to_dict(report), indent=2) if args.json else render(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
