#!/usr/bin/env python3
"""Hold the settings that keep git refs from accreting, against the real remote.

Plan 164 Stage 3. ``scripts/audit_git_refs.py`` *reports* configuration drift to
whoever runs it; nothing made that drift fail. This does, in CI, and it checks
the half of the configuration that is **shared**: the remote is one object that
both machines talk to, so a fact asserted about it holds for all of them at
once.

What this deliberately does **not** check, and why:

* **Local branch state.** A CI runner clones fresh and has one branch, so a test
  reading ``git branch`` would pass forever regardless of what either developer
  machine looks like. That is `docs/TESTING.md`'s "the harness must not decide
  the outcome" in its dangerous direction -- a false green -- and it is the
  exact shape that let `test_planning_docs.py` pass on one checkout path and
  fail on another. Local refs are reported by the audit and enforced by nothing,
  on purpose.
* **``fetch.prune`` and ``push.autoSetupRemote``.** Per machine, invisible from
  here, and *not* correctness properties once the remote is clean: their absence
  costs a stale tracking ref, not an accreting remote. The audit names them
  wherever it runs, which is the only place that can see them.

The three settings this does hold, and what each is worth:

1. ``delete_branch_on_merge`` -- the whole cause. With it off, 13 merged head
   branches a day survive forever; measured 2026-09-04, 67 of 69 remote branches
   were already merged.
2. ``allow_squash_merge`` and ``allow_rebase_merge`` -- both land a PR without a
   merge commit. The merge commit's subject
   (``Merge pull request #296 from whitewalls86/docs/...``) is the only
   permanent record of a branch name, and it is what attributed 9 of 209 commits
   in the week measured for `plan-week`. Turning either on removes a recap
   fallback silently.
3. The accretion itself, as a ratcheted budget. Settings are the cause, but a
   count is the symptom, and a symptom check catches paths the cause check does
   not -- a branch pushed and merged outside a PR is never auto-deleted.

Usage::

    python scripts/verify_git_ref_hygiene_contract.py
    python scripts/verify_git_ref_hygiene_contract.py --repo owner/name
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# The remote settings that must hold, as {field: required value}. Read as a
# mapping rather than three ifs so the failure message can name every violation
# in one run instead of one per fix-and-push cycle.
REQUIRED_REMOTE_SETTINGS = {
    "deleteBranchOnMerge": True,
    "squashMergeAllowed": False,
    "rebaseMergeAllowed": False,
}

# A ratchet, dated and owned, in the shape `docs/TESTING.md` sanctions: it only
# ever shrinks. It is not a list you can append to -- it is one number, and
# raising it is a visible edit that has to be argued for in a diff.
#
#   2026-09-04  67  measured before any of this landed; `delete_branch_on_merge`
#                   had never been on, so every merged head branch since the
#                   repository's start was still there.
#   2026-09-04   5  after the one-time backfill swept 65 of them. The true
#                   count is 2 -- both merged branches an active worktree is
#                   sitting on, which the sweep deliberately left. The three
#                   of headroom absorbs a merge landing between GitHub
#                   deleting the head branch and this job reading the remote;
#                   it does not absorb a regression, which is 13 a day.
MERGED_BRANCH_BUDGET = 5
BUDGET_SET_ON = "2026-09-04"


class VerificationError(RuntimeError):
    """The remote is configured to accrete refs."""


def check_settings(payload: dict) -> list[str]:
    """Violations in a repository settings payload. Pure; no network."""
    problems = []
    for field, required in REQUIRED_REMOTE_SETTINGS.items():
        actual = payload.get(field)
        if actual is None:
            # Fail closed. A field the API did not return is a field this cannot
            # vouch for, and "absent" must never read as "satisfied".
            problems.append(f"{field}: not present in the API response, so it cannot be checked")
        elif actual != required:
            problems.append(f"{field}: is {actual!r}, must be {required!r}")
    return problems


def check_accretion(merged_branches: list[str], budget: int = MERGED_BRANCH_BUDGET) -> list[str]:
    """Violations from the count of merged-but-undeleted remote branches. Pure."""
    if len(merged_branches) <= budget:
        return []
    excess = len(merged_branches) - budget
    listed = ", ".join(sorted(merged_branches)[:10])
    return [
        f"{len(merged_branches)} remote branches are fully merged into the trunk and "
        f"still exist, which is {excess} over the budget of {budget} set on "
        f"{BUDGET_SET_ON}. Either the merge settings regressed, or branches are "
        f"landing outside a pull request. First ten: {listed}"
    ]


def remote_settings(repo: str | None = None) -> dict:
    """The live repository settings, via ``gh``."""
    args = ["gh", "repo", "view"]
    if repo:
        args.append(repo)
    args += ["--json", ",".join(REQUIRED_REMOTE_SETTINGS)]
    completed = subprocess.run(
        args, cwd=str(REPO_ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if completed.returncode != 0:
        raise VerificationError(f"could not read repository settings: {completed.stderr.strip()}")
    return json.loads(completed.stdout or "{}")


def merged_remote_branches(remote: str = "origin", trunk: str = "master") -> list[str]:
    """Remote branches whose tip is already an ancestor of the trunk.

    Ancestry, not patch identity, and deliberately so: this counts refs that
    carry **nothing unique at all**, which is the population
    ``delete_branch_on_merge`` is supposed to have removed. A replayed-patch
    branch is a judgement call for a person; this is not.
    """
    trunk_ref = f"{remote}/{trunk}"
    listing = subprocess.run(
        ["git", "for-each-ref", "--format=%(refname:short)", f"refs/remotes/{remote}/"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if listing.returncode != 0:
        raise VerificationError(f"could not list remote branches: {listing.stderr.strip()}")

    candidates = [
        ref for ref in listing.stdout.split() if ref != trunk_ref and not ref.endswith("/HEAD")
    ]
    if not candidates and not listing.stdout.split():
        # Fail closed on an empty scan. A shallow or single-branch clone has no
        # remote-tracking refs at all, and an accretion check that silently
        # counts zero on such a checkout reports green forever -- the false
        # green `docs/TESTING.md` calls the dangerous direction. CI must use
        # `fetch-depth: 0`, and this is what says so when it does not.
        raise VerificationError(
            f"no remote-tracking refs under refs/remotes/{remote}/ -- this is a shallow or "
            "single-branch checkout, so the accretion count would be meaningless. "
            "Use actions/checkout with fetch-depth: 0."
        )

    merged = []
    for ref in candidates:
        contained = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ref, trunk_ref],
            cwd=str(REPO_ROOT),
            capture_output=True,
        )
        if contained.returncode == 0:
            merged.append(ref.removeprefix(f"{remote}/"))
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=None, help="owner/name; defaults to the checkout's")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--trunk", default="master")
    args = parser.parse_args(argv)

    try:
        problems = check_settings(remote_settings(args.repo))
        merged = merged_remote_branches(args.remote, args.trunk)
        problems += check_accretion(merged)
    except VerificationError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1

    if problems:
        print("FAIL: the remote is configured to accrete git refs.\n", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\nThese are settings on the remote, so fixing them fixes every machine at "
            "once. See .claude/skills/ref-hygiene/SKILL.md and "
            "docs/planning/cycle_close_order.md.",
            file=sys.stderr,
        )
        return 1

    print(
        f"OK: the remote deletes merged head branches, every PR lands as a merge commit, "
        f"and {len(merged)} merged branch(es) remain against a budget of "
        f"{MERGED_BRANCH_BUDGET}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
