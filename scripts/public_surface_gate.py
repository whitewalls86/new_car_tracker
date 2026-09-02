"""Plan 138 Stage 1c: hold a commit that edits a public surface until it is read.

A ``PreToolUse`` hook on ``Bash``. It blocks a ``git commit`` that stages
``README.md`` or ``ops/templates/info.html`` until the ``public-surface-check``
skill has run against exactly that staged content.

**Why a hook and not a test.** Stage 1c first tried tests. Every drift this plan
has actually caught -- "36 Flyway migrations" against 49, "971 tests" against
3,661, "eleven Docker containers" against more than two dozen -- was a surface
disagreeing with the *repository*, and none of it is a wording a phrase list
could hold. Judging whether a claim is still true needs the tree read with
judgment, which is a skill's job. But a check you must remember is weaker than
one you cannot forget, and Plan 146 exists because things vanished when nothing
forced attention. The hook is what forces it.

**The stamp is what makes it satisfiable.** A gate that fires on every commit
and cannot be cleared is one you learn to route around. So the skill records the
digest of the staged diff it read, and this script clears the gate for that
digest alone: re-stage a further edit and the digest changes, so the gate closes
again. Passing once does not buy passage for content nobody looked at.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import subprocess
import sys

# The two surfaces Plan 138 owns. Deliberately named, not discovered: the skill
# they trigger is expensive, and a wildcard over docs/ would fire it on work
# that has no public surface in it at all.
SURFACES = ("README.md", "ops/templates/info.html")

STAMP_NAME = "public-surface-stamp"


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False
    , encoding="utf-8").stdout


def staged_digest(paths: list[str]) -> str:
    """Identify the staged content, so the stamp cannot outlive it."""
    return hashlib.sha256(_git("diff", "--cached", "--", *paths).encode()).hexdigest()


def stamp_path() -> pathlib.Path:
    # --git-dir resolves to the worktree's own admin directory, so a stamp set
    # in one worktree never clears the gate in another.
    git_dir = _git("rev-parse", "--git-dir").strip() or ".git"
    return pathlib.Path(git_dir) / STAMP_NAME


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        # A hook that cannot parse its input must not block work. Failing open
        # is right here: the cost of a missed check is a review catch, and the
        # cost of failing closed is an unclearable gate on every commit.
        return 0

    command = payload.get("tool_input", {}).get("command", "")
    if "git commit" not in command:
        return 0

    staged = _git("diff", "--cached", "--name-only").split()
    touched = [surface for surface in SURFACES if surface in staged]
    if not touched:
        return 0

    digest = staged_digest(touched)
    stamp = stamp_path()
    if stamp.exists() and stamp.read_text(encoding="utf-8").strip() == digest:
        return 0

    print(
        "Plan 138 Stage 1c: this commit stages a public surface -- "
        + ", ".join(touched)
        + ".\n\n"
        "Run the `public-surface-check` skill before committing. It reads the "
        "staged diff only, checks the changed claims against the repository "
        "and against the other surface, and stamps this exact staged content "
        "when it is done.\n\n"
        "If the check has already run and you have since re-staged, run it "
        "again -- the content it read is no longer the content being "
        "committed.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
