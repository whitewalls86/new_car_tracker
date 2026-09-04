"""Hold a commit until the ``commit-plan-attribution`` skill has run for it.

A ``PreToolUse`` hook on ``Bash``, and the counterpart to
``.githooks/commit-msg``. The two enforce different halves of the same
contract:

- **this script** enforces that the *skill was invoked* for this commit. The
  skill writes a stamp when it runs; a ``git commit`` with no stamp is refused.
- **the git hook** enforces that the *message carries attribution*, inside
  ``git commit``, whoever typed it and however the command was assembled.

**Why the skill and not just the message.** A message can be given the right
shape by hand while the mapping in it was never checked against
``docs/PLANS.md``. The skill is what confirms the plan, and a check you must
remember to run is one that stops running -- this repository has the evidence.

**Why a stamp survives bundling.** Plan 175 closed a hole where
``public_surface_gate.py`` matched ``git commit`` in the command and then read
``git diff --cached``, which a bundled ``git add … && git commit`` has not
populated yet. This gate reads no index. Bundling defeats index reads; it does
not defeat "did the skill run".

**The stamp is consumed by one commit.** ``.githooks/commit-msg`` deletes it
once the message passes, so the next commit needs its own invocation. A stamp
that outlived its commit would make the rule "once per session", which is the
rule that failed.
"""
from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

STAMP_NAME = "commit-attribution-stamp"

# Matches ``git commit`` only where a command can actually start: the beginning
# of the string, after a separator, or after a shell keyword. Anchoring matters
# because a commit message quoting `git commit` -- which the messages this very
# gate demands frequently do -- must not be read as a second commit.
COMMIT = re.compile(
    r"(?:^|[;&|(]|\b(?:then|else|do)\s)"      # a place a command can start
    r"\s*(?:[\w.]+=\S+\s+)*"                  # VAR=x prefixes
    r"git\s+"
    r"(?:(?!commit(?=\s|$))[^\s;&|]+\s+)*"    # global options, e.g. -C <path>
    r"commit(?=\s|$)"                         # a whole token, not commit.gpgsign
)

MESSAGE = """The `commit-plan-attribution` skill has not run for this commit.

Invoke it, and let it make the commit. It resolves the work to a plan in
docs/PLANS.md and writes that attribution into the message, which is the only
thing `plan-week` can read it from later.

This fires on every `git commit`, not once per session: the stamp is consumed
by the commit it was written for, so each commit is its own invocation.

If this work genuinely belongs to no plan, the skill is still what records
that -- its message carries an explicit `No plan -- <reason>` line."""


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    ).stdout


def strip_heredocs(command: str) -> str:
    """Remove heredoc bodies before looking for a command in the text.

    A commit message is passed to git *inside* the command string, and the
    messages this repository writes quote shell verbatim. Twice in one session
    a heredoc body containing the words ``git commit`` tripped a gate keyed on
    a bare substring search, so the body is removed rather than searched.
    """
    out, lines, i = [], command.split("\n"), 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        opener = re.search(r"<<-?\s*['\"]?(\w+)['\"]?", line)
        i += 1
        if opener:
            terminator = opener.group(1)
            while i < len(lines) and lines[i].strip() != terminator:
                i += 1
            i += 1  # drop the terminator itself
    return "\n".join(out)


def stamp_path() -> pathlib.Path:
    git_dir = _git("rev-parse", "--git-dir").strip() or ".git"
    return pathlib.Path(git_dir) / STAMP_NAME


def main(argv: list[str] | None = None) -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        # Same argument as the public-surface gate: a hook that cannot read its
        # input must not block every command in the session.
        return 0

    command = payload.get("tool_input", {}).get("command", "")
    if not COMMIT.search(strip_heredocs(command)):
        return 0

    if stamp_path().exists():
        return 0

    print(MESSAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
