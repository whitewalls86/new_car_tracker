# Plan 175: Close the Commit Gate's Bundling Hole

## What this plan is for

A check holds a commit that edits this project's public copy until that change
has been read against the rest of the repository. It only sees edits staged by
an earlier command, so a commit that stages its own changes passes unread. This
plan closes that hole.

## The case

**Found on 2026-09-04**, while reading the gate that
[Plan 138](plan_138_public_surface_refresh.md) Stage 10 had just given its
first tests (PR #364, merged the same day).

[`scripts/public_surface_gate.py`](../../scripts/public_surface_gate.py) is a
`PreToolUse` hook on `Bash`, registered in `.claude/settings.json`. It blocks a
`git commit` that stages `README.md` or `ops/templates/info.html` until the
`public-surface-check` skill has read that exact staged content and stamped its
digest. `PreToolUse` runs *before* the command executes, and the gate reads
`git diff --cached --name-only`. So what it sees depends entirely on how the
commit was invoked:

| Invocation | Index at hook time | Result |
|---|---|---|
| `git add README.md`, then a separate `git commit` | holds `README.md` | fires correctly |
| `git add README.md && git commit -m x`, one call | empty | **silent pass** |
| `git commit -am "x"` — `-a` stages during the commit | empty | **silent pass** |

**The hole is worse than the gate not existing, because it looks like a pass.**
A gate that never fired would leave the author knowing the check is theirs to
remember. This one fires, finds nothing, and returns 0, which is
indistinguishable from a surface that was read and cleared. Stage 1c's own
argument for building the hook was that "a check you must remember is weaker
than one you cannot forget"; a check that silently reports success when it
looked at nothing is weaker than either.

Nothing records this today. Gap **P4** in
[`docs/PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md#the-gap-list) records a
*different* limit of the same hook — that a `PreToolUse` hook holds an agent's
commits and not one typed in a terminal — and Stage 10 narrowed and re-argued
that entry on 2026-09-04 without noticing the bundling case sitting next to it.
The gate's nine tests, written in the same stage, all drive `main()` with an
index that is already populated, so none of them can see it either.

**The durable fix is cheap to reach, and does not exist yet.** A real
`pre-commit` hook runs inside `git commit`, after `-a` staging, whichever way
the command was assembled and whoever typed it — which closes the bundling
case, the `commit -am` case, and P4's recorded agent-only residue together.
There is no such hook here now: `.git/hooks/` holds only the stock `.sample`
files and `core.hooksPath` is unset, so nothing is being replaced. The cost is
that git does not track hooks, so a tracked hooks directory needs a one-time
`git config` in every clone and worktree — a real setup burden in a repository
that is developed on two machines and does most of its work in worktrees, and
the thing to argue about before committing to it.

## Design

**Enforcement moves inside `git commit`.** A tracked `.githooks/pre-commit`
runs the same stamp check, and `core.hooksPath` points at it. Git runs a
`pre-commit` hook *after* `-a` has staged, so the index it reads is the index
that will be committed — which is precisely what the `PreToolUse` hook cannot
see. How the command was assembled then stops mattering: bundled or separate,
`-am` or not, an agent's `Bash` call or a person at a terminal.

The check is shared, not reimplemented.
[`scripts/public_surface_gate.py`](../../scripts/public_surface_gate.py)
already knows the two surfaces, the digest and the stamp; the hook is a second
entry point into that module, not a second copy of its logic. Two files that
must agree about which surfaces are guarded is the same class of defect this
plan is closing.

### Setup is one command per clone, not one per worktree

Git does not track hooks, so `core.hooksPath` has to be set locally — the cost
that has to be worth paying. It is smaller than it looks. Config is shared
across a clone's linked worktrees: from
`.claude/worktrees/car-41-plan-164`, `git config --show-origin` resolves
`core.*` to the main clone's `.git/config`, while `git rev-parse --git-dir`
still returns that worktree's own admin directory. So the hook is installed
once per clone — twice in total, since this repository is developed on two
machines — and the stamp stays per-worktree exactly as it is today, because it
is keyed on `--git-dir`.

### The hook that can be missing is watched by the hook that cannot

A clone that never ran the install has no `pre-commit` hook and says nothing
about it, which is the same silent-absence class as the bug itself. So the
`PreToolUse` hook keeps a job: it refuses, printing the install command, when
`core.hooksPath` is not pointed at the tracked directory. It can do this
because `.claude/settings.json` is tracked, so unlike the git hook it is live
in every clone from the first checkout.

That leaves a person at a terminal in a freshly cloned repository as the one
case nothing catches — narrower than today's, where every clone has that hole.

### Fail closed, with the escape hatch named

The `PreToolUse` hook fails open on unparseable input and argues for it: it
runs before *every* `Bash` call, so a bug there could block all work, and the
cost of a missed check is a review catch. The `pre-commit` hook's economics are
the opposite. It runs only when a commit is happening, its input is git rather
than a JSON payload on stdin, and git already provides `--no-verify` as a
deliberate, visible bypass. A hook that failed open when its interpreter was
missing would rebuild the exact defect this plan exists to close — a check that
looks like a pass while reading nothing.

### What remains, and is accepted

`git commit --no-verify`, and a person committing at a terminal in a clone
where the install was never run. Both are recorded in `docs/PUBLIC_SURFACE.md`
as the residue of gap **P4**, replacing the agent-only limit this plan removes.
Neither is silent in the way the bundling hole is: `--no-verify` is typed on
purpose, and the missing install is loud for every commit an agent makes.

### Rejected on the way here

- **A bundling refusal in the `PreToolUse` hook first, as a stop-gap.** The
  right call if the real fix were weeks out; here both land in one sitting, so
  it would be written and deleted without ever having held anything. Dropping
  it also drops its entire problem class — refusing a bundle means parsing a
  command string, where `--amend` contains `-a` and a heredoc commit body
  quoting ``git add … && git commit`` trips the refusal on its own message. The
  `pre-commit` hook parses nothing; it reads the index.
- **A `permissions.deny` rule on compound `Bash` containing `git commit`.**
  Blunter than either hook, silent about why, and unable to tell a commit that
  touches a surface from one that does not.
- **Widening the gate to hash working-tree content**, so a bundled commit could
  still be stamped. The digest is worth something only because it identifies
  content that was staged *and read*; stamping content that is neither would
  make the gate clearable without anyone looking.
- **Failing open on a missing interpreter**, for symmetry with the existing
  hook. Symmetry is not the goal; not silently passing is.

## Stages

### Stage A — Enforcement moves inside `git commit`

One stage, because the three pieces are one change and no intermediate state is
one anybody would want to stop at: a hook nothing installs, an install check
for a hook that does not exist, or a record describing enforcement that is not
there yet.

Add `.githooks/pre-commit`, calling the existing module; set `core.hooksPath`
and document the one-line install; teach the `PreToolUse` hook to refuse when
that config is absent; shrink gap **P4** and the "what guards each published
surface" table to what is then true.

**Exit:** in a disposable clone with `core.hooksPath` set, a `git commit -am`
typed at a terminal — no agent, no `PreToolUse` hook in the picture — is
refused by git itself when it stages `README.md` with no matching stamp, and
succeeds once the stamp matches. With `core.hooksPath` unset, an agent's
`git commit` staging a surface refuses and prints the exact install command.
Both behaviours are covered by tests watched failing against a deliberate
mutation, and `docs/PUBLIC_SURFACE.md` P4 names only the residue that actually
remains.

| Order | Stage | Estimate | Status |
|---:|---|---:|---|
| 1 | A — Enforcement moves inside `git commit` | 1 | done |

## Public summary

**Commit gate bundling hole** — A check that stops this project's public pages
changing unread could be sidestepped by a common way of writing a commit, and
reported success when it was. Moved the check inside the commit itself, so it
now reads what is actually being committed and the shortcut no longer passes.

## Record

### Stage A — Enforcement moves inside `git commit` — 2026-09-04

Landed in [PR #368](https://github.com/whitewalls86/new_car_tracker/pull/368),
merged `622e3d7`. `.githooks/pre-commit` calls
`scripts/public_surface_gate.py --pre-commit`, and git runs it after `-a` has
staged, so the index it reads is the index being committed. The check is
shared, not copied — `check_staged()` has two entry points. The `PreToolUse`
path keeps the one job a git hook cannot do for itself: refuse any
`git commit` until `core.hooksPath` is set, checked before the index read
rather than after, since a missing install is exactly the case where the index
proves nothing.

**Verified in a disposable clone, with no agent in the picture.** A terminal
`git commit -am` on an unread `README.md` was refused by git itself (exit 1)
and committed once the stamp matched (`ace8270`, exit 0). With
`core.hooksPath` unset the `PreToolUse` path printed the install command and
exited 2. A relative `core.hooksPath` was confirmed to resolve against the
working-tree root rather than the cwd, so a commit made from a subdirectory is
held too. Four deliberate mutations were each caught by the intended
assertion: `hooks_installed()` forced `True`, the install check made
conditional on the index, `--pre-commit` falling through to the payload path,
and `--chmod=-x` on the hook. 3,658 unit tests, `ruff check` clean, CI green on
every job.

**The stage reintroduced its own defect twice, and that is the finding worth
keeping.** First, `chmod +x` on Windows does not reach the index —
`core.fileMode` is off — so the hook committed as `100644`. Git silently
declines to run a hook without the execute bit, so it would have been checked
out on macOS installed, configured, and never firing. Caught by reading
`git ls-files -s`, not by any test; fixed before merge and now asserted on the
tracked mode, watched failing against the real `--chmod=-x`.

Second, and after the merge: `hooks_installed()` compared
`git config core.hooksPath` against the literal string `.githooks`. Within
hours the value in this clone had become an absolute path to the same
directory — rewritten by something outside this work, unexplained — so git was
still running the hook while the gate called it uninstalled and refused every
agent commit. **A false refusal is the one failure this mechanism cannot
afford**, because it is how a gate teaches people to route around it. Fixed in
`d9eaf8d` by resolving the configured value against the working-tree root and
comparing paths, so an absolute path, `./.githooks` and a trailing separator
all read as installed; two tests watched failing against the exact merged bug.
Both defects are the same shape as the one the plan set out to close: a
mechanism that looks installed and is not, or looks broken and is not.

Setup cost is one command per clone, not one per worktree: linked worktrees
share `core.hooksPath`, while the stamp stays per-worktree because it is keyed
on `--git-dir`. That was measured before the design was committed to, against
`.claude/worktrees/car-41-plan-164`, whose `git config --show-origin` resolves
to the main clone's `.git/config`. Gap **P4** in
[`docs/PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md) narrowed accordingly, to
`--no-verify` and a clone where the install was never run.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work. The only quantity in range — "More than 3,000 tests run
in CI", in both — stays true at 3,660, and is deliberately rounded.

Cost: estimate 1 → actual **not measured**. Elapsed from `startedAt` to merge
was 1h14m, which is calendar time and not effort; no effort figure was taken.
