# Plan 164: The Cycle-Close Ritual

## Status

Written 2026-08-31, out of Cycle 1's close. Three steps of a ritual that
already exists in prose have no owner, and all three are only ever correct at
the close — which is what makes them one plan rather than three tickets.

Priority, effort and position are proposed in [`docs/PLANS.md`](../PLANS.md),
which owns all three; this document does not choose them.

## The problem

[Plan 149](plan_149_linear_execution_layer.md) Stage 2 step 6 says to record
the cycle measures "after the cycle closes", so a close ritual is already
specified. Most of it has skills. Some of it does not.

| Cycle-close step | Owner today |
|---|---|
| Close finished issues, write their evidence | `close-out` |
| Move rows in `PLANS.md` and the archive | `plans` |
| Write the weekly recap | `plan-week` |
| Seed the next cycle | `fill-cycle` |
| **Record Plan 149's cycle measures** | **nobody** |
| **Roll unfinished issues to the next cycle** | **nobody** |
| **Git ref and worktree hygiene** | **nobody** |

The three unowned steps share the property that makes them a package: **each is
wrong to perform at any other time.**

- Rolling issues early falsifies Plan 149's own rollover measure — the repo
  already carries that rule, and it is why Cycle 1's two rollovers were left to
  Linear's automatic move at `endsAt` rather than done by hand.
- The measures cannot be read before the close. Cycle 1 proved the cost of
  reading them early: the mid-cycle read of 2026-08-29 recorded ~30 points
  completed and the closed cycle recorded 44, a 47% undercount that was written
  into Stage 1's budget and into two skills before it was caught.
- Branch deletion must come *after* the recap, for the reason in the next
  section.

## This is not a stage of an existing plan

**Not `plan-week`.** [Plan 146](plan_146_planning_system.md) Stage 6 already
states the rule:

> Stage 5's skill writes state and never prose; this one writes prose and never
> state, and the two must not be merged — a tool that both summarises work *and*
> moves rows between tables can move a row because its own summary said so,
> which is a self-confirming record.

Folding branch cleanup into the recap is that violation in a sharper form. The
recap reads git history *as its source of truth*, so a skill that both reads
history to write prose and deletes refs can destroy the evidence for the claim
it just made. The failure would not even look like one: the recap would still
be internally consistent.

**Not Plan 146.** It sits in closeout with a 2026-09-14 gate whose whole subject
is whether the recap habit stuck. Adding a stage reopens a plan to prove
something about its own consistency.

**Not Plan 149.** Its Out of scope list names *"team ceremonies, story-point
accounting or sprint-performance targets"* verbatim, and it is scoped to Linear
as a projection. Git refs are not a projection of anything.

**And the category does not exist yet.** Every skill here declares a "writes X,
never Y" boundary — `docs/` prose, `docs/` state, or Linear. Git refs are a
fourth kind of state, and nothing currently claims it.

## The ordering is the point, and it is why this is a package

`plan-week` already anticipates the deletion:

> The containing branch is a hint, never a source. `git branch --contains`
> answers only while the branch still exists, and merging is supposed to delete
> it. On the measured week the branch layer rescued 18 commits over 30 days.

So the recap is *robust* to deleted branches and still *helped* by live ones —
18 commits over 30 days. That fixes a sequence rather than merging a tool:
**recap first, then clean.** A package with a fixed order is the artifact; one
skill that does both is the thing Plan 146 forbids.

## Stages

### Stage 0 — Write the order down

No code. The close sequence, and for each step the condition that makes it safe
to run: what must have happened before it, and what it must not run before.
This is the stage the other three depend on, and it is the one that makes the
package a package rather than three skills that happen to run on Sunday.

### Stage 1 — Record Plan 149's cycle measures

A skill that reads Linear's closed-cycle history and the repository, proposes
the six measures, and writes them into Plan 149's *Cycle measures* table on
approval. Writes one plan document's evidence table and nothing else.

Two rules Cycle 1 supplies directly:

- **Never read an open cycle.** The mid-cycle read undercounted completion by
  47%.
- **A partial measure is recorded as partial.** Cycle 1's "state corrections"
  row is three known occurrences with no exhaustive count, and says so. A total
  that is not one is worse than an admitted gap.

### Stage 2 — Roll unfinished issues

Cycle 1 rolled two issues, and **Linear did it automatically at `endsAt`** with
no manual action. So the first job of this stage is to establish what is
actually left to do: propose the rollover set, confirm what the platform already
moved, and act only on the remainder.

The standing rule holds — nothing touches a `cycleId` until the cycle has
actually ended.

### Stage 3 — Git ref and worktree hygiene

The one that needs the most care, because its mistakes are unrecoverable.

## What the 2026-08-31 cleanup proved Stage 3 must encode

Every item below was measured on this machine on 2026-08-31, not reasoned out.
A naive `git branch --merged | xargs git branch -d` gets all of them wrong.

**1. Compare against `origin/master` after `fetch --prune`, never local
master.** Local `master` was **57 commits behind** `origin/master` at the time
of the cleanup. Every merged/unmerged verdict taken against it would have been
wrong in the dangerous direction.

**2. `--merged` under-reports, so ancestry is not the test.** Seven commits on
`millerandrewpreston/car-10-plan-141-…` were byte-identical patches already on
master under different SHAs — confirmed pair by pair with
`git patch-id --stable`, not inferred from subjects. Ancestry called that branch
unmerged; deleting it was correct. `git cherry` or patch-id is the test.

*The casualty is worth recording.* Those seven commits are now reachable from no
ref at all. `8688fd9` still resolves out of the object database and will until
gc. That is the correct outcome — but it is also proof that this step destroys
evidence, which is why it goes last.

**3. `--no-merged` may hold the only copy in existence. Push first, then verify
the push.** `feature/plan-125-portability-audit` carries 17 commits and ~4,700
lines. Its remote ref did not exist until the cleanup created it — the reflog
reads `update by push` at **2026-08-31 09:09:13 -0500**. Verification is
`git rev-list --left-right --count <branch>...origin/<branch>` reading `0 0`,
not the absence of an error from `git push`.

**4. A branch that is safe to *delete* is not a branch whose content has
*landed*.** This is the trap that a merge-status check cannot see, and today
produced two opposite answers on two branches that looked alike:

- `docs/plan-149-cycle-1-findings` carried an index true-up that was correct
  when written on 2026-08-29 and **superseded within a day** by sixteen commits.
  Merging its `docs/PLANS.md` would have restored two archived plans to the
  build order. Its *other* commit was worth keeping in full.
- `plan-131-packed-cold-storage` carried one commit from 2026-08-13 whose
  content had **never landed anywhere**, and whose base predated Plan 146
  Stage 3's file move — so the merge resurrected `docs/plan_131_*.md` at a path
  that no longer exists, as a modify/delete conflict.

So the rule is per-commit, not per-branch: for each commit, decide whether its
content **landed**, was **superseded**, or is **owed** — and for owed content
whose base predates a move, relocate it rather than merging it.

**5. Stashes are not branches, and a branch cleanup does not see them.** Six
stashes survive here, four of them naming branches that no longer exist. They
are invisible to every `git branch` command and appear in `git log --all`, which
is how they were found. Report them; never drop one automatically.

**7. `git branch -d`'s safety check is against the branch's configured
*upstream*, not against `HEAD`** — so it returns opposite verdicts for branches
in identical states. Measured deleting these three on 2026-08-31: two had no
upstream, so `-d` tested them against `HEAD`, found them merged, and deleted
them. The third had `branch.<name>.merge = refs/heads/master`, so `-d` tested it
against `origin/master`, and refused — while saying plainly *"not yet merged to
`refs/remotes/origin/master`, even though it is merged to HEAD"*.

The refusal was correct and the message was honest; the trap is the
inconsistency. Two succeed, one fails, and the obvious next move is the one git
suggests in its own hint: `-D`. **A `-d` refusal is a signal to find out which
ref it compared against, never a cue to reach for `-D`.** Establish where the
content actually is first; here it was reachable from the pushed PR branch, and
`-D` was justified only because of that.

This also means an unmerged-PR workflow makes `-d` unreliable in the safe
direction: a branch folded into a review branch is not merged to `master` yet, so
`-d` will refuse or permit it depending on an unrelated config value.

**6. Never touch the current branch or `master`, check open PRs before deleting,
and assume another agent is mid-edit.** The repository already carries the
shared-worktree rule for staging; it applies with more force to ref deletion,
which has no `git status` to inspect first.

## Success criteria

1. The close order is written down, and each step names what must precede it.
2. Plan 149's Cycle 2 measures are recorded by the Stage 1 skill, from a closed
   cycle, with any partial measure marked partial.
3. A cycle close runs end to end in the fixed order, and the recap for that week
   is written before any ref is deleted.
4. Stage 3 refuses, on a real repository, every one of the six cases above —
   demonstrated rather than asserted, in the shape Plan 158 Stage 3 used: a
   deliberately constructed case per rule.
5. No skill in the package both writes prose and deletes state.

## Non-goals

- **Merging any of these into `plan-week`, `plans`, `close-out` or
  `fill-cycle`.** The boundary each declares is the reason they are trustworthy.
- **Automatic branch deletion on a schedule.** Every deletion is proposed and
  approved; the traps above are why.
- **Dropping stashes**, ever, automatically.
- **Garbage collection or history rewriting.** Out of scope entirely.
- **Making the recap read Linear.** Recorded as a possible follow-up in Plan
  149's Cycle 1 measures and deliberately not claimed here; the recap's source
  is git, and that is the property that makes it independent of the board.

## Intersections

### Plan 146 — the planning system

Supplies the boundary rule this plan obeys, and its 2026-09-14 closeout gate is
the reason this is a new plan rather than a Stage 7. Plan 146's own gate is
about the recap habit sticking; nothing here should be allowed to disturb that
measurement.

### Plan 149 — Linear as the execution layer

Supplies the ritual this plan completes and owns the measures Stage 1 writes.
Its Stage 3 keep/change/remove decision lands 2026-09-15; if Linear is removed,
Stages 1 and 2 of this plan go with it and Stage 3 stands alone. That is a
reason to keep Stage 3 independently shippable.

### Plan 142 — scoped operational coordination

Unrelated despite the vocabulary overlap: Plan 142's "worktree" is a checkout on
the production VM, not a `git worktree` on a developer machine. Named here so
the next reader does not have to check.
