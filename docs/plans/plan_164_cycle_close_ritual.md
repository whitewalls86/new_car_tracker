# Plan 164: The Cycle-Close Ritual

## What this plan is for

Three steps of the routine that closes a work cycle — recording how the cycle
actually went, rolling unfinished work forward, and tidying the branches left
behind — have no owner today, and each is only ever correct at the close. This
plan gives all three a home.

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
| Close finished issues, write their evidence | `stage-close` |
| Close out a plan that has finished | `close-out` |
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

## The ordering, and why this is a package

`plan-week` already anticipates the deletion. **As it read when this plan was
written**, and it has since been corrected — see below:

> The containing branch is a hint, never a source. `git branch --contains`
> answers only while the branch still exists, and merging is supposed to delete
> it. On the measured week the branch layer rescued 18 commits over 30 days.

So the recap is *robust* to deleted branches and still *helped* by live ones.
That fixes a sequence rather than merging a tool: **recap first, then clean.** A
package with a fixed order is the artifact; one skill that does both is the
thing Plan 146 forbids.

**Corrected 2026-09-04, by measuring it.** The paragraph above used to claim
the branch layer "rescued 18 commits over 30 days" — the quotation above, kept
as it stood — and this section used to be titled *"The ordering is the point"*.
Both overstated it, and `plan-week` now carries the corrected text. Over 2026-08-24..30,
200 of 209 non-merge commits attributed from their own subject or body; for the
9 that did not, `git branch --contains` returned **75 refs each** and
discriminated nothing, while the enclosing merge commit answered all 9 — and a
merge commit's subject is permanent history that no branch deletion touches.
`--contains` is informative only for commits *not* on `origin/master` (24, at
1-2 refs each), and those sit on branches Stage 3 refuses to delete because they
carry unlanded commits.

So the ordering is **defence in depth, not the load-bearing constraint**. It is
kept — it costs nothing and the risk is one-sided — and the package is still a
package, but for the reason in the section above rather than this one: the three
steps share a *time*, not a dependency. The thing that would genuinely cost the
recap its fallback is enabling squash or rebase merging, which leaves no merge
commit at all; that is a repository setting, and Stage 3 now holds it in CI.

## Stages

### Stage 0 — Write the order down

No code. The close sequence, and for each step the condition that makes it safe
to run: what must have happened before it, and what it must not run before.
This is the stage the other three depend on, and it is the one that makes the
package a package rather than three skills that happen to run on Sunday.

### Stage 1 — Record Plan 149's cycle measures

A skill that reads Linear's cycle history and the repository, proposes the six
measures, and writes them into Plan 149's *Cycle measures* table on approval.
Writes one plan document's evidence table and nothing else.

**Amended 2026-09-04.** This stage was scheduled behind a closed cycle on the
grounds that it could not be developed without one. That conflated two acts:
recording a final measure needs a closed cycle, building the skill does not. The
skill carries a **provisional** mode that reads an open cycle, labels every
number provisional and refuses to write — which is how it was developed, and is
independently useful for seeing where a running cycle stands.

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
actually ended. **Amended 2026-09-04** on the same grounds as Stage 1: the rule
governs the write, not the read, so this stage also carries a provisional mode
that reports what *would* roll and changes nothing.

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

## The checks

Both outstanding success criteria need one real cycle close, and Cycle 2 is the
first that can supply one. Nothing here owes code: all four stages closed
2026-09-04 with their `## Record` entries and CAR-41 is `Done`.

**1 — Cycle 2's measures, recorded from a closed cycle.** Success criterion 2.

- **Deployed:** `.claude/skills/cycle-measures/`, 2026-09-04.
- **Watched:** Plan 149's `## Cycle measures` table, its Cycle 2 column, filled
  by that skill in **final** mode, with any partial measure opening
  `**Partial —**`.
- **Due:** Cycle 2 ends 2026-09-07T05:00Z; the reading is taken at the close and
  never before it.
- **Receives the result:** a paragraph inside `### Stage 1 — 2026-09-04`, via
  `note-evidence`.
- **What would falsify it:** `seeded issues` coming out wrong rather than
  partial. Cycle 2 is the first cycle with roll-ins — CAR-17 and CAR-31 — which
  is precisely the case Cycle 1 could not exercise, so the clean Cycle 1
  validation does not cover it.

**2 — One close runs end to end, in the written order.** Success criterion 3.

- **Deployed:** [`docs/planning/cycle_close_order.md`](../planning/cycle_close_order.md)
  and all four skills, 2026-09-04.
- **Watched:** that the eight steps run in the order the document gives, and
  specifically that `docs/recaps/2026-09-06.md` exists before `ref-hygiene`
  deletes anything.
- **Due:** 2026-09-09, the recap deadline for the week ending 2026-09-06.
- **Receives the result:** a paragraph inside `### Stage 0 — 2026-09-04`, via
  `note-evidence`.
- **What would falsify it:** a step turning out to be in the wrong place. The
  order was derived from two rules rather than from having run it, and the first
  real close is the only thing that tests that derivation.

This plan's check 1 also feeds [Plan 149](plan_149_linear_execution_layer.md)'s
own 2026-09-15 gate, which requires the Cycle measures table to hold real
post-cycle reads for all three cycles. Plan 149 owns that gate; this plan owes
it the Cycle 2 column.

## Record

### Stage 0 — 2026-09-04

The close sequence is written to
[`docs/planning/cycle_close_order.md`](../planning/cycle_close_order.md): eight
steps across six skills and two gaps, split around the cycle's `endsAt`
boundary, each naming what must precede it and what it must not run before.

The order is presented as a consequence of two rules rather than as a list, so
a step's position is explicable rather than remembered: a read comes before any
write that would perturb it, and the one step that destroys evidence runs after
every step that reads it. A third rule decides which side of the boundary a
step sits on — nothing touches a `cycleId` until the cycle has ended.

Steps 2 and 3 have no owner. Stages 1 and 2 of this plan are where they get
one, and the document says so and states their by-hand constraints rather than
implying coverage it does not have.

**One claim this stage was built on did not survive being checked.** Measured
2026-09-04 over the window 2026-08-24..30: 200 of 209 non-merge commits
attributed from their own subject or body. For the 9 that did not,
`git branch --contains` returned **75 refs each** and discriminated nothing,
while the enclosing merge commit answered all 9 — and a merge commit's subject
is permanent history that no branch deletion touches. `--contains` is
informative only for commits *not* on `origin/master` (24, at 1–2 refs each),
and those sit on branches `ref-hygiene` refuses to delete because they carry
unlanded commits. So recap-before-clean is **defence in depth, not the
load-bearing constraint** this plan claims above. It is kept — it costs nothing
and the risk is one-sided — and both `cycle_close_order.md` and the skill
record it that way.

Verified by `pytest tests/test_planning_docs.py --noconftest`, 52 passed,
which includes the dangling-link check over all of `docs/`.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

### Stage 1 — 2026-09-04

[`.claude/skills/cycle-measures/`](../../.claude/skills/cycle-measures/SKILL.md)
reads a cycle and proposes Plan 149's six measures. It writes that one table and
nothing else — no `cycleId`, no issue, no other section.

**The stage's stated precondition was wrong, and correcting it is what let the
stage happen.** This plan scheduled it behind a closed cycle because it "needs a
closed cycle to develop against". Recording an authoritative number needs one;
building the skill does not, and the 47% undercount that motivated the rule came
from a mid-cycle number being *written*, not read. The skill therefore carries a
**provisional** mode that reads any cycle, labels every number provisional and
refuses to write.

**Validated against known-good rather than asserted.** Cycle 1 is closed and its
column was written by hand on 2026-08-31, so it is a regression target. Eight of
nine derivations reproduce it exactly — total and completed issues and points,
rollover in both units, seeded issues, and issues added after start. The ninth,
seeded *points*, derives 16 against 17 because CAR-6 carries no estimate at all:
it was created 2026-08-24 and Fibonacci estimation was not enabled until the
25th. Named in the skill rather than rounded away.

**Running it against the live board then found two defects in the first draft.**
Both are now encoded, and neither was visible from reasoning:

- *The source depends on the mode, and getting it backwards undercounts both
  ways.* On a closed cycle the history arrays are the truth and membership has
  decayed — Cycle 1 answers with **23 issues, not the 25 it closed with**,
  because Linear moved the rolled pair into Cycle 2. On an open cycle membership
  is the truth and the arrays lag: Cycle 2's last daily rollup read 44 issues /
  78 points against a live 48 / 84.
- *`seeded issues` is exact only for the first cycle.* A cycle's `startsAt` is
  the previous cycle's `endsAt`, so an issue rolled *in* was also created before
  the start and `createdAt` cannot tell it from a seed. Cycle 2 carries two. The
  clean Cycle 1 validation proved less than it looked, because Cycle 1 had no
  predecessor.

**Outstanding:** no measure has been recorded yet. Cycle 2 closes 2026-09-07 and
is this skill's first authoritative write.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

### Stage 2 — 2026-09-04

[`.claude/skills/roll-cycle/`](../../.claude/skills/roll-cycle/SKILL.md)
establishes what a closing cycle leaves unfinished, reconciles it against what
Linear already moved, and acts only on the remainder. It sets `cycleId` on
approved issues and touches nothing else — no status, no estimate, no plan
document.

**The skill's first job is not to move anything.** Cycle 1 rolled CAR-17 and
CAR-31 and nobody did anything: Linear moved both at `endsAt`. So the skill
assumes the move already happened and verifies, rather than assuming it did not
and duplicating it. Three outcomes per unfinished issue — already moved, the
remainder, and *should not roll at all* — and the third is reported and never
acted on, because cancelling an issue to improve a rollover number is the
failure this measure would be most susceptible to.

Amended on the same grounds as Stage 1: the standing rule governs the **write**,
not the read, so this skill also carries a provisional mode that reports what
*would* roll and changes nothing.

**Exercised against the live board.** Cycle 2, read 2026-09-04 with three days
left: **6 issues / 10 points would roll** — CAR-89 and CAR-31 soaking, CAR-79,
CAR-81 and CAR-83 ready, and CAR-41 itself. Three issues were cancelled during
the cycle (CAR-64, CAR-75, CAR-76, 4 points). Nothing was written, and the
report carried the caveat that Cycle 1's last two days landed four plans, so a
mid-cycle spill figure is a loose upper bound.

**Outstanding:** nothing has been rolled. Cycle 2 closes 2026-09-07 and is this
skill's first authoritative write.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

### Stage 3 — 2026-09-04

[`scripts/audit_git_refs.py`](../../scripts/audit_git_refs.py) classifies every
local branch, worktree and stash and deletes nothing;
[`.claude/skills/ref-hygiene/`](../../.claude/skills/ref-hygiene/SKILL.md)
proposes a set and performs only what is approved. Only `landed` authorises
anything, and a failed or skipped fetch, or an unlistable pull-request state,
collapses every verdict to `unknown`, which refuses.

**Success criterion 4 is met by construction.**
`tests/scripts/test_audit_git_refs.py` builds a real repository with a real
remote holding a deliberate case per finding, in the shape Plan 158 Stage 3
used:

| Finding | What its constructed case proves |
|---|---|
| 1 — the trunk is `origin/master` | both directions: `git branch --merged master` **clears** a branch whose content exists nowhere else, and calls a landed branch unmerged |
| 2 — ancestry is not the test | a patch replayed under a different SHA is `landed` by `git cherry`, unmerged by ancestry |
| 3 — the only copy | no remote ref, and ahead-of-remote, both refuse; `0 0` is the verification, not a quiet `git push` |
| 4 — safe to delete is not landed | pushed and verified and still `owed`, with its commits listed individually |
| 5 — stashes | reported, never deletable, invisible to every `git branch` command |
| 6 — protected refs | trunk, HEAD, worktree-held and open-PR head, with an identical unheld branch as the control |

**The stage changed direction once, on a measurement, and the first design was
wrong.** It was built as a cleanup ritual. Measured 2026-09-04: 69 remote
branches, **67 already merged**, 0 open PRs — against 9 local branches, 7 held
by worktrees. The accretion was never local, and its cause was one setting.
`delete_branch_on_merge` was off, so a merged branch's remote ref survived, so
no local branch could ever read `gone`, so the only detector left was the
patch-id archaeology this stage had built.

Applied: `delete_branch_on_merge` on; squash and rebase merging off (both land a
PR with no merge commit, and that subject is the only permanent record of a
branch name); `fetch.prune` and `push.autoSetupRemote` in `~/.gitconfig`. A
one-time backfill then swept the 65 merged branches no worktree held: **69
remote refs became 4**, and three local branches immediately began reading
`gone` — the same fact that reaches the second machine at its next fetch, with
no hook and nothing to remember.

**Enforced, not merely recommended.**
[`scripts/verify_git_ref_hygiene_contract.py`](../../scripts/verify_git_ref_hygiene_contract.py)
runs in its own CI job against the live remote and fails on a settings
regression or on merged branches climbing past a dated ratchet (67 → 5 after
the sweep; the true count is 2). Local branch state is deliberately
**unenforced**: a runner clones fresh and has one branch, so a test reading
`git branch` would pass forever — the false green `docs/TESTING.md` names.

Two further assertions landed here but hold **Stage 0's order, not this
stage's refs**: `test_the_recap_series_has_no_interior_gap` and
`test_the_recap_series_is_not_stale`. The week ending Sunday N is owed a recap
by end of N+3, so CI turns red on the Thursday; the deadline is a pure function
pinned by six rows rather than described in prose. Both were mutation-tested —
removing `docs/recaps/2026-08-09.md` fails the first naming that Sunday,
removing the two newest fails the second naming the date one is owed by.

**Running it for real earned its place twice.** The first live run raised
`UnicodeEncodeError` on the Windows console from one `→` in the worktree list,
which every test had passed over; the report is ASCII now with an assertion
holding it. The first run of the CI verifier found `allow_rebase_merge` still
true, which the squash decision had missed.

Verified by 53 tests across `tests/scripts/test_audit_git_refs.py` and
`tests/scripts/test_verify_git_ref_hygiene_contract.py`; `pytest tests/ -m "not
integration"`, 3698 passed; the documentation job, 52 passed; and the verifier
green against the live remote.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work. Both surfaces mention branches only as code branches.
