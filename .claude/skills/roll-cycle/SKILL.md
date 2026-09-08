---
name: roll-cycle
description: "Establish what a closing cycle leaves unfinished, confirm what Linear already moved on its own, and act only on the remainder. Use when a cycle has ended and its unfinished work needs rolling forward, or when the user wants a provisional read of what would roll if the current cycle ended now. This skill writes issue cycle assignments and nothing else: it never touches a cycleId before the cycle has actually ended, never edits a plan document, docs/PLANS.md, a status marker or an estimate, and never closes or cancels an issue to keep it out of the count."
---

# Rolling a cycle's unfinished work

Step 3 of [the close order](../../../docs/planning/cycle_close_order.md), and
the one with the least work in it — because **Linear usually does it for you.**

Cycle 1 rolled two issues, CAR-17 and CAR-31, and **nobody did anything.**
Linear moved both at `endsAt`. So the first job of this skill is not to move
work; it is to find out what is actually left to move.

## The boundary

| You do | You never do |
|---|---|
| read what a cycle leaves unfinished | change an issue's status, estimate, title or body |
| confirm what Linear already moved | move anything before `endsAt` has passed |
| propose a remainder and wait | move an issue the user did not approve |
| set `cycleId` on approved issues | close or cancel an issue to keep it out of a count |
| — | edit a plan document, `docs/PLANS.md`, or the archive |

Recording the measures is `cycle-measures`, and it runs **first**. This skill
rewrites the field those measures are read from, so taking the number
afterwards reads a set this skill has already edited.

## The rule that has not changed

**Nothing touches a `cycleId` until the cycle has actually ended.**

Rolling early falsifies Plan 149's rollover measure, which is a measure *of how
much moved* — move it by hand a day early and the measure records your action
rather than the cycle's outcome. The repository already carried this rule before
this skill existed, and the skill does not soften it.

## Two modes, and only one of them writes

| Mode | When | What it does |
|---|---|---|
| **final** | `endsAt` has passed | proposes the remainder and, on approval, sets `cycleId` |
| **provisional** | any time | reports what *would* roll if the cycle ended now, and **writes nothing** |

Provisional answers a real question — *how much is going to spill?* — and it is
safe because it cannot write. Say plainly that it is a snapshot: work completed
in the days remaining leaves the set, and Cycle 1's last two days moved four
plans, so a mid-cycle spill estimate is an upper bound and usually a loose one.

## Phase 1 — Gather

1. **The cycle and its boundary.** `list_cycles` for team
   `ee63b26b-de49-4fa5-8617-bbaed7c1227d`. **Compare `endsAt` to now and state
   the mode before anything else.**
2. **The unfinished set.** `list_issues` for that cycle with `status`,
   `statusType`, `estimate`. Unfinished is `statusType` other than `completed`
   and `canceled`.
3. **What Linear already moved.** After the close, a rolled issue answers as the
   *next* cycle's, not this one's — so list the next cycle too and identify
   issues that were this cycle's work. `issueCountHistory[-1]` minus
   `completedIssueCountHistory[-1]` gives the count that should have left, which
   is what you reconcile against.
4. **Why each one is unfinished**, in one line: a soak with a date, a blocker, a
   dependency, or genuinely unstarted. This is the part worth a person's
   attention and the reason the skill stops.

## The platform moves things, and the reconciliation is the work

Measured on Cycle 1: 25 issues at close, 23 completed, and the 2 remaining were
in Cycle 2 by the time anyone looked. **Assume the move already happened** and
verify, rather than assuming it did not and duplicating it.

Three outcomes per unfinished issue, and all three are reported:

- **Already moved by Linear** → nothing to do. Say so; do not re-set a `cycleId`
  to the value it already holds.
- **Not moved, and should be** → this is the remainder, and it is what the
  approval covers.
- **Not moved, and should not be** → an issue that ought to go back to `Backlog`,
  be canceled, or be re-scoped is not a rollover. Name it and let the user
  decide; **do not cancel an issue to make a rollover number look better.**

## Rollover is a signal, not just a chore

Plan 149 reads this measure as gating, not slicing. Cycle 1's two rollovers were
a seven-day observation window and an issue waiting on four defects in another
plan — neither an oversized slice. So when you report the remainder, say **why**
each one rolled. A cycle whose rollovers are all soaks is healthy; a cycle whose
rollovers are all unstarted work was over-seeded, and that is a finding for
`fill-cycle`, which runs after this.

## Phase 2 — Propose, then stop

Present:

1. **The mode**, with `endsAt` against now.
2. **What Linear already moved**, by identifier, with the reconciliation.
3. **The remainder** — each issue, its points, its status, and one line on why
   it did not finish.
4. **Anything that should not roll at all**, named, with what you think should
   happen and no action taken.
5. **The total**, in issues and points, since that is what `fill-cycle` consumes
   as headroom.

Then **stop**. In provisional mode there is nothing to approve: report and end.

## Phase 3 — Write, only in final mode and only after approval

Set `cycleId` on the approved issues, one at a time, and nothing else. Do not
touch status, estimate, title, description, labels or assignee — an issue that
rolls is the same issue in a different cycle.

If any move fails, stop and report. A half-moved rollover is worse than none,
because the next reader cannot tell which half was intended.

## After writing

Report what moved, what Linear had already moved, what was deliberately not
moved and why, and the resulting headroom in issues and points. Then name the
next step: `fill-cycle`, step 7 of the close order, which needs this number to
size the new cycle correctly.

## What this skill must never do

- **Touch a `cycleId` before `endsAt` has passed.** The rule predates this skill
  and survives it.
- **Move an issue the user did not approve**, or move one Linear already moved.
- **Cancel, close, or re-estimate an issue** to change how the rollover reads.
- **Edit a plan document, `docs/PLANS.md`, a status marker or the archive.**
- **Run before `cycle-measures`.** It rewrites the field those measures read.
