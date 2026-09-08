---
name: cycle-measures
description: "Read a Linear cycle and propose Plan 149's six cycle measures — derived from the cycle's own history and the issues themselves, with every partial measure marked partial. Use when a cycle has closed and its measures are owed, or when the user wants a provisional mid-cycle read of where the current cycle stands. This skill writes one table in one plan document and nothing else: it touches no cycleId, no issue, no docs/PLANS.md row, no status marker and no other section, and it refuses to write a final measure for a cycle that has not ended."
---

# Reading a cycle's measures

[Plan 149](../../../docs/plans/plan_149_linear_execution_layer.md) defines six
measures and a `## Cycle measures` table to hold them. Filling that table was
step 2 of [the close order](../../../docs/planning/cycle_close_order.md) and had
no owner. This is it.

## The boundary

You fill one table. You do not touch the board.

| You do | You never do |
|---|---|
| read a cycle's history and its issues | change an issue, a status, or a `cycleId` |
| propose six measures and wait | write before approval |
| write Plan 149's `## Cycle measures` table | edit any other section of that document |
| mark a partial measure partial | present a partial as a total |
| ask the maintainer for the two qualitative rows | invent one, or infer it from the numbers |
| — | edit `docs/PLANS.md`, the archive, or any other plan |

Rolling issues forward is `roll-cycle`. It runs **after** this skill, and the
reason is the first rule of the close order: this is a read, and that is a write
that would perturb it.

## Two modes, and only one of them writes

**This is the correction to Plan 164's original claim** that Stages 1 and 2
"need a closed cycle and cannot run before" one exists. Building a skill and
recording an authoritative number are different acts. The rule was only ever
about the second.

| Mode | When | What it does |
|---|---|---|
| **final** | the cycle's `endsAt` has passed | derives the six measures and proposes the table edit |
| **provisional** | any time, including mid-cycle | derives the same numbers, labels every one of them provisional, and **writes nothing** |

Provisional is not a lesser mode kept for testing. It is how you see where a
running cycle stands, and it is safe precisely because it cannot write.

**What a provisional read is not is a forecast.** Cycle 1's mid-cycle read on
2026-08-29 saw ~30 points against the 44 the closed cycle recorded — a **47%
undercount**, because the last two days landed four plans. Say that with the
number, every time, in the same breath:

> Provisional, cycle N is still open. Cycle 1's equivalent read saw ~30 points
> against the 44 it closed with; treat this as a trajectory, not a total.

A provisional number must never be transcribed into the table, into a budget, or
into another skill. That is the exact path by which the 47% undercount reached
Stage 1's points budget and two skills before anyone caught it.

## Phase 1 — Gather

1. **The cycle.** `list_cycles` for team `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
   Record `number`, `startsAt`, `endsAt`, and the four history arrays. **Compare
   `endsAt` to now and state which mode you are in before anything else.**
2. **Its issues.** `list_issues` with that cycle, requesting at least `estimate`,
   `status`, `statusType`, `createdAt`, `completedAt`, `canceledAt`.
3. **The table as it stands**, in Plan 149's `## Cycle measures`, so a proposal
   lands in the right column and does not overwrite a neighbour.

## The derivations, and what each is worth

Every number below reproduces Cycle 1's hand-written column, checked on
2026-09-04 against the values recorded on 2026-08-31.

**Which source you read depends on the mode, and getting it backwards
undercounts in both directions.** Measured 2026-09-04:

| Mode | Read | Because the other one |
|---|---|---|
| **final** (closed) | the history arrays | membership has *decayed* — Cycle 1 answers with **23 issues, not the 25 it closed with**, because Linear moved the rolled pair into Cycle 2 |
| **provisional** (open) | live issue membership | the history arrays *lag* — Cycle 2's last daily rollup read 44 issues / 78 points against a live 48 / 84, four issues and six points behind |

The arrays are daily snapshots. On a closed cycle the last one is the truth and
membership has moved on; on an open cycle membership is the truth and the last
snapshot is stale. Neither source is right in both modes.

| Quantity | Derivation | Cycle 1 |
|---|---|---|
| total issues | `issueCountHistory[-1]` | 25 ✔ |
| completed issues | `completedIssueCountHistory[-1]` | 23 ✔ |
| total points | `scopeHistory[-1]` | 48 ✔ |
| completed points | `completedScopeHistory[-1]` | 44 ✔ |
| rollover issues | `issueCountHistory[-1] − completedIssueCountHistory[-1]` | 2 ✔ |
| rollover points | `scopeHistory[-1] − completedScopeHistory[-1]` | 4 ✔ |
| seeded issues | issues whose `createdAt` < the cycle's `startsAt`, **less the previous cycle's rollover** | 8 ✔ |
| added after start | total − seeded | 17 ✔ |

**`seeded issues` is exact only for the first cycle, and Cycle 1 is the first
cycle** — which is why it validated cleanly and why that validation proves less
than it looks. A cycle's `startsAt` is the previous cycle's `endsAt`, so an
issue rolled *in* was also created before this cycle started and `createdAt`
cannot tell it from a seed. Cycle 2 carries two: CAR-17 and CAR-31, both created
during Cycle 1 and moved by the platform.

So subtract the previous cycle's recorded rollover, which `roll-cycle` reports
and the previous column of this very table records. **If that figure was never
recorded, mark `seeded issues` partial** rather than counting roll-ins as
seeds — the error inflates seeding and deflates "added after start", which is
the measure that turned the eight-issue cap into a points budget.

**Eight of nine reproduce exactly. The ninth is instructive and is not a bug.**
Seeded *points* derives 16 against the 17 recorded, because CAR-6 carries no
estimate at all: it was created 2026-08-24 and Fibonacci estimation was not
enabled on the team until 2026-08-25. An unestimated issue in the denominator is
worth surfacing by name rather than rounding away — say which issue and why,
exactly as this paragraph does.

### The measures that decay after the close, and why you run this at the close

**Linear moves a rolled issue out of the cycle it rolled from.** Querying Cycle
1 on 2026-09-04 returns **23 issues, not the 25 it closed with** — CAR-17 and
CAR-31 are in Cycle 2 now and answer as Cycle 2's.

So on a **closed** cycle any measure computed from current membership is
reading a set the platform has since edited. The history arrays are immune,
which is why final mode reads them. `seeded issues` cannot be — it needs each
issue's `createdAt`, which only membership carries — and that is the measure
that degrades the longer you wait.

The consequences, stated rather than worked around:

- Run this **at the close**. The order exists for this reason.
- Run it late and `seeded issues` and everything derived from it are **partial**
  — they cannot see an issue that has since left. Say so in the cell.
- Never reconcile a history number against a membership count and "fix" the
  history. The history is what the cycle was; membership is what it is now.

## The two qualitative rows are the maintainer's

*Time to choose next work* and *Recap effort* are judgements about how the week
felt. **Ask for them. Do not infer them from the countable rows**, and do not
write a cell because the numbers imply an answer — Cycle 1's first row records
friction that the completion figures alone would not have suggested, and its
value is that a person said it.

If the user does not supply them, write `—` and say the row is unanswered. An
empty cell is honest; a manufactured one is not.

## A partial measure is recorded as partial

*State corrections* and *Duplicate edits* need per-issue history the list API
does not return. Cycle 1's *State corrections* cell is three known occurrences
with no exhaustive count and says so, in the cell.

**A total that is not one is worse than an admitted gap.** So a cell that cannot
be complete opens with `**Partial —**`, gives what is known, and names what
would be needed for a real figure. Do not quietly present the known subset as
the number.

## Phase 2 — Propose, then stop

Present:

1. **The mode**, and the evidence for it: the cycle's `endsAt` against now. In
   provisional mode, the 47% sentence, verbatim.
2. **The countable rows**, each with its derivation shown, so the number can be
   checked rather than trusted.
3. **Anything unestimated or otherwise anomalous**, by issue identifier.
4. **The two qualitative rows** — as questions, not as drafts.
5. **Which cells are partial**, and what would make them whole.
6. **The exact table text** you propose, and which column it goes in.

Then **stop**. In provisional mode there is nothing to approve and nothing is
written: report and end there.

## Phase 3 — Write, only in final mode and only after approval

Edit **only** the cells of Plan 149's `## Cycle measures` table for that cycle's
column. Do not touch its prose, its other sections, its stage sections, or any
other file.

If the read changes what a *neighbouring* section claims — as Cycle 1's close
did, when it showed Stage 1's ~30-point budget was 47% low — **say so and stop**.
Editing that prose is a separate decision, and it is not this skill's.

## After writing

Report the cells written, the cells left `—`, the cells marked partial, and
anything you could not derive. Then name the next step: `roll-cycle`, which is
step 3 of the close order and always runs after this one.

## What this skill must never do

- **Write a final measure for a cycle that has not ended.** The mode check is
  the first thing in phase 1 for this reason.
- **Let a provisional number leave the report.** Not into the table, not into a
  budget, not into another skill.
- **Invent a qualitative row**, or derive one from the countable rows.
- **Present a partial as a total.**
- **Touch an issue, a status, or a `cycleId`.** Reading the board is this
  skill's whole interaction with it.
- **Edit any section of Plan 149 but its measures table**, or any other file.
